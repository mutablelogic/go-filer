package s3

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"strings"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// create creates the file at the path, with the content, and the request
func create(b *S3, path, content string, req schema.CreateObjectRequest) (*schema.Object, error) {
	req.Path, req.Body = path, strings.NewReader(content)
	return b.CreateObject(context.Background(), req)
}

// failingReader returns its content, and then the error
type failingReader struct {
	r   io.Reader
	err error
}

func (r *failingReader) Read(data []byte) (int, error) {
	n, err := r.r.Read(data)
	if errors.Is(err, io.EOF) {
		return n, r.err
	}
	return n, err
}

func Test_S3_CreateObject(t *testing.T) {
	bucket, b := newFakeBucket(t, "", "a.txt")

	// A file is written, with its type from its extension, and returned
	object, err := create(b, "/c/new.txt", "hello", schema.CreateObjectRequest{})
	require.NoError(t, err)
	assert.Equal(t, "/c/new.txt", object.Path)
	assert.Equal(t, int64(5), *object.Size)
	assert.Equal(t, etagOf("hello"), *object.ETag)
	assert.Equal(t, "text/plain; charset=utf-8", object.ContentType)
	bucket.Lock()
	assert.Equal(t, "hello", bucket.keys["c/new.txt"])
	assert.Equal(t, 1, bucket.puts)
	bucket.Unlock()

	// A file is replaced, and a nil body is an empty file
	object, err = create(b, "/a.txt", "replaced", schema.CreateObjectRequest{})
	require.NoError(t, err)
	assert.Equal(t, etagOf("replaced"), *object.ETag)
	object, err = b.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/empty.txt"}})
	require.NoError(t, err)
	assert.Equal(t, int64(0), *object.Size)

	// The type is the request's, when the extension isn't known, which can't
	// be another type
	object, err = create(b, "/data", "{}", schema.CreateObjectRequest{ObjectMeta: schema.ObjectMeta{ContentType: "application/json"}})
	require.NoError(t, err)
	assert.Equal(t, "application/json", object.ContentType)
	_, err = create(b, "/b.txt", "b", schema.CreateObjectRequest{ObjectMeta: schema.ObjectMeta{ContentType: "TEXT/PLAIN"}})
	assert.NoError(t, err)
	_, err = create(b, "/b.txt", "b", schema.CreateObjectRequest{ObjectMeta: schema.ObjectMeta{ContentType: "image/png"}})
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)

	// The root, a directory, and a volume which is read-only, can't be written
	for _, path := range []string{"/", "", "/c/"} {
		_, err := create(b, path, "x", schema.CreateObjectRequest{})
		assert.ErrorIs(t, err, fs.ErrInvalid, path)
	}
	b.readonly = true
	_, err = create(b, "/x.txt", "x", schema.CreateObjectRequest{})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
}

func Test_S3_CreateMultipart(t *testing.T) {
	defer func(size int) { partSize = size }(partSize)
	partSize = 4
	bucket, b := newFakeBucket(t, "")
	counts := func() (int, int, int, int, int) {
		bucket.Lock()
		defer bucket.Unlock()
		return bucket.puts, bucket.createMultiparts, bucket.parts, bucket.completes, bucket.aborts
	}

	// A file which is no larger than a part is written in one request
	for _, content := range []string{"012", "0123"} {
		_, err := create(b, "/small.bin", content, schema.CreateObjectRequest{})
		require.NoError(t, err)
	}
	puts, creates, parts, completes, _ := counts()
	assert.Equal(t, []int{2, 0, 0, 0}, []int{puts, creates, parts, completes})

	// A larger one is written in parts, the last of which is smaller, or
	// isn't empty, when the file is a number of parts
	for _, test := range []struct {
		content string
		parts   int
	}{{"0123456789", 3}, {"01234567", 2}, {"01234", 2}} {
		bucket.Lock()
		bucket.parts = 0
		bucket.Unlock()
		object, err := create(b, "/large.bin", test.content, schema.CreateObjectRequest{Meta: []schema.Meta{{Key: "a", Value: json.RawMessage(`1`)}}})
		require.NoError(t, err)
		assert.Equal(t, int64(len(test.content)), *object.Size)
		assert.Equal(t, "application/octet-stream", object.ContentType)
		require.Len(t, object.Meta, 1)
		bucket.Lock()
		assert.Equal(t, test.content, bucket.keys["large.bin"])
		assert.Equal(t, test.parts, bucket.parts)
		assert.Empty(t, bucket.uploads)
		bucket.Unlock()
	}

	// A body which can't be read, or ends before it should, isn't written,
	// and its parts are aborted
	for _, readErr := range []error{errors.New("broken"), io.ErrUnexpectedEOF} {
		_, err := b.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/broken.bin"}, Body: &failingReader{strings.NewReader("0123456789"), readErr}})
		assert.ErrorIs(t, err, readErr)
		_, err = b.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/broken.bin"}, Body: &failingReader{strings.NewReader("01"), readErr}})
		assert.ErrorIs(t, err, readErr)
	}
	_, _, _, _, aborts := counts()
	assert.Equal(t, 2, aborts)
	bucket.Lock()
	_, exists := bucket.keys["broken.bin"]
	assert.False(t, exists)
	assert.Empty(t, bucket.uploads)
	bucket.Unlock()
}

func Test_S3_CreateConditions(t *testing.T) {
	for _, size := range []int{8 << 20, 4} {
		func() {
			defer func(size int) { partSize = size }(partSize)
			partSize = size
			bucket, b := newFakeBucket(t, "", "a.txt")

			// A file which exists isn't replaced, when it mustn't exist, and is
			// created when it doesn't
			_, err := create(b, "/a.txt", "replaced", schema.CreateObjectRequest{IfNotExists: true})
			assert.ErrorIs(t, err, gofiler.ErrConflict)
			_, err = create(b, "/new.txt", "created", schema.CreateObjectRequest{IfNotExists: true})
			assert.NoError(t, err)

			// A file which doesn't exist isn't created, when it must exist, and
			// is replaced when it does
			_, err = create(b, "/missing.txt", "created", schema.CreateObjectRequest{IfExists: true})
			assert.ErrorIs(t, err, gofiler.ErrPreconditionFailed)
			_, err = create(b, "/a.txt", "replaced", schema.CreateObjectRequest{IfExists: true})
			assert.NoError(t, err)
			_, err = create(b, "/a.txt", "x", schema.CreateObjectRequest{IfExists: true, IfNotExists: true})
			assert.ErrorIs(t, err, gofiler.ErrBadParameter)

			bucket.Lock()
			defer bucket.Unlock()
			assert.Equal(t, "replaced", bucket.keys["a.txt"])
			assert.Equal(t, "created", bucket.keys["new.txt"])
			_, exists := bucket.keys["missing.txt"]
			assert.False(t, exists)
			assert.Empty(t, bucket.uploads)
		}()
	}
}

func Test_S3_CreateMeta(t *testing.T) {
	bucket, b := newFakeBucket(t, "")

	// The user's metadata is written, by the key, or the namespace and the
	// key, separated by a ".", with values which are JSON, with characters
	// which aren't ASCII escaped, and is returned
	meta := []schema.Meta{
		{Key: "rating", Value: json.RawMessage(`5`)},
		{Namespace: "photo", Key: "camera", Value: json.RawMessage(` "Fujifilm X100V" `)},
		{Namespace: "photo", Key: "caption", Value: json.RawMessage(`"Zoë 📷"`)},
		{Namespace: "photo", Key: "lens", Value: json.RawMessage(`{"mm": 35}`)},
	}
	object, err := create(b, "/a.jpg", "jpeg", schema.CreateObjectRequest{Meta: meta})
	require.NoError(t, err)
	bucket.Lock()
	assert.Equal(t, map[string]string{
		"rating": `5`, "photo.camera": `"Fujifilm X100V"`, "photo.caption": `"Zo\u00eb \ud83d\udcf7"`, "photo.lens": `{"mm":35}`,
	}, bucket.meta["a.jpg"])
	bucket.Unlock()
	require.Len(t, object.Meta, 4)
	for i, want := range []struct{ namespace, key, value string }{
		{"photo", "camera", `"Fujifilm X100V"`}, {"photo", "caption", `"Zoë 📷"`}, {"photo", "lens", `{"mm":35}`}, {"", "rating", `5`},
	} {
		assert.Equal(t, want.namespace, object.Meta[i].Namespace)
		assert.Equal(t, want.key, object.Meta[i].Key)
		assert.JSONEq(t, want.value, string(object.Meta[i].Value))
	}

	// Metadata whose names aren't lowercase letters, numbers and "-", whose
	// values aren't JSON, which is repeated, or is more than 2 KB, isn't
	// written
	for _, meta := range [][]schema.Meta{
		{{Key: "Rating", Value: json.RawMessage(`5`)}},
		{{Key: "my_rating", Value: json.RawMessage(`5`)}},
		{{Key: "a.b", Value: json.RawMessage(`5`)}},
		{{Key: "", Value: json.RawMessage(`5`)}},
		{{Namespace: "Photo", Key: "camera", Value: json.RawMessage(`5`)}},
		{{Namespace: "a.b", Key: "camera", Value: json.RawMessage(`5`)}},
		{{Key: "rating", Value: json.RawMessage(`five`)}},
		{{Key: "rating"}},
		{{Key: "rating", Value: json.RawMessage(`5`)}, {Key: "rating", Value: json.RawMessage(`6`)}},
		{{Key: "big", Value: json.RawMessage(`"` + strings.Repeat("x", metaSize) + `"`)}},
	} {
		_, err := create(b, "/b.jpg", "jpeg", schema.CreateObjectRequest{Meta: meta})
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, meta)
	}
	bucket.Lock()
	_, exists := bucket.keys["b.jpg"]
	assert.False(t, exists)
	bucket.Unlock()
}

func Test_S3_CreatePrefix(t *testing.T) {
	bucket, b := newFakeBucket(t, "photos/2026")

	// The file's key is beneath the prefix
	object, err := create(b, "/a.jpg", "jpeg", schema.CreateObjectRequest{})
	require.NoError(t, err)
	assert.Equal(t, "/a.jpg", object.Path)
	bucket.Lock()
	assert.Equal(t, "jpeg", bucket.keys["photos/2026/a.jpg"])
	bucket.Unlock()
}

func Test_S3_CreateAndRead(t *testing.T) {
	defer func(size int) { partSize = size }(partSize)
	partSize = 4
	_, b := newFakeBucket(t, "")

	// A file which is written in parts is read, with its metadata
	content := "the quick brown fox jumps over the lazy dog"
	_, err := create(b, "/fox.txt", content, schema.CreateObjectRequest{Meta: []schema.Meta{{Namespace: "story", Key: "animal", Value: json.RawMessage(`"fox"`)}}})
	require.NoError(t, err)
	read, object, err := b.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/fox.txt"}})
	require.NoError(t, err)
	require.Len(t, object.Meta, 1)
	assert.Equal(t, "story", object.Meta[0].Namespace)
	require.NoError(t, read(context.Background(), func(r io.Reader) error {
		data, err := io.ReadAll(r)
		assert.Equal(t, content, string(data))
		return err
	}))
}

// closedReader returns its content, and then io.EOF, and then an error, as
// a pipe which is closed at its end does
type closedReader struct {
	r   io.Reader
	eof bool
}

func (r *closedReader) Read(data []byte) (int, error) {
	if r.eof {
		return 0, io.ErrClosedPipe
	}
	n, err := r.r.Read(data)
	if errors.Is(err, io.EOF) {
		r.eof = true
	}
	return n, err
}

func Test_S3_CreateReads(t *testing.T) {
	defer func(size int) { partSize = size }(partSize)
	partSize = 4
	bucket, b := newFakeBucket(t, "")

	// The body isn't read again, once it ends, in one request, or in parts
	for _, content := range []string{"012", "0123", "0123456789", "01234567"} {
		_, err := b.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.bin"}, Body: &closedReader{r: strings.NewReader(content)}})
		require.NoError(t, err, content)
		bucket.Lock()
		assert.Equal(t, content, bucket.keys["a.bin"])
		bucket.Unlock()
	}

	// A cancelled request doesn't read the body, and one which is cancelled
	// while it's read stops reading it
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := b.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/b.bin"}, Body: strings.NewReader("0123456789")})
	assert.ErrorIs(t, err, context.Canceled)
	ctx, cancel = context.WithCancel(context.Background())
	_, err = b.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/b.bin"}, Body: &cancellingReader{r: strings.NewReader("0123456789"), cancel: cancel}})
	assert.ErrorIs(t, err, context.Canceled)
	bucket.Lock()
	_, exists := bucket.keys["b.bin"]
	assert.False(t, exists)
	bucket.Unlock()
}

// cancellingReader reads a byte at a time, and cancels after the first
type cancellingReader struct {
	r      io.Reader
	cancel context.CancelFunc
	reads  int
}

func (r *cancellingReader) Read(data []byte) (int, error) {
	if r.reads++; r.reads == 2 {
		r.cancel()
	}
	return r.r.Read(data[:1])
}
