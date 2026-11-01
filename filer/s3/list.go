package s3

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"mime"
	"path"
	"slices"
	"strings"
	"sync"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// listIterator lists the objects beneath a directory, a page of keys at a
// time, in the same order as a filesystem's, which is S3's, by the byte
// order of their keys, where a directory sorts as its name followed by "/".
// A directory is a common prefix of the keys, or a key which ends with "/",
// and when the listing is recursive, it's returned before the first key
// beneath it, as S3 has no directories.
type listIterator struct {
	*S3
	mu      sync.Mutex
	pages   *s3.ListObjectsV2Paginator // the pages of keys, or nil when there are no more
	dir     string                     // the key of the directory which is listed
	last    string                     // the key of the last object returned, or where it starts
	pending []*schema.Object           // the objects of the page which aren't returned yet
	closed  bool
	span    trace.Span  // the span of the listing, which the requests for pages are within
	end     func(error) // ends the span, once there are no more objects, or an error, or it's closed
}

// listEntry is a key of a page, which is an object, or a common prefix
type listEntry struct {
	key    string
	object *s3types.Object // nil for a common prefix
}

var _ schema.ListObjectsIterator = (*listIterator)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// listPageSize is how many keys are requested in each page
var listPageSize int32 = 1000

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListObjects lists the objects beneath the path, which is a directory, or
// an object, when only it's returned. The directories are returned, and
// descended into when it's recursive, in the order of their keys. The
// objects which sort before or at StartAfter aren't returned. A directory
// which doesn't exist is fs.ErrNotExist, except the root, which is empty.
func (self *S3) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (_ schema.ListObjectsIterator, err error) {
	// The span lasts as long as the iterator, unless it can't be returned
	ctx, end := self.span(ctx, "ListObjects", req)
	defer func() {
		if err != nil {
			end(err)
		}
	}()

	rel := strings.Trim(path.Clean("/"+req.Path), "/")
	iter := &listIterator{S3: self, dir: self.prefix, last: self.key(req.StartAfter), span: trace.SpanFromContext(ctx), end: end}
	if rel != "" {
		iter.dir = self.prefix + rel + "/"
	}

	// The path is a file, when only it's returned, or a directory, which
	// exists when there is a key beneath it, or the root
	if object, err := self.stat(ctx, "list", req.Path); err != nil {
		return nil, err
	} else if !object.IsDir {
		if self.prefix+rel > iter.last {
			iter.pending = append(iter.pending, object)
		}
		return iter, nil
	}

	// List the keys beneath the directory, by a page at a time, which roll up
	// into the directories immediately within it, unless it's recursive
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(self.bucket),
		Prefix:  aws.String(iter.dir),
		MaxKeys: aws.Int32(listPageSize),
	}
	if iter.last > iter.dir {
		input.StartAfter = aws.String(iter.last)
	}
	if !req.Recursive {
		input.Delimiter = aws.String("/")
	}
	iter.pages = s3.NewListObjectsV2Paginator(self.client, input)

	// Return success
	return iter, nil
}

// Next returns the next object, or io.EOF when there are no more objects.
// The requests for pages are within the listing's span, which ends once
// there are no more objects, or there's an error.
func (self *listIterator) Next(ctx context.Context) (*schema.Object, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	if self.Tracer != nil {
		ctx = trace.ContextWithSpan(ctx, self.span)
	}
	object, err := self.next(ctx)
	if errors.Is(err, io.EOF) {
		self.finish(nil)
	} else if err != nil && !errors.Is(err, fs.ErrClosed) {
		self.finish(err)
	}
	return object, err
}

// next returns the next object, or io.EOF, which must be locked
func (self *listIterator) next(ctx context.Context) (*schema.Object, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		} else if self.closed {
			return nil, fs.ErrClosed
		} else if len(self.pending) > 0 {
			object := self.pending[0]
			self.pending = self.pending[1:]
			return object, nil
		} else if self.pages == nil || !self.pages.HasMorePages() {
			return nil, io.EOF
		}

		// Read the next page of keys
		page, err := self.pages.NextPage(ctx)
		if err != nil {
			return nil, self.objectError("list", strings.TrimPrefix(self.dir, self.prefix), err)
		}
		self.add(page)
	}
}

// Close the iterator. Any further calls to Next return fs.ErrClosed
func (self *listIterator) Close() error {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.pages, self.pending, self.closed = nil, nil, true
	self.finish(nil)
	return nil
}

// finish ends the listing's span, with the error, once, which must be locked
func (self *listIterator) finish(err error) {
	if self.end != nil {
		self.end(err)
		self.end = nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// add adds the objects of the page, which are its keys and common prefixes,
// in the order of their keys, with the directories which aren't returned
// yet before the first key beneath them, skipping those which sort before
// or at the last object returned, and keys which aren't paths of objects
func (self *listIterator) add(page *s3.ListObjectsV2Output) {
	entries := make([]listEntry, 0, len(page.Contents)+len(page.CommonPrefixes))
	for i := range page.Contents {
		entries = append(entries, listEntry{key: aws.ToString(page.Contents[i].Key), object: &page.Contents[i]})
	}
	for _, prefix := range page.CommonPrefixes {
		entries = append(entries, listEntry{key: aws.ToString(prefix.Prefix)})
	}
	slices.SortFunc(entries, func(a, b listEntry) int {
		return strings.Compare(a.key, b.key)
	})

	for _, entry := range entries {
		rel, ok := strings.CutPrefix(entry.key, self.dir)
		if !ok || rel == "" {
			continue
		}

		// The directories the key is beneath, which aren't returned yet, up
		// to an element which isn't one of a path, such as "" or "..", when
		// the key isn't an object, so a directory with only such keys is
		// listed, as it is when they roll up into it
		elems := strings.Split(strings.TrimSuffix(rel, "/"), "/")
		dir := self.dir
		for _, elem := range elems[:len(elems)-1] {
			if !isElem(elem) {
				break
			}
			dir += elem + "/"
			self.addDir(dir)
		}
		if !isRelPath(rel) {
			continue
		}

		// A common prefix, or a key which ends with "/", is a directory, and
		// otherwise it's a file
		if entry.object == nil || strings.HasSuffix(rel, "/") {
			self.addDir(entry.key)
		} else if entry.key > self.last {
			self.pending = append(self.pending, self.object(entry.key, entry.object))
			self.last = entry.key
		}
	}
}

// addDir adds the directory of the key, which ends with "/", unless it's
// returned already, or it sorts before where the listing starts
func (self *listIterator) addDir(key string) {
	if key > self.last {
		self.pending = append(self.pending, dirObject(self.path(key)))
		self.last = key
	}
}

// object returns the file of the key, from its listing, whose type is by
// its extension
func (self *S3) object(key string, object *s3types.Object) *schema.Object {
	p := self.path(key)
	return &schema.Object{
		ObjectKey:  schema.ObjectKey{Path: p},
		ObjectMeta: schema.ObjectMeta{ContentType: mime.TypeByExtension(path.Ext(p))},
		ObjectAttr: schema.ObjectAttr{
			Size:    object.Size,
			ETag:    etag(object.ETag),
			ModTime: aws.ToTime(object.LastModified),
		},
	}
}

// headObject returns the file of the path, which is relative to the prefix,
// for the operation
func (self *S3) headObject(ctx context.Context, op, rel string) (*schema.Object, error) {
	key := self.prefix + rel
	response, err := self.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(self.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, self.objectError(op, rel, err)
	}
	object := self.object(key, &s3types.Object{
		Size:         response.ContentLength,
		ETag:         response.ETag,
		LastModified: response.LastModified,
	})

	// The type is the extension's, as it is in a listing, or the object's,
	// when the extension isn't known
	if object.ContentType == "" {
		object.ContentType = aws.ToString(response.ContentType)
	}

	// The user's metadata, which a listing doesn't have
	object.Meta = userMeta(response.Metadata)
	return object, nil
}

// key returns the key of an object's path, which is relative to the prefix,
// or the prefix, when the path is empty
func (self *S3) key(path string) string {
	if path = strings.TrimPrefix(path, "/"); path == "" {
		return ""
	}
	return self.prefix + path
}

// path returns the path of the object of a key, which is beneath the prefix,
// without the "/" of a directory
func (self *S3) path(key string) string {
	return "/" + strings.TrimSuffix(strings.TrimPrefix(key, self.prefix), "/")
}

// isRelPath returns true when the key, which is relative to a directory, is
// the path of an object, with or without a "/" at its end, so it has no
// elements which are empty, or "." or ".."
func isRelPath(rel string) bool {
	for _, elem := range strings.Split(strings.TrimSuffix(rel, "/"), "/") {
		if !isElem(elem) {
			return false
		}
	}
	return true
}

// isElem returns true when the element of a key is one of a path, so it
// isn't empty, or "." or ".."
func isElem(elem string) bool {
	return elem != "" && elem != "." && elem != ".."
}

// etag returns the entity tag, without its quotes
func etag(value *string) *string {
	if value == nil {
		return nil
	}
	return types.Ptr(strings.Trim(*value, `"`))
}
