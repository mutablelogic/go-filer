package s3

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type rangeReadCloser struct {
	ctx    context.Context
	client *s3svc.Client
	bucket string
	key    string
	size   int64
	pos    int64
	body   io.ReadCloser
	closed bool
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Read object content from the backend. Caller must close the returned reader.
func (self *S3Backend) ReadObject(ctx context.Context, req schema.GetObjectRequest) (_ io.ReadCloser, _ *schema.Object, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "s3.ReadObject",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Get the object metadata
	object, err := self.GetObject(ctx, req)
	if err != nil {
		return nil, nil, err
	} else if object.ContentType == schema.ContentTypeDirectory {
		return nil, nil, gofiler.ErrBadParameter.Withf("cannot read content of a directory: %q", req.Path)
	}

	// Return a range-backed reader so metadata extractors can seek within large
	// remote objects without forcing a full S3 download into memory.
	basePrefix := strings.TrimPrefix(strings.TrimSuffix(self.url.Path, "/"), "/")
	size := int64(-1)
	if object.Size != nil {
		size = types.Value(object.Size)
	}
	return &rangeReadCloser{
		ctx:    ctx,
		client: self.client,
		bucket: self.url.Host,
		key:    s3KeyFromPath(req.Path, basePrefix),
		size:   size,
	}, object, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (r *rangeReadCloser) Read(data []byte) (int, error) {
	if r.closed {
		return 0, fs.ErrClosed
	}
	if len(data) == 0 {
		return 0, nil
	}
	if r.size >= 0 && r.pos >= r.size {
		return 0, io.EOF
	}
	if r.size >= 0 && int64(len(data)) > r.size-r.pos {
		data = data[:int(r.size-r.pos)]
	}
	for {
		if r.body == nil {
			if err := r.open(len(data)); err != nil {
				return 0, err
			}
		}
		n, err := r.body.Read(data)
		r.pos += int64(n)
		if err == io.EOF {
			_ = r.body.Close()
			r.body = nil
			if r.size < 0 || r.pos >= r.size {
				return n, err
			}
			if n == 0 {
				continue
			}
			return n, nil
		}
		return n, err
	}
}

func (r *rangeReadCloser) Seek(offset int64, whence int) (int64, error) {
	if r.closed {
		return 0, fs.ErrClosed
	}

	var pos int64
	switch whence {
	case io.SeekStart:
		pos = offset
	case io.SeekCurrent:
		pos = r.pos + offset
	case io.SeekEnd:
		if r.size < 0 {
			return 0, fmt.Errorf("s3 seek end: object size is unknown")
		}
		pos = r.size + offset
	default:
		return 0, fmt.Errorf("s3 seek: invalid whence %d", whence)
	}
	if pos < 0 {
		return 0, fmt.Errorf("s3 seek: negative position %d", pos)
	}
	if r.body != nil {
		_ = r.body.Close()
		r.body = nil
	}
	r.pos = pos
	return r.pos, nil
}

func (r *rangeReadCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.body != nil {
		return r.body.Close()
	}
	return nil
}

func (r *rangeReadCloser) open(size int) error {
	if size <= 0 {
		return nil
	}
	rangeEnd := r.pos + int64(size) - 1
	if r.size >= 0 && rangeEnd >= r.size {
		rangeEnd = r.size - 1
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", r.pos, rangeEnd)
	out, err := r.client.GetObject(r.ctx, &s3svc.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return err
	}
	r.body = out.Body
	return nil
}
