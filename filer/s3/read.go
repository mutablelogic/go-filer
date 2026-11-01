package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// reader reads a file's content, from its offset, which can seek, so it can
// be read in ranges. The content is requested from the start before it's
// read, and again from the offset it seeks to, within the file, with the
// file's entity tag, so a file which changed since it was returned isn't
// read, which is known before a response is written. Seeking to the end, or
// beyond it, isn't requested, and a seek to where the content is requested
// from, such as back to the start, keeps it.
type reader struct {
	*S3
	ctx    context.Context
	rel    string        // the path of the file, relative to the prefix
	etag   string        // the entity tag of the file, which it must still have
	size   int64         // the size of the file
	offset int64         // where the next read is from
	at     int64         // where the content which is requested is read from
	body   io.ReadCloser // the content which is requested, or nil when it isn't
}

var _ io.ReadSeekCloser = (*reader)(nil)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ReadObject returns the function which reads the file at the path, and the
// file, whose content is read within the function, which can seek, so it
// can be read in ranges. A directory can't be read, which is fs.ErrInvalid,
// and a path which ends with "/" is one. Returns ErrNotModified when the
// request's conditions say the file hasn't changed. The file is read with
// its entity tag, so when it has changed since it was returned, it isn't
// read, and ErrPreconditionFailed is returned.
func (self *S3) ReadObject(ctx context.Context, req schema.GetObjectRequest) (_ gofiler.ReadObjectFunc, _ *schema.Object, err error) {
	ctx, end := self.span(ctx, "ReadObject", req)
	defer func() { end(err) }()

	object, err := self.stat(ctx, "read", req.Path)
	if err != nil {
		return nil, nil, err
	} else if object.IsDir {
		return nil, nil, &fs.PathError{Op: "read", Path: req.Path, Err: fs.ErrInvalid}
	} else if object.NotModified(req) {
		return nil, nil, gofiler.ErrNotModified.Withf("%q", object.Path)
	}

	// Return the function which reads the file, and the file
	rel := strings.TrimPrefix(object.Path, "/")
	return func(ctx context.Context, fn func(io.Reader) error) (err error) {
		ctx, end := self.span(ctx, "ReadObject.Read", req)
		defer func() { end(err) }()

		// The content is requested before it's read, so a file which changed
		// isn't read, before a response is written
		r := &reader{S3: self, ctx: ctx, rel: rel, etag: aws.ToString(object.ETag), size: aws.ToInt64(object.Size)}
		defer func() { err = errors.Join(err, r.Close()) }()
		if err := r.request(); err != nil {
			return err
		}
		return fn(r)
	}, object, nil
}

// Read reads the content from the offset, which is requested when it isn't,
// unless it's at the end of the file, or beyond it
func (r *reader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	} else if r.offset >= r.size {
		return 0, io.EOF
	} else if r.body == nil || r.at != r.offset {
		if err := r.request(); err != nil {
			return 0, err
		}
	}
	n, err := r.body.Read(data)
	r.offset += int64(n)
	r.at += int64(n)
	if err != nil && !errors.Is(err, io.EOF) {
		if ctxErr := r.ctx.Err(); ctxErr != nil {
			err = ctxErr
		} else {
			err = r.objectError("read", r.rel, err)
		}
	}
	return n, err
}

// Seek changes the offset of the next read, from the start or end of the
// file, or the offset, and the content is requested from it, unless it's
// requested from there, or it's at the end of the file, or beyond it, so a
// file which changed is known before a response of the range is written
func (r *reader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.offset
	case io.SeekEnd:
		offset += r.size
	default:
		return 0, &fs.PathError{Op: "seek", Path: "/" + r.rel, Err: fs.ErrInvalid}
	}
	if offset < 0 {
		return 0, &fs.PathError{Op: "seek", Path: "/" + r.rel, Err: fs.ErrInvalid}
	}
	r.offset = offset
	if r.offset < r.size && (r.body == nil || r.at != r.offset) {
		if err := r.request(); err != nil {
			return 0, err
		}
	}
	return r.offset, nil
}

// Close closes the content which is requested, if any
func (r *reader) Close() error {
	if r.body == nil {
		return nil
	}
	err := r.body.Close()
	r.body = nil
	return err
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// request requests the content from the offset, of the file with the
// entity tag, which is ErrPreconditionFailed when it has changed, in place of
// the content which is requested
func (r *reader) request() error {
	if err := r.Close(); err != nil {
		return err
	}
	input := &s3.GetObjectInput{
		Bucket:  aws.String(r.bucket),
		Key:     aws.String(r.prefix + r.rel),
		IfMatch: aws.String(`"` + r.etag + `"`),
	}
	if r.offset > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-", r.offset))
	}
	response, err := r.client.GetObject(r.ctx, input)
	if err != nil {
		return r.objectError("read", r.rel, err)
	}
	r.body, r.at = response.Body, r.offset
	return nil
}
