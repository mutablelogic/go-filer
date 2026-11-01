package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"mime"
	"path"
	"strings"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// maxParts is the most parts of an object which S3 accepts
	maxParts = 10000
)

// partSize is the size of each part of an object which is uploaded, which is
// read into memory, so an object which is no larger is uploaded in one
// request, and a larger one in parts, so at most maxParts times it
var partSize = 8 << 20

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// CreateObject creates the file at the path, or replaces it, with the body,
// as it's read, which is a nil body for an empty file, and the type, which is
// its extension's, or the request's, when its extension isn't known, and the
// user's metadata (see s3Metadata). When IfNotExists is set, a file which
// exists isn't replaced, which is ErrConflict, and when IfExists is set, a
// file which doesn't exist isn't created, which is ErrPreconditionFailed, as
// they're the conditions of the request which writes it. A file is replaced
// once it's written, so a body which can't be read leaves it as it was. Its
// modification time is when it's written, as S3 sets it. Returns the file.
func (self *S3) CreateObject(ctx context.Context, req schema.CreateObjectRequest) (_ *schema.Object, err error) {
	ctx, end := self.span(ctx, "CreateObject", req)
	defer func() { end(err) }()

	// The volume can be written, and the path is a file's
	rel := strings.Trim(path.Clean("/"+req.Path), "/")
	if self.Readonly() {
		return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
	} else if rel == "" || strings.HasSuffix(req.Path, "/") || !isRelPath(rel) {
		return nil, &fs.PathError{Op: "create", Path: req.Path, Err: fs.ErrInvalid}
	} else if req.IfNotExists && req.IfExists {
		return nil, gofiler.ErrBadParameter.With("IfNotExists and IfExists can't both be set")
	}

	// The type is the extension's, as it is when it's returned, unless the
	// extension isn't known, and the user's metadata
	contentType := mime.TypeByExtension(path.Ext(rel))
	if contentType == "" {
		contentType = req.ContentType
	} else if !sameType(req.ContentType, contentType) {
		return nil, gofiler.ErrNotImplemented.Withf("Content type %q isn't supported, as the type of %q is from its extension", req.ContentType, req.Path)
	}
	metadata, err := s3Metadata(req.Meta)
	if err != nil {
		return nil, err
	}

	// The conditions of the request which writes the file: none must exist,
	// or the one which exists must be replaced
	upload := &upload{S3: self, key: self.prefix + rel, contentType: contentType, metadata: metadata}
	if req.IfNotExists {
		upload.ifNoneMatch = aws.String("*")
	} else if req.IfExists {
		if object, err := self.headObject(ctx, "create", rel); isNotExist(err) {
			return nil, gofiler.ErrPreconditionFailed.Withf("Object doesn't exist: %q", req.Path)
		} else if err != nil {
			return nil, err
		} else {
			upload.ifMatch = aws.String(`"` + aws.ToString(object.ETag) + `"`)
		}
	}

	// Write the file, when the conditions are met
	if err := upload.write(ctx, req.Body); errors.Is(err, gofiler.ErrPreconditionFailed) && req.IfNotExists {
		return nil, gofiler.ErrConflict.Withf("Object already exists: %q", req.Path)
	} else if errors.Is(err, gofiler.ErrPreconditionFailed) {
		return nil, gofiler.ErrPreconditionFailed.Withf("Object changed while it was written: %q", req.Path)
	} else if err != nil {
		return nil, err
	}

	// Return the file
	return self.headObject(ctx, "create", rel)
}

////////////////////////////////////////////////////////////////////////////////
// TYPES

// upload writes a file, in one request, or in parts, when it's larger than
// a part, with the conditions of the request which writes it
type upload struct {
	*S3
	key         string
	contentType string
	metadata    map[string]string
	ifMatch     *string
	ifNoneMatch *string
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// write writes the file with the body, which is uploaded in one request,
// when it's no larger than a part, or otherwise in parts, as it's read,
// which are aborted when the body can't be read, or the upload fails
func (self *upload) write(ctx context.Context, body io.Reader) error {
	if body == nil {
		body = bytes.NewReader(nil)
	}
	part := make([]byte, partSize)
	n, eof, err := readPart(body, part)
	if err != nil {
		return err
	} else if eof {
		return self.put(ctx, part[:n])
	}
	return self.multipart(ctx, body, part, n)
}

// put writes the file in one request
func (self *upload) put(ctx context.Context, data []byte) error {
	_, err := self.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(self.bucket),
		Key:           aws.String(self.key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   contentType(self.contentType),
		Metadata:      self.metadata,
		IfMatch:       self.ifMatch,
		IfNoneMatch:   self.ifNoneMatch,
	})
	return self.uploadError(err)
}

// multipart writes the file in parts, the first of which is read, and is
// completed with the conditions, or aborted when it fails
func (self *upload) multipart(ctx context.Context, body io.Reader, part []byte, n int) (err error) {
	response, err := self.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(self.bucket),
		Key:         aws.String(self.key),
		ContentType: contentType(self.contentType),
		Metadata:    self.metadata,
	})
	if err != nil {
		return self.uploadError(err)
	}
	uploadId := response.UploadId

	// Abort the upload when it fails, which isn't cancelled with it, so its
	// parts aren't kept
	defer func() {
		if err != nil {
			_, abortErr := self.client.AbortMultipartUpload(context.WithoutCancel(ctx), &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(self.bucket),
				Key:      aws.String(self.key),
				UploadId: uploadId,
			})
			err = errors.Join(err, self.uploadError(abortErr))
		}
	}()

	// Upload each part, as it's read
	var parts []s3types.CompletedPart
	for number := int32(1); ; number++ {
		if number > maxParts {
			return gofiler.ErrBadParameter.Withf("Object is larger than %d parts of %d bytes", maxParts, partSize)
		}
		response, err := self.client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        aws.String(self.bucket),
			Key:           aws.String(self.key),
			UploadId:      uploadId,
			PartNumber:    aws.Int32(number),
			Body:          bytes.NewReader(part[:n]),
			ContentLength: aws.Int64(int64(n)),
		})
		if err != nil {
			return self.uploadError(err)
		}
		parts = append(parts, s3types.CompletedPart{ETag: response.ETag, PartNumber: aws.Int32(number)})

		// Read the next part, unless it's the end of the body
		var eof bool
		if n, eof, err = readPart(body, part); err != nil {
			return err
		} else if eof && n == 0 {
			break
		} else if eof {
			part = part[:n]
		}
	}

	// Complete the upload, when the conditions are met
	_, err = self.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(self.bucket),
		Key:             aws.String(self.key),
		UploadId:        uploadId,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: parts},
		IfMatch:         self.ifMatch,
		IfNoneMatch:     self.ifNoneMatch,
	})
	return self.uploadError(err)
}

// uploadError returns the error of a request to write the file
func (self *upload) uploadError(err error) error {
	return self.objectError("create", strings.TrimPrefix(self.key, self.prefix), err)
}

// readPart reads the body into the part, until it's full, or the body ends,
// when eof is true. An error reading the body, such as one which ends before
// it should, is returned, rather than ending it.
func readPart(body io.Reader, part []byte) (n int, eof bool, err error) {
	for n < len(part) {
		m, err := body.Read(part[n:])
		n += m
		if errors.Is(err, io.EOF) {
			return n, true, nil
		} else if err != nil {
			return n, false, err
		}
	}
	return n, false, nil
}

// contentType returns the type, or nil, when it's empty
func contentType(value string) *string {
	if value == "" {
		return nil
	}
	return aws.String(value)
}

// sameType returns true when the content type of a request is empty, or the
// same media type as the type of the file, ignoring parameters, such as the
// charset, and case
func sameType(contentType, objectType string) bool {
	if contentType == "" {
		return true
	}
	want, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	got, _, err := mime.ParseMediaType(objectType)
	return err == nil && got == want
}
