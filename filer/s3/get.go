package s3

import (
	"context"
	"io/fs"
	"path"
	"strings"

	// Packages
	aws "github.com/aws/aws-sdk-go-v2/aws"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// GetObject returns the metadata of the object at the path, which is a file,
// or a directory, which exists when a key is beneath it, or the root, which
// always exists. A path which ends with "/" is a directory, even when a file
// has its name. Returns ErrNotModified when the request's conditions say the
// object hasn't changed (see schema.Object.NotModified), and fs.ErrNotExist
// when it doesn't exist.
func (self *S3) GetObject(ctx context.Context, req schema.GetObjectRequest) (_ *schema.Object, err error) {
	ctx, end := self.span(ctx, "GetObject", req)
	defer func() { end(err) }()

	object, err := self.stat(ctx, "get", req.Path)
	if err != nil {
		return nil, err
	} else if object.NotModified(req) {
		return nil, gofiler.ErrNotModified.Withf("%q", object.Path)
	}
	return object, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// stat returns the object at the path, for the operation, which is a file,
// unless the path ends with "/", or a directory, when a key is beneath it, or
// the root, without a request. Returns fs.ErrNotExist when it doesn't exist.
func (self *S3) stat(ctx context.Context, op, p string) (*schema.Object, error) {
	rel := strings.Trim(path.Clean("/"+p), "/")
	if rel == "" {
		return dirObject("/"), nil
	}

	// The path is a file, unless it ends with "/"
	if !strings.HasSuffix(p, "/") {
		if object, err := self.headObject(ctx, op, rel); err == nil {
			return object, nil
		} else if !isNotExist(err) {
			return nil, err
		}
	}

	// The path is a directory, which exists when a key is beneath it
	if response, err := self.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(self.bucket),
		Prefix:  aws.String(self.prefix + rel + "/"),
		MaxKeys: aws.Int32(1),
	}); err != nil {
		return nil, self.objectError(op, rel, err)
	} else if len(response.Contents) == 0 {
		return nil, &fs.PathError{Op: op, Path: "/" + rel, Err: fs.ErrNotExist}
	}
	return dirObject("/" + rel), nil
}

// dirObject returns the directory of the path
func dirObject(path string) *schema.Object {
	return &schema.Object{
		ObjectKey:  schema.ObjectKey{Path: path},
		ObjectMeta: schema.ObjectMeta{ContentType: schema.DirectoryType},
		ObjectAttr: schema.ObjectAttr{IsDir: true},
	}
}
