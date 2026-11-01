package s3

import (
	"context"
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
// PUBLIC METHODS

// DeleteObjects deletes the file at the path, or the directory, when the
// request is recursive, with every key beneath it, including those which
// aren't paths of objects, and keys which are directories, and returns the
// files which are deleted. A directory which isn't recursive is ErrConflict,
// and the root of the volume can't be deleted. A directory which a key ends
// with "/" isn't deleted when a file beneath it is, as it's a key of its own.
// When a key beneath a directory can't be deleted, the error is returned,
// once the keys before it, and the others of its page, of up to 1,000 keys,
// are, but those after them aren't.
func (self *S3) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (_ *schema.DeleteObjectsResponse, err error) {
	ctx, end := self.span(ctx, "DeleteObjects", req)
	defer func() { end(err) }()

	// The volume can be written, and the path isn't its root
	rel := strings.Trim(path.Clean("/"+req.Path), "/")
	if self.Readonly() {
		return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
	} else if rel == "" {
		return nil, gofiler.ErrBadParameter.Withf("Cannot delete the root of volume %q", self.name)
	}

	// The file, or the directory, which must be recursive
	object, err := self.stat(ctx, "delete", req.Path)
	if err != nil {
		return nil, err
	} else if object.IsDir && !req.Recursive {
		return nil, gofiler.ErrConflict.Withf("Cannot delete directory %q unless recursive", req.Path)
	} else if object.IsDir {
		objects, err := self.deleteBeneath(ctx, rel)
		if err != nil {
			return nil, err
		}
		return &schema.DeleteObjectsResponse{Body: objects}, nil
	}

	// Delete the file
	if _, err := self.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(self.bucket),
		Key:    aws.String(self.prefix + rel),
	}); err != nil {
		return nil, self.objectError("delete", rel, err)
	}
	return &schema.DeleteObjectsResponse{Body: []*schema.Object{deletedObject(object)}}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// deleteBeneath deletes every key beneath the directory, a page at a time,
// each of which is deleted in one request, and returns the files which are
// deleted, which are those which are paths of objects
func (self *S3) deleteBeneath(ctx context.Context, rel string) ([]*schema.Object, error) {
	dir := self.prefix + rel + "/"
	pages := s3.NewListObjectsV2Paginator(self.client, &s3.ListObjectsV2Input{
		Bucket:  aws.String(self.bucket),
		Prefix:  aws.String(dir),
		MaxKeys: aws.Int32(listPageSize),
	})
	result := []*schema.Object{}
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, self.objectError("delete", rel, err)
		} else if len(page.Contents) == 0 {
			continue
		}

		// Delete the page's keys
		keys := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			keys = append(keys, s3types.ObjectIdentifier{Key: object.Key})
		}
		response, err := self.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(self.bucket),
			Delete: &s3types.Delete{Objects: keys, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return nil, self.objectError("delete", rel, err)
		} else if len(response.Errors) > 0 {
			return nil, self.deleteError(response.Errors[0])
		}

		// The files which are deleted, which aren't directories, or keys which
		// aren't paths of objects
		for i := range page.Contents {
			key := aws.ToString(page.Contents[i].Key)
			if name := strings.TrimPrefix(key, dir); !strings.HasSuffix(key, "/") && isRelPath(name) {
				result = append(result, deletedObject(self.object(key, &page.Contents[i])))
			}
		}
	}
	return result, nil
}

// deleteError returns the error of a key which can't be deleted
func (self *S3) deleteError(err s3types.Error) error {
	rel := strings.TrimPrefix(aws.ToString(err.Key), self.prefix)
	if aws.ToString(err.Code) == "AccessDenied" {
		return gofiler.ErrForbidden.Withf("delete /%s: access denied", rel)
	}
	return gofiler.ErrServiceUnavailable.Withf("delete /%s: %s", rel, aws.ToString(err.Message))
}

// deletedObject returns the file which is deleted, without its type or
// metadata, which a response of a delete doesn't have
func deletedObject(object *schema.Object) *schema.Object {
	object.ContentType, object.Meta = "", nil
	return object
}
