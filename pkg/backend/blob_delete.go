package backend

import (
	"context"
	"fmt"
	"io"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
	blob "gocloud.dev/blob"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// DeleteObject deletes an object
func (b *blobbackend) DeleteObject(ctx context.Context, req schema.DeleteObjectRequest) (*schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)

	// Attributes may not exist, continue with delete
	attrs, err := b.bucket.Attributes(ctx, sk)
	if err != nil {
		attrs = nil
	}

	// Perform delete
	if err := b.bucket.Delete(ctx, sk); err != nil {
		return nil, blobErr(err, b.Name()+":"+objPath)
	}

	obj := &schema.Object{Name: b.Name(), Path: objPath}
	if attrs != nil {
		obj = b.attrsToObject(objPath, attrs)
		obj.Name = b.Name()
	}
	return obj, nil
}

// DeleteObjects deletes objects in the backend.
// If a single object exists at the path, it deletes just that object.
// Otherwise, it treats the path as a prefix and deletes all matching objects.
// Use Recursive=true to delete nested objects, or Recursive=false for immediate children only.
func (b *blobbackend) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	sk := b.key(req.Path)

	// Response
	response := schema.DeleteObjectsResponse{
		Name: b.Name(),
	}

	// Check if this path refers to a single real object (not a phantom directory)
	if b.isRealObject(ctx, sk) != nil {
		if obj, err := b.DeleteObject(ctx, schema.DeleteObjectRequest{Path: req.Path}); err != nil {
			return nil, err
		} else if obj != nil {
			response.Body = append(response.Body, *obj)
		}
		return &response, nil
	}

	// Object doesn't exist (or key is empty for root), treat as prefix
	prefix := strings.TrimSuffix(sk, "/")
	if prefix != "" {
		prefix = prefix + "/"
	}

	// List and delete objects with prefix
	var delim string
	if !req.Recursive {
		delim = "/"
	}

	// Keep listing and deleting until no more objects match
	for {
		iter := b.bucket.List(&blob.ListOptions{
			Prefix:    prefix,
			Delimiter: delim,
		})

		deletedInPass := 0
		for {
			obj, err := iter.Next(ctx)
			if err == io.EOF {
				break
			} else if err != nil {
				return &response, blobErr(err, b.Name()+":"+req.Path)
			}

			// Skip the prefix itself and directories (when non-recursive)
			if obj.Key == prefix || obj.IsDir {
				continue
			}

			objPath := b.pathFromStorageKey(obj.Key)

			// Delete the object
			if err := b.bucket.Delete(ctx, obj.Key); err != nil {
				return &response, blobErr(err, b.Name()+":"+objPath)
			}

			// Add to response
			o := schema.Object{
				Path:    objPath,
				Size:    obj.Size,
				ModTime: obj.ModTime,
			}
			if len(obj.MD5) > 0 {
				o.ETag = fmt.Sprintf("%x", obj.MD5)
			}
			response.Body = append(response.Body, o)
			deletedInPass++
		}

		// If no objects were deleted in this pass, we're done
		if deletedInPass == 0 {
			break
		}
	}

	return &response, nil
}
