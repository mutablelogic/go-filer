package backend

import (
	"context"
	"fmt"
	"io"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/pkg/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	blob "gocloud.dev/blob"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// DeleteObject deletes an object
func (b *blobbackend) DeleteObject(ctx context.Context, req schema.DeleteObjectRequest) (*schema.Object, error) {
	// Compute key using the request path
	key := b.Key(req.Path)
	if key == "" {
		return nil, httpresponse.ErrBadRequest.Withf("path %q not handled by backend %q", req.Path, b.Name())
	}
	sk := b.storageKey(key)

	// Attributes may not exist, continue with delete
	attrs, err := b.bucket.Attributes(ctx, sk)
	if err != nil {
		attrs = nil
	}

	// Perform delete
	if err := b.bucket.Delete(ctx, sk); err != nil {
		return nil, blobErr(err, b.Name()+":"+key)
	} else if attrs != nil {
		return b.attrsToObject(b.Name(), key, attrs), nil
	}

	// Return empty object in the case there is no attributes
	return &schema.Object{Name: b.Name(), Path: key}, nil
}

// DeleteObjects deletes objects in the backend.
// If a single object exists at the path, it deletes just that object.
// Otherwise, it treats the path as a prefix and deletes all matching objects.
// Use Recursive=true to delete nested objects, or Recursive=false for immediate children only.
func (b *blobbackend) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	// Compute key using the request path
	key := b.Key(req.Path)
	if key == "" {
		return nil, httpresponse.ErrBadRequest.Withf("path %q not handled by backend %q", req.Path, b.Name())
	}
	sk := b.storageKey(key)

	// Response
	response := schema.DeleteObjectsResponse{
		Name: b.Name(),
	}

	// If key is non-empty and doesn't end with /, check if object exists at this exact path
	// Also check it's not a "phantom directory" (size=0 pseudo-object created by some S3 implementations)
	if sk != "" && !strings.HasSuffix(sk, "/") {
		if attrs, err := b.bucket.Attributes(ctx, sk); err == nil {
			// If object has content (size > 0), delete just this one regardless of children
			// If size is 0, it might be a phantom directory - check for children
			if attrs.Size > 0 {
				if obj, err := b.DeleteObject(ctx, schema.DeleteObjectRequest{Path: req.Path}); err != nil {
					return nil, err
				} else if obj != nil {
					response.Body = append(response.Body, *obj)
				}
				return &response, nil
			}

			// Size is 0 - check if there are objects with this key as a prefix
			iter := b.bucket.List(&blob.ListOptions{
				Prefix: sk + "/",
			})
			if _, err := iter.Next(ctx); err == io.EOF {
				// No children - this is a real (empty) object, delete it
				if obj, err := b.DeleteObject(ctx, schema.DeleteObjectRequest{Path: req.Path}); err != nil {
					return nil, err
				} else if obj != nil {
					response.Body = append(response.Body, *obj)
				}
				return &response, nil
			}
			// Has children and size=0 - treat as phantom directory, fall through to bulk delete
		}
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
				return nil, blobErr(err, b.Name()+":"+req.Path)
			}

			// Skip the prefix itself and directories (when non-recursive)
			if obj.Key == prefix || obj.IsDir {
				continue
			}

			objPath := b.pathFromStorageKey(obj.Key)

			// Delete the object
			if err := b.bucket.Delete(ctx, obj.Key); err != nil {
				return nil, blobErr(err, b.Name()+":"+objPath)
			}

			// Add to response
			o := schema.Object{
				Name:    b.Name(),
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
