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

// ListObjects lists objects in the backend.
// If a single object exists at the path, it returns just that object.
// Otherwise, it treats the path as a prefix and lists all matching objects.
// Use Recursive=true to list nested objects, or Recursive=false for immediate children only.
func (b *blobbackend) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (*schema.ListObjectsResponse, error) {
	// Compute key using the request path
	key := b.Key(req.Path)
	if key == "" {
		return nil, httpresponse.ErrBadRequest.Withf("path %q not handled by backend %q", req.Path, b.Name())
	}
	sk := b.storageKey(key)

	// Response
	response := schema.ListObjectsResponse{
		Name: b.Name(),
	}

	// If key is non-empty and doesn't end with /, check if object exists at this exact path
	// Also check it's not a "phantom directory" (size=0 pseudo-object created by some S3 implementations)
	if sk != "" && !strings.HasSuffix(sk, "/") {
		if obj, err := b.GetObject(ctx, schema.GetObjectRequest{Path: req.Path}); err == nil && obj != nil {
			// If object has content (size > 0), return just this one regardless of children
			// If size is 0, it might be a phantom directory - check for children
			if obj.Size > 0 {
				appendObject(&response, *obj)
				return &response, nil
			}

			// Size is 0 - check if there are objects with this key as a prefix
			iter := b.bucket.List(&blob.ListOptions{
				Prefix: sk + "/",
			})
			if _, err := iter.Next(ctx); err == io.EOF {
				// No children - this is a real (empty) object, return it
				appendObject(&response, *obj)
				return &response, nil
			}
			// Has children and size=0 - treat as phantom directory, fall through to listing
		}
	}

	// Object doesn't exist (or key is empty for root), treat as prefix
	prefix := strings.TrimSuffix(sk, "/")
	if prefix != "" {
		prefix = prefix + "/"
	}

	// List objects with prefix
	var delim string
	if !req.Recursive {
		delim = "/"
	}
	iter := b.bucket.List(&blob.ListOptions{
		Prefix:    prefix,
		Delimiter: delim,
	})

	for {
		obj, err := iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, blobErr(err, b.Name()+":"+req.Path)
		}

		// Skip the prefix itself
		if obj.Key == prefix {
			continue
		}

		o := schema.Object{
			Name:    b.Name(),
			Path:    b.pathFromStorageKey(obj.Key),
			Size:    obj.Size,
			ModTime: obj.ModTime,
		}
		if len(obj.MD5) > 0 {
			o.ETag = fmt.Sprintf("%x", obj.MD5)
		}
		appendObject(&response, o)
	}

	return &response, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func appendObject(resp *schema.ListObjectsResponse, obj schema.Object) {
	resp.Body = append(resp.Body, obj)
}
