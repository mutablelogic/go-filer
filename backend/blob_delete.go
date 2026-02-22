package backend

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	blob "gocloud.dev/blob"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// DeleteObject deletes an object
func (b *blobbackend) DeleteObject(ctx context.Context, req schema.DeleteObjectRequest) (*schema.Object, error) {
	// Parse the URL
	u, err := url.Parse(req.URL)
	if err != nil {
		return nil, err
	}

	// Validate the URL matches this backend
	key := b.Path(u)
	if key == "" {
		return nil, httpresponse.ErrBadRequest.Withf("URL %q not handled by this backend", req.URL)
	}

	// Attributes may not exist, continue with delete
	attrs, err := b.bucket.Attributes(ctx, key)
	if err != nil {
		attrs = nil
	}

	// Perform delete
	if err := b.bucket.Delete(ctx, key); err != nil {
		return nil, blobErr(err, req.URL)
	} else if attrs != nil {
		return b.attrsToObject(req.URL, attrs), nil
	}

	// Return empty object in the case there is no attributes
	return &schema.Object{URL: req.URL}, nil
}

// DeleteObjects deletes objects in the backend.
// If a single object exists at the URL, it deletes just that object.
// Otherwise, it treats the URL as a prefix and deletes all matching objects.
// Use Recursive=true to delete nested objects, or Recursive=false for immediate children only.
func (b *blobbackend) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	u, err := url.Parse(req.URL)
	if err != nil {
		return nil, err
	}

	// Get the relative path within this backend
	key := b.Path(u)

	// Response
	response := schema.DeleteObjectsResponse{
		URL: req.URL,
	}

	// If key is non-empty and doesn't end with /, check if object exists at this exact path
	// Also check it's not a "phantom directory" (size=0 pseudo-object created by some S3 implementations)
	if key != "" && !strings.HasSuffix(key, "/") {
		if attrs, err := b.bucket.Attributes(ctx, key); err == nil {
			// If object has content (size > 0), delete just this one regardless of children
			// If size is 0, it might be a phantom directory - check for children
			if attrs.Size > 0 {
				if obj, err := b.DeleteObject(ctx, schema.DeleteObjectRequest{URL: req.URL}); err != nil {
					return nil, err
				} else if obj != nil {
					response.Body = append(response.Body, *obj)
				}
				return &response, nil
			}

			// Size is 0 - check if there are objects with this key as a prefix
			iter := b.bucket.List(&blob.ListOptions{
				Prefix: key + "/",
			})
			if _, err := iter.Next(ctx); err == io.EOF {
				// No children - this is a real (empty) object, delete it
				if obj, err := b.DeleteObject(ctx, schema.DeleteObjectRequest{URL: req.URL}); err != nil {
					return nil, err
				} else if obj != nil {
					response.Body = append(response.Body, *obj)
				}
				return &response, nil
			}
			// Has children and size=0 - treat as phantom directory, fall through to bulk delete
		}
	}

	// Validate this backend handles the URL (key is empty only for root of this backend)
	if key == "" && u.Host != b.url.Host {
		return nil, httpresponse.ErrBadRequest.Withf("URL %q not handled by this backend", req.URL)
	}

	// Object doesn't exist (or key is empty for root), treat as prefix
	prefix := strings.TrimSuffix(key, "/")
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
				return nil, blobErr(err, req.URL)
			}

			// Skip the prefix itself and directories (when non-recursive)
			if obj.Key == prefix || obj.IsDir {
				continue
			}

			// Build the full URL for the object
			fullPath := obj.Key
			if b.prefix != "" {
				fullPath = b.prefix + "/" + obj.Key
			}
			objURL := fmt.Sprintf("%s://%s/%s", b.url.Scheme, b.url.Host, fullPath)

			// Delete the object
			if err := b.bucket.Delete(ctx, obj.Key); err != nil {
				return nil, blobErr(err, objURL)
			}

			// Add to response
			o := schema.Object{
				URL:     objURL,
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
