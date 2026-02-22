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

// ListObjects lists objects in the backend.
// If a single object exists at the URL, it returns just that object.
// Otherwise, it treats the URL as a prefix and lists all matching objects.
// Use Recursive=true to list nested objects, or Recursive=false for immediate children only.
func (b *blobbackend) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (*schema.ListObjectsResponse, error) {
	u, err := url.Parse(req.URL)
	if err != nil {
		return nil, err
	}

	// Validate the URL matches this backend
	key := b.Key(u)
	if key == "" {
		return nil, httpresponse.ErrBadRequest.Withf("URL %q not handled by this backend", req.URL)
	}
	sk := b.storageKey(key)

	// Response
	response := schema.ListObjectsResponse{
		URL: req.URL,
	}

	// If key is non-empty and doesn't end with /, check if object exists at this exact path
	// Also check it's not a "phantom directory" (size=0 pseudo-object created by some S3 implementations)
	if sk != "" && !strings.HasSuffix(sk, "/") {
		if obj, err := b.GetObject(ctx, schema.GetObjectRequest{URL: req.URL}); err == nil && obj != nil {
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
			return nil, blobErr(err, req.URL)
		}

		// Skip the prefix itself
		if obj.Key == prefix {
			continue
		}

		// Build the full URL for the object
		objURL := fmt.Sprintf("%s://%s/%s", b.url.Scheme, b.url.Host, obj.Key)

		o := schema.Object{
			URL:     objURL,
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
