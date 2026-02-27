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

// ListObjects lists objects in the backend.
// If a single object exists at the path, it returns just that object.
// Otherwise, it treats the path as a prefix and lists all matching objects.
// Use Recursive=true to list nested objects, or Recursive=false for immediate children only.
func (b *blobbackend) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (*schema.ListObjectsResponse, error) {
	sk := b.key(req.Path)

	// Response
	response := schema.ListObjectsResponse{
		Name: b.Name(),
	}

	// Check if this path refers to a single real object (not a phantom directory)
	if attrs := b.isRealObject(ctx, sk); attrs != nil {
		obj := b.attrsToObject(cleanPath(req.Path), attrs)
		response.Body = append(response.Body, *obj)
		return &response, nil
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
			Path:    b.pathFromStorageKey(obj.Key),
			Size:    obj.Size,
			ModTime: obj.ModTime,
		}
		if len(obj.MD5) > 0 {
			o.ETag = fmt.Sprintf("%x", obj.MD5)
		}
		response.Body = append(response.Body, o)
	}

	return &response, nil
}
