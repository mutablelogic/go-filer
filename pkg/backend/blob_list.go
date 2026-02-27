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
// Response.Count reflects the total number of matches before Offset/Limit are applied.
// Limit=0 returns only the count (Body is nil); Limit>0 returns up to Limit objects starting at Offset.
func (b *blobbackend) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (*schema.ListObjectsResponse, error) {
	sk := b.key(req.Path)

	// Collect all matching objects first so Count can reflect the full result set
	// before Offset/Limit pagination is applied.
	var all []schema.Object

	// Check if this path refers to a single real object (not a phantom directory)
	attrs, err := b.isRealObject(ctx, sk)
	if err != nil {
		return nil, err
	}
	if attrs != nil {
		obj := b.attrsToObject(cleanPath(req.Path), attrs)
		obj.Name = b.Name()
		all = []schema.Object{*obj}
	} else {
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
			all = append(all, o)
		}
	}

	// Apply Offset and Limit.
	start := req.Offset
	if start < 0 {
		start = 0
	}
	if start > len(all) {
		start = len(all)
	}
	page := all[start:]
	if req.Limit > 0 && req.Limit < len(page) {
		page = page[:req.Limit]
	}

	// Limit==0 is count-only: return the total with no body.
	if req.Limit == 0 {
		page = nil
	}

	return &schema.ListObjectsResponse{
		Name:  b.Name(),
		Count: len(all),
		Body:  page,
	}, nil
}
