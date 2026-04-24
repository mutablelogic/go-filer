package blob

import (
	"context"
	"fmt"
	"io"
	"strings"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	attribute "go.opentelemetry.io/otel/attribute"
	blob "gocloud.dev/blob"
	gcerrors "gocloud.dev/gcerrors"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// DeleteObject deletes an object
func (b *backend) DeleteObject(ctx context.Context, req schema.DeleteObjectRequest) (*schema.Object, error) {
	sk := b.key(req.Path)
	objPath := cleanPath(req.Path)
	deleteKey := sk
	candidates := b.storageKeyCandidates(sk)
	addSpanAttrs(ctx,
		attribute.String("blob.path", objPath),
		attribute.String("blob.storage_key", sk),
		attribute.String("blob.storage_candidates", strings.Join(candidates, ",")),
	)

	// Fetch attributes to return in the response.
	// Only NotFound is tolerated (the object may have already been deleted);
	// other errors (e.g. PermissionDenied) are propagated.
	var attrs *blob.Attributes
	for _, candidate := range candidates {
		a, err := b.bucket.Attributes(ctx, candidate)
		if err == nil {
			attrs = a
			deleteKey = candidate
			addSpanAttrs(ctx, attribute.String("blob.storage_hit_key", candidate))
			break
		}
		if gcerrors.Code(err) == gcerrors.PermissionDenied {
			return nil, blobErr(err, b.Name()+":"+objPath)
		}
	}

	// Perform delete
	if err := b.bucket.Delete(ctx, deleteKey); err != nil {
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
func (b *backend) DeleteObjects(ctx context.Context, req schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	sk := b.key(req.Path)

	// Response
	response := schema.DeleteObjectsResponse{
		Name: b.Name(),
	}

	// Check if this path refers to a single real object (not a phantom directory)
	realObj, err := b.isRealObject(ctx, sk)
	if err != nil {
		return nil, err
	}
	if realObj != nil {
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

	// Keep listing and deleting until no more objects match.
	// maxDeletePasses prevents an infinite loop when a deletion silently fails
	// (returns no error but the object persists).
	const maxDeletePasses = 10
	for pass := 0; pass < maxDeletePasses; pass++ {
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
				Name:    b.Name(),
				Path:    objPath,
				Size:    obj.Size,
				ModTime: obj.ModTime,
			}
			if len(obj.MD5) > 0 {
				o.ETag = normaliseETag(fmt.Sprintf("%x", obj.MD5))
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
