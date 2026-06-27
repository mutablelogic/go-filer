package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	pg "github.com/mutablelogic/go-pg"
	pgqueueschema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) GetObject(ctx context.Context, req schema.ObjectKey) (_ *schema.Object, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetObject",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Check the volume first
	volume, err := manager.GetVolume(ctx, req.Volume)
	if err != nil {
		return nil, err
	} else if types.Value(volume.Enabled) == false {
		return nil, gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", req.Volume)
	}

	// Get the backend
	backend := manager.volumes.Get(volume.Name)
	if backend == nil {
		return nil, gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", req.Volume)
	}

	// Get the object from the backend first
	object, err := backend.GetObject(ctx, schema.GetObjectRequest{
		ObjectKey: req,
	})
	if err != nil {
		return nil, err
	}

	// If the volume is not indexed, or the index delta is zero, return the backend object directly
	if types.Value(volume.IndexDelta) == 0 || volume.IndexedAt == nil {
		return object, nil
	}

	// Get the metadata from the database (on error, return the object we have)
	var result schema.Object
	if err := manager.PoolConn.Get(ctx, &result, req); errors.Is(err, pg.ErrNotFound) {
		// Kick off an indexing job for the object
		return object, manager.enqueueIndexObject(ctx, object.ObjectKey, true)
	} else if err != nil {
		return object, err
	}

	// If the objects match, return the result from the database
	if result.Matches(object) {
		return types.Ptr(result), nil
	}

	// Kick off an indexing job for the object, and return the object we have
	if err := manager.enqueueIndexObject(ctx, object.ObjectKey, true); err != nil {
		return object, err
	}

	// Return the backend object in preference to the database object, since the database object is stale
	return object, nil
}

func (manager *Manager) ListObjects(ctx context.Context, req schema.ObjectListRequest) (_ *schema.ObjectList, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListObjects",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Check the volume first
	volume, err := manager.GetVolume(ctx, req.Volume)
	if err != nil {
		return nil, err
	} else if types.Value(volume.Enabled) == false {
		return nil, gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", req.Volume)
	}

	// Check prefix/path argument
	if path := types.Value(req.Path); path != "" {
		// TODO: Path should be a valid prefix or valid object
		// or else return NotFound
	}

	// If indexing is not enabled, or no indexing has happened yet, or there is no last indexed object,
	// iterate the backend directly.
	if types.Value(volume.IndexDelta) == 0 || volume.IndexedAt == nil || volume.LastIndexedObjectAt == nil {
		b := manager.volumes.Get(volume.Name)
		if b == nil {
			return nil, gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", req.Volume)
		}

		offset := req.Offset
		limit := uint64(schema.ObjectListLimit)
		if req.Limit != nil {
			limit = *req.Limit
		}

		iterator := &schema.ObjectListIterator{
			Path:      req.Path,
			Type:      req.Type,
			Recursive: req.Recursive,
		}

		var result schema.ObjectList
		n := uint64(0)
	outer:
		for {
			err := b.ListObjects(ctx, iterator)
			done := errors.Is(err, io.EOF)
			if err != nil && !done {
				return nil, err
			}
			for _, obj := range iterator.Body {
				if n >= offset {
					if uint64(len(result.Body)) >= limit {
						break outer
					}
					result.Body = append(result.Body, obj)
				}
				n++
			}
			if done {
				break
			}
		}

		req.Path = iterator.Path // reflect normalised path back into the response
		result.ObjectListRequest = req
		result.OffsetLimit.Limit = types.Ptr(limit)
		return types.Ptr(result), nil
	}

	// Use the database index to return the objects
	// TODO: We're not respecting the recursive flag here, and maybe not the
	// directory flag either?
	var result schema.ObjectList
	if err := manager.PoolConn.List(ctx, &result, &req); err != nil {
		return nil, err
	} else {
		result.ObjectListRequest = req
		result.OffsetLimit.Clamp(uint64(result.Count))
	}

	// Return success
	return types.Ptr(result), nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (manager *Manager) touchObject(ctx context.Context, req schema.ObjectKey) (_ *schema.Object, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "touchObject",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	var result schema.Object
	if err := manager.Tx(ctx, func(conn pg.Conn) error {
		return conn.Update(ctx, &result, schema.ObjectTouch(req), nil)
	}); err != nil {
		return nil, err
	}
	return types.Ptr(result), nil
}

func (manager *Manager) enqueueIndexObject(ctx context.Context, key schema.ObjectKey, force bool) error {
	if manager.indexQueue == nil {
		return gofiler.ErrServiceUnavailable.With("index queue not available")
	}
	payload, err := json.Marshal(indexObjectTask{ObjectKey: key, Force: force})
	if err != nil {
		return fmt.Errorf("failed to marshal index task: %w", err)
	}
	_, err = manager.queue.CreateTask(ctx, manager.indexQueue.Queue, pgqueueschema.TaskMeta{
		Payload: payload,
	})
	return err
}

func (manager *Manager) createObject(ctx context.Context, req schema.ObjectCreate, artwork []*schema.ArtworkMeta) (_ *schema.Object, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "createObject",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// If the content type is not provided, default to application/octet-stream
	if typ := strings.TrimSpace(req.ContentType); typ == "" {
		req.ContentType = types.ContentTypeBinary
	} else if t, params, err := mime.ParseMediaType(typ); err != nil {
		return nil, gofiler.ErrBadParameter.Withf("invalid content type: %q", typ)
	} else if t == schema.ContentTypeDirectory {
		return nil, gofiler.ErrBadParameter.Withf("invalid content type: %q", typ)
	} else {
		req.ContentType = t
		for key, v := range params {
			if key = strings.TrimSpace(key); types.IsIdentifier(key) {
				req.Meta = schema.AppendMeta(req.Meta, key, v)
			}
		}
	}

	// Upsert the object
	var result schema.Object
	if err := manager.Tx(ctx, func(conn pg.Conn) error {
		// Delete the object first, to clear the metadata
		if err := conn.Delete(ctx, &result, req.ObjectKey); errors.Is(err, pg.ErrNotFound) {
			// Ignore not found errors, we will insert a new object
		} else if err != nil {
			return err
		}

		// Insert the object
		if err := conn.Insert(ctx, &result, req); err != nil {
			return err
		}

		// Insert the metadata
		for _, meta := range req.ObjectMeta.Meta {
			var metaresult schema.Meta
			if err := conn.With("volume", result.Volume, "path", result.Path).Insert(ctx, &metaresult, meta); err != nil {
				return err
			} else {
				result.Meta = append(result.Meta, metaresult)
			}
		}

		// Insert the artwork
		for _, meta := range artwork {
			var artwork schema.Artwork
			if err := conn.Insert(ctx, &artwork, meta); err != nil {
				return err
			}
			var link schema.ObjectArtwork
			if err := conn.Insert(ctx, &link, schema.ObjectArtwork{
				ObjectKey:  result.ObjectKey,
				ArtworkKey: artwork.ETag,
			}); err != nil {
				return err
			}
		}

		// Return success
		return nil
	}); err != nil {
		return nil, pg.NormalizeError(err)
	}

	// Re-fetch the object to get the metadata and artwork
	if err := manager.PoolConn.Get(ctx, &result, req.ObjectKey); err != nil {
		return nil, pg.NormalizeError(err)
	}

	// Return the inserted object
	return types.Ptr(result), nil
}
