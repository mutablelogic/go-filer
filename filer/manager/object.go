package manager

import (
	"context"
	"errors"
	"mime"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	pg "github.com/mutablelogic/go-pg"
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

	var result schema.Object
	if err := manager.PoolConn.Get(ctx, &result, req); err != nil {
		return nil, err
	}

	// Return success
	return types.Ptr(result), nil
}

func (manager *Manager) ListObjects(ctx context.Context, req schema.ObjectListRequest) (_ *schema.ObjectList, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListObjects",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

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

func (manager *Manager) touchObject(ctx context.Context, req schema.ObjectKey) (err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "touchObject",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Update indexed_at for the object and its volume
	if err := manager.Tx(ctx, func(conn pg.Conn) error {
		var touchedObject schema.Object
		if err := conn.Update(ctx, &touchedObject, schema.ObjectTouch(req), nil); err != nil {
			return err
		}

		var touchedVolume schema.Volume
		if err := conn.Update(ctx, &touchedVolume, schema.VolumeTouch(req.Volume), nil); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	// Return success
	return nil
}

func (manager *Manager) createObject(ctx context.Context, req schema.ObjectCreate) (_ *schema.Object, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "createObject",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Cannot create directories
	if req.IsDir {
		return nil, gofiler.ErrConflict.With("cannot create directory object")
	}

	// If the content type is not provided, default to application/octet-stream
	if typ := strings.TrimSpace(req.ContentType); typ == "" {
		req.ContentType = types.ContentTypeBinary
	} else if t, params, err := mime.ParseMediaType(typ); err != nil {
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

		// Touch indexed_at for the volume
		var touched schema.Volume
		if err := manager.PoolConn.Update(ctx, &touched, schema.VolumeTouch(req.Volume), nil); err != nil {
			return err
		}

		// Return success
		return nil

	}); err != nil {
		return nil, err
	}

	// Return success
	return types.Ptr(result), nil
}
