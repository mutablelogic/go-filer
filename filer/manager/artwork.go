package manager

import (
	"context"

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

func (manager *Manager) CreateArtwork(ctx context.Context, req schema.ArtworkMeta, object *schema.ObjectKey) (_ *schema.Artwork, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "CreateArtwork",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Create the artwork in a transaction
	var result schema.Artwork
	if err := manager.PoolConn.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.Insert(ctx, &result, req); err != nil {
			return err
		}
		if object == nil {
			return nil
		}
		var link schema.ObjectArtwork
		return conn.Insert(ctx, &link, schema.ObjectArtwork{
			ObjectKey:  *object,
			ArtworkKey: result.ETag,
		})
	}); err != nil {
		return nil, pg.NormalizeError(err)
	}

	// Return the inserted artwork
	return types.Ptr(result), nil
}

func (manager *Manager) LinkArtwork(ctx context.Context, object schema.ObjectKey, artwork schema.ArtworkKey) (_ *schema.ObjectArtwork, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "LinkArtwork",
		attribute.String("object", types.Stringify(object)),
		attribute.String("artwork", string(artwork)),
	)
	defer func() { endSpan(err) }()

	var result schema.ObjectArtwork
	if err := manager.PoolConn.Tx(ctx, func(conn pg.Conn) error {
		return conn.Insert(ctx, &result, schema.ObjectArtwork{
			ObjectKey:  object,
			ArtworkKey: artwork,
		})
	}); err != nil {
		return nil, pg.NormalizeError(err)
	}
	return types.Ptr(result), nil
}

func (manager *Manager) UnlinkArtwork(ctx context.Context, object schema.ObjectKey, artwork schema.ArtworkKey) (_ *schema.ObjectArtwork, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "UnlinkArtwork",
		attribute.String("object", types.Stringify(object)),
		attribute.String("artwork", string(artwork)),
	)
	defer func() { endSpan(err) }()

	var result schema.ObjectArtwork
	if err := manager.PoolConn.Tx(ctx, func(conn pg.Conn) error {
		return conn.Delete(ctx, &result, schema.ObjectArtwork{
			ObjectKey:  object,
			ArtworkKey: artwork,
		})
	}); err != nil {
		return nil, pg.NormalizeError(err)
	}
	return types.Ptr(result), nil
}

func (manager *Manager) GetArtwork(ctx context.Context, req schema.GetArtworkRequest) (_ *schema.Artwork, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetArtwork",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// If conditional headers are present, do a cheap metadata-only fetch first
	// to avoid pulling the binary data when a 304 will be returned.
	if req.IfModifiedSince != nil || req.IfNoneMatch != nil {
		var info schema.ArtworkInfo
		if err := manager.PoolConn.Get(ctx, &info, schema.ArtworkInfoKey(req.Key)); err != nil {
			return nil, pg.NormalizeError(err)
		}
		if req.IfNoneMatch != nil && (*req.IfNoneMatch == "*" || *req.IfNoneMatch == string(info.ETag)) {
			return nil, gofiler.ErrNotModified
		}
		if req.IfModifiedSince != nil && !info.CreatedAt.After(*req.IfModifiedSince) {
			return nil, gofiler.ErrNotModified
		}
	}

	// Fetch the full artwork including binary data
	var result schema.Artwork
	if err := manager.PoolConn.Get(ctx, &result, req.Key); err != nil {
		return nil, pg.NormalizeError(err)
	}

	// Return the artwork
	return types.Ptr(result), nil
}
