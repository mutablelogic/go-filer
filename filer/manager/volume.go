package manager

import (
	"context"
	"errors"
	"io"
	"net/url"
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

// GetVolume returns a volume status
func (filer *Filer) GetVolume(ctx context.Context, name string) (_ *schema.Volume, err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "GetVolume",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Get the volume record from the database
	var result schema.Volume
	if err := filer.Get(ctx, &result, schema.VolumeName(name)); err != nil {
		return nil, err
	}

	// Return success
	return types.Ptr(result), nil
}

// UpdateVolume updates a volume record in the database, and returns the updated record.
func (filer *Filer) UpdateVolume(ctx context.Context, name string, meta schema.VolumeMeta) (_ *schema.Volume, err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "UpdateVolume",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	var volume schema.Volume
	if err := filer.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.Update(ctx, &volume, schema.VolumeName(name), meta); err != nil {
			return err
		}

		// Return success
		return nil
	}); err != nil {
		return nil, err
	}

	// Return success
	return types.Ptr(volume), nil
}

// ListVolumes returns all volumes as a list.
func (filer *Filer) ListVolumes(ctx context.Context, req schema.VolumeListRequest) (_ *schema.VolumeList, err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "ListVolumes",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	resp := schema.VolumeList{VolumeListRequest: req}
	if err := filer.PoolConn.List(ctx, &resp, &req); err != nil {
		return nil, err
	} else {
		resp.OffsetLimit.Clamp(resp.Count)
	}
	return types.Ptr(resp), nil
}

// DeleteVolume deletes a volume record from the database, and returns the deleted record.
func (filer *Filer) DeleteVolume(ctx context.Context, name string) (_ *schema.Volume, err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "DeleteVolume",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	var volume schema.Volume
	if err := filer.Tx(ctx, func(conn pg.Conn) error {
		// Get the volume record from the database
		if err := conn.Get(ctx, &volume, schema.VolumeName(name)); err != nil {
			return err
		}

		// The volume must be unmounted before it can be deleted
		if types.Value(volume.Enabled) {
			return gofiler.ErrConflict.With("volume must be unmounted before it can be deleted")
		}

		// Delete the volume record from the database
		if err := conn.Delete(ctx, &volume, schema.VolumeName(name)); err != nil {
			return err
		}

		// Return success
		return nil
	}); err != nil {
		return nil, err
	}

	// Return success
	return types.Ptr(volume), nil
}

// CreateVolume creates a new volume record in the database, and returns the created record.
func (filer *Filer) CreateVolume(ctx context.Context, url *url.URL, meta schema.VolumeMeta) (_ *schema.Volume, err error) {
	if url == nil {
		return nil, gofiler.ErrBadParameter.With("url is required")
	}
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "CreateVolume",
		attribute.String("url", url.String()),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	// Check that the backend can be handled, and return the name
	name, err := filer.volumes.Validate(ctx, url)
	if err != nil {
		return nil, err
	}

	// Insert the volume record in the database - which then syncs the volume with the volume registry
	var result schema.Volume
	if err := filer.Tx(ctx, func(conn pg.Conn) error {
		return conn.With("name", name).Insert(ctx, &result, schema.VolumeCreate{
			URL:        url.String(),
			VolumeMeta: meta,
		})
	}); err != nil {
		return nil, err
	}

	// Return the created volume record
	return types.Ptr(result), nil
}

// ReindexVolume reindexes objects in a volume according to the provided object filters.
func (filer *Filer) ReindexVolume(ctx context.Context, name string, req schema.ObjectListFilters, force bool) (err error) {
	ctx, endSpan := otel.StartSpan(filer.tracer, ctx, "ReindexVolume",
		attribute.String("volume", name),
		attribute.String("req", types.Stringify(req)),
		attribute.Bool("force", force),
	)
	defer func() { endSpan(err) }()

	// Check the volume first
	volume, err := filer.GetVolume(ctx, name)
	if err != nil {
		return err
	} else if types.Value(volume.Enabled) == false {
		return gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", volume.Name)
	}

	// If indexing is not enabled, return an error
	if types.Value(volume.IndexDelta) == 0 {
		return gofiler.ErrServiceUnavailable.Withf("volume %q is not indexed", volume.Name)
	}

	// Get the backend
	backend := filer.volumes.Get(volume.Name)
	if backend == nil {
		return gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", volume.Name)
	}

	// If the path points at a single object rather than a directory, reindex
	// just that object. ListObjects only lists directory contents, so a file
	// path would otherwise be rejected with a "not a directory" error.
	if reqPath := types.Value(req.Path); reqPath != "" {
		if object, err := backend.GetObject(ctx, schema.GetObjectRequest{
			ObjectKey: schema.ObjectKey{Volume: volume.Name, Path: reqPath},
		}); err == nil {
			return filer.enqueueIndexObject(ctx, object.ObjectKey, force)
		}
	}

	// Iterate through the objects in the volume, and reindex them according to the provided filters
	var iterator schema.ObjectListIterator

	// Set iterator filters
	iterator.Path = req.Path
	iterator.Recursive = req.Recursive
	iterator.Type = req.Type

	for {
		err := backend.ListObjects(ctx, &iterator)
		done := errors.Is(err, io.EOF)
		if err != nil && !done {
			return err
		}

		reqType := types.Value(req.Type)
		for _, object := range iterator.Body {
			if object.ContentType == schema.ContentTypeDirectory {
				continue
			}
			// The backend only special-cases text/directory when listing; any
			// other requested type (e.g. "image" for "image/*") still needs to
			// be matched against the object's actual content type here.
			if reqType != "" && reqType != schema.ContentTypeDirectory && !strings.HasPrefix(object.ContentType, reqType) {
				continue
			}
			if err := filer.enqueueIndexObject(ctx, object.ObjectKey, force); err != nil {
				return err
			}
		}

		if done {
			break
		}
	}

	// Return success
	return nil
}
