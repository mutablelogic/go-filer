package manager

import (
	"context"
	"fmt"
	"net/url"

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
func (manager *Manager) GetVolume(ctx context.Context, name string) (_ *schema.Volume, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetVolume",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Get the volume record from the database
	var result schema.Volume
	if err := manager.Get(ctx, &result, schema.VolumeName(name)); err != nil {
		return nil, err
	}

	// Return success
	return types.Ptr(result), nil
}

// UpdateVolume updates a volume record in the database, and returns the updated record.
func (manager *Manager) UpdateVolume(ctx context.Context, name string, meta schema.VolumeMeta) (_ *schema.Volume, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "UpdateVolume",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	var volume schema.Volume
	if err := manager.Tx(ctx, func(conn pg.Conn) error {
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
func (manager *Manager) ListVolumes(ctx context.Context, req schema.VolumeListRequest) (_ *schema.VolumeList, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListVolumes",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	resp := schema.VolumeList{VolumeListRequest: req}
	if err := manager.PoolConn.List(ctx, &resp, &req); err != nil {
		return nil, err
	} else {
		resp.OffsetLimit.Clamp(resp.Count)
	}
	return types.Ptr(resp), nil
}

// DeleteVolume deletes a volume record from the database, and returns the deleted record.
func (manager *Manager) DeleteVolume(ctx context.Context, name string) (_ *schema.Volume, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "DeleteVolume",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	var volume schema.Volume
	if err := manager.Tx(ctx, func(conn pg.Conn) error {
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
func (manager *Manager) CreateVolume(ctx context.Context, url *url.URL, meta schema.VolumeMeta) (_ *schema.Volume, err error) {
	if url == nil {
		return nil, gofiler.ErrBadParameter.With("url is required")
	}
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "CreateVolume",
		attribute.String("url", url.String()),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	// Check that the backend can be handled, and return the name
	name, err := manager.volumes.Validate(url)
	if err != nil {
		return nil, err
	}

	// Insert the volume record in the database - which then syncs the volume with the volume registry
	var result schema.Volume
	if err := manager.Tx(ctx, func(conn pg.Conn) error {
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
func (manager *Manager) ReindexVolume(ctx context.Context, name string, req schema.ObjectListFilters) (err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ReindexVolume",
		attribute.String("volume", name),
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Check the volume first
	volume, err := manager.GetVolume(ctx, name)
	if err != nil {
		return err
	} else if types.Value(volume.Enabled) == false {
		return gofiler.ErrServiceUnavailable.Withf("volume %q is not mounted", volume.Name)
	}

	// If indexing is not enabled, or no indexing has happened yet, return an error
	if types.Value(volume.IndexDelta) == 0 || volume.IndexedAt == nil || volume.Objects == 0 {
		return gofiler.ErrServiceUnavailable.Withf("volume %q is not indexed", volume.Name)
	}

	// Iterate through the objects in the volume, and reindex them according to the provided filters
	var list schema.ObjectListRequest
	list.Volume = volume.Name
	list.ObjectListFilters = req
	for {
		objects, err := manager.ListObjects(ctx, list)
		if err != nil {
			return err
		} else if len(objects.Body) == 0 {
			break
		}

		for _, object := range objects.Body {
			if !object.IsDir {
				fmt.Println("Reindexing object:", object.Path)
			}
		}

		// Iterate the list offset for the next iteration
		list.Offset += uint64(len(objects.Body))
	}

	// Return success
	return nil
}
