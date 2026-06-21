package manager

import (
	"context"
	"net/url"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListVolumes returns all volumes as a list.
func (manager *Manager) ListVolumes(ctx context.Context, req schema.VolumeListRequest) (_ *schema.VolumeList, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListVolumes",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	resp := schema.VolumeList{VolumeListRequest: req}
	if err := manager.List(ctx, &resp, &req); err != nil {
		return nil, err
	} else {
		resp.OffsetLimit.Clamp(resp.Count)
	}
	return types.Ptr(resp), nil
}

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
	if err := manager.With("name", name).Insert(ctx, &result, schema.VolumeCreate{
		URL:        url.String(),
		VolumeMeta: meta,
	}); err != nil {
		return nil, err
	}

	// Return the created volume record
	return &result, nil
}
