package manager

import (
	"context"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
	metric "go.opentelemetry.io/otel/metric"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) RegisterVolumeMetrics(name string) (err error) {
	// Register a gauge for indexed object count per volume.
	if guage, err := manager.metrics.Int64ObservableGauge(
		name,
		metric.WithDescription("Number of indexed objects in a volume"),
		metric.WithUnit("{object}"),
	); err != nil {
		return pg.ErrInternalServerError.Withf("RegisterVolumeMetrics: %v", err)
	} else if _, err := manager.metrics.RegisterCallback(func(parent context.Context, observer metric.Observer) (err error) {
		// Otel span
		ctx, endSpan := otel.StartSpan(manager.tracer, parent, "ObserveVolumeMetrics",
			attribute.String("name", name),
		)
		defer func() { endSpan(err) }()

		// Paginate through volumes
		var offset uint64
		var req schema.VolumeListRequest
		for {
			// Perform pagination
			req.Offset = offset
			volumes, err := manager.ListVolumes(ctx, req)
			if err != nil {
				return pg.ErrInternalServerError.Withf("RegisterVolumeMetrics: %v", err)
			}
			if len(volumes.Body) == 0 {
				break
			}
			offset += uint64(len(volumes.Body))

			// Record the metrics
			for _, volume := range volumes.Body {
				observer.ObserveInt64(guage, int64(volume.Objects), metric.WithAttributes(
					attribute.String("volume", volume.Name),
					attribute.String("url", volume.URL),
					attribute.Bool("enabled", types.Value(volume.Enabled)),
				))
			}

		}
		return nil
	}, guage); err != nil {
		return pg.ErrInternalServerError.Withf("RegisterVolumeMetrics: %v", err)
	}

	// Return success
	return nil
}
