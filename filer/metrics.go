package filer

import (
	"context"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	attribute "go.opentelemetry.io/otel/attribute"
	metric "go.opentelemetry.io/otel/metric"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// metrics are the registry's metrics, which are pushed to the metrics
// server by the meter's provider, or nil, without a meter
type metrics struct {
	worker attribute.KeyValue
	events metric.Int64Counter
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// metricEvents is the counter of the changes to the volumes' objects, by
	// their volumes and types
	metricEvents = "filer.events"

	// The attributes of a change, which are the worker, its volume, and its
	// type, such as created
	attrWorker    = "filer.worker"
	attrVolume    = "filer.volume"
	attrEventType = "filer.event.type"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newMetrics registers the registry's metrics with the meter, which are of
// the worker, or returns nil, without a meter
func newMetrics(meter metric.Meter, worker string) (*metrics, error) {
	if meter == nil {
		return nil, nil
	}
	events, err := meter.Int64Counter(metricEvents,
		metric.WithDescription("Changes to the objects of the volumes, by the worker, and their volumes and types"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}
	return &metrics{worker: attribute.String(attrWorker, worker), events: events}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// event counts a change, by the worker, and its volume and type
func (self *metrics) event(ctx context.Context, event schema.Event) {
	if self == nil {
		return
	}
	self.events.Add(ctx, 1, metric.WithAttributes(
		self.worker,
		attribute.String(attrVolume, event.Name),
		attribute.String(attrEventType, event.EventType.String()),
	))
}
