package store

import (
	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	broadcaster "github.com/mutablelogic/go-server/broadcaster"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opts are the options of a store, which each store's New is given
type Opts struct {
	// The OpenTelemetry tracer, of a span of each query, or nil
	Tracer trace.Tracer

	// The broadcaster, which changes to the volumes are published to, so
	// they're watched, such as of those added by another server, or nil.
	// A store publishes a change when a volume is created, updated or
	// deleted, of schema.VolumeChange.
	Broadcaster *broadcaster.Broadcaster
}

// Opt is an option of a store
type Opt func(*Opts) error

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Apply returns the options
func Apply(opts ...Opt) (Opts, error) {
	var o Opts
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return o, err
		}
	}
	return o, nil
}

// WithTracer traces the store's queries, which a nil tracer doesn't
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *Opts) error {
		o.Tracer = tracer
		return nil
	}
}

// WithBroadcaster publishes changes to the store's volumes, once they're
// committed, to the broadcaster: when one is created, updated, which is
// when its URL, or whether it's enabled, changes, or deleted
func WithBroadcaster(b *broadcaster.Broadcaster) Opt {
	return func(o *Opts) error {
		if b == nil {
			return gofiler.ErrBadParameter.With("the broadcaster can't be nil")
		}
		o.Broadcaster = b
		return nil
	}
}

// Publish publishes the change of a volume to the broadcaster, when there's
// one. A store calls it once the change is committed.
func (o Opts) Publish(action changes.ChangeAction, volume *schema.Volume) {
	if o.Broadcaster != nil && volume != nil {
		o.Broadcaster.Publish(schema.VolumeChange(action, volume))
	}
}
