package registry

import (
	"os"
	"strings"
	"time"

	// Packages
	credential "github.com/mutablelogic/go-auth/credential"
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/indexer"
	store "github.com/mutablelogic/go-filer/store"
	broadcaster "github.com/mutablelogic/go-server/broadcaster"
	metric "go.opentelemetry.io/otel/metric"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is a functional option of the registry
type Opt func(*opt) error

type opt struct {
	broadcaster *broadcaster.Broadcaster
	meter       metric.Meter
	backendOpts gofiler.Opts  // the options every backend is created with: its tracer, credentials, and client options
	store       store.Store   // where the volumes, and their indexes, are kept, which is required
	worker      string        // the name of the worker, which is in the metrics
	debounce    time.Duration // how long a change is held, until its object is quiet
	metadata    indexer.Task  // the task which enriches the files of volumes with index=metadata, or nil
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func (o *opt) apply(opt []Opt) error {
	o.defaults()

	// Apply options
	for _, fn := range opt {
		if err := fn(o); err != nil {
			return err
		}
	}

	// The store is required, as it's the volumes of the registry
	if o.store == nil {
		return gofiler.ErrBadParameter.With("the registry needs a store, with WithStore")
	}

	// Return success
	return nil
}

func (o *opt) defaults() {
	o.worker = defaultWorker()
	o.debounce = debounceDelay
}

////////////////////////////////////////////////////////////////////////////////
// OPTIONS

// WithWorker sets the name of the worker, such as the server's, which is in
// the metrics, so those of the workers are told apart, and which is the
// hostname by default
func WithWorker(name string) Opt {
	return func(o *opt) error {
		if name = strings.TrimSpace(name); name == "" {
			return gofiler.ErrBadParameter.With("the worker's name can't be empty")
		}
		o.worker = name
		return nil
	}
}

// WithDebounce sets how long a change to an object is held, until it's quiet
// for it, when its changes are merged, so an object which changes many times,
// such as a file which is written in parts, is one change, which is 250ms by
// default, or zero to send the changes at once. A change is held for at most
// two seconds, or the delay, when it's longer.
func WithDebounce(delay time.Duration) Opt {
	return func(o *opt) error {
		if delay < 0 {
			return gofiler.ErrBadParameter.Withf("the debounce can't be negative: %v", delay)
		}
		o.debounce = delay
		return nil
	}
}

// WithBroadcaster sets the broadcaster which Run publishes the changes to
// the volumes to, which its owner runs, and serves, such as a server, which
// can share it with its other modules. Without it, the changes aren't
// published, and WatchObjects is ErrNotImplemented.
func WithBroadcaster(b *broadcaster.Broadcaster) Opt {
	return func(o *opt) error {
		if b == nil {
			return gofiler.ErrBadParameter.With("the broadcaster can't be nil")
		}
		o.broadcaster = b
		return nil
	}
}

// WithTracer sets the tracer of the backends' operations, such as fs's,
// and S3's
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opt) error {
		o.backendOpts.Tracer = tracer
		return nil
	}
}

// WithMeter sets the OpenTelemetry meter used for metrics.
func WithMeter(meter metric.Meter) Opt {
	return func(o *opt) error {
		o.meter = meter
		return nil
	}
}

// WithMetadataTask sets the task which enriches the files of the volumes
// which are indexed with index=metadata, with their metadata, such as
// go-metadata's, which each volume's indexer does, apart from indexing it
func WithMetadataTask(task indexer.Task) Opt {
	return func(o *opt) error {
		if task == nil {
			return gofiler.ErrBadParameter.With("a metadata task is nil")
		}
		o.metadata = task
		return nil
	}
}

// WithCredentials sets the provider of the credentials of the backends,
// which expands the placeholders of the credentials in their URLs, such as
// ${S3_SECRET}, so the URLs don't have the credentials
func WithCredentials(credentials credential.CredentialProvider) Opt {
	return func(o *opt) error {
		o.backendOpts.Credentials = credentials
		return nil
	}
}

// WithClientOpts sets the client options of the backends which call other
// services over HTTP, such as Google's, which they're given as they are,
// such as a user agent, a tracer, of client.OptTracer, a trace, or a
// timeout, as a command's are, of server.Cmd's ClientEndpoint. The registry
// doesn't add its own tracer to them, so a request isn't traced twice, or
// left out of a trace it's given.
func WithClientOpts(clientopts ...client.ClientOpt) Opt {
	return func(o *opt) error {
		o.backendOpts.ClientOpts = clientopts
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// defaultWorker returns the hostname, which is the name of the worker by
// default, or "unknown", when it can't be got
func defaultWorker() string {
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		return hostname
	}
	return "unknown"
}

// WithStore sets the store of the volumes, and their indexes, which is
// required. The registry mounts the volumes which are enabled in it, and a
// volume with the index option, such as index=folders, is indexed in it.
func WithStore(s store.Store) Opt {
	return func(o *opt) error {
		if s == nil {
			return gofiler.ErrBadParameter.With("the store can't be nil")
		}
		o.store = s
		return nil
	}
}
