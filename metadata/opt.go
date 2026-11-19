package metadata

import (
	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	gometadata "github.com/mutablelogic/go-metadata"
	acoustid "github.com/mutablelogic/go-metadata/pkg/acoustid"
	tmdb "github.com/mutablelogic/go-metadata/pkg/tmdb"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is an option of the enrichment of files with their metadata
type Opt func(*opt) error

type opt struct {
	tracer     trace.Tracer
	clientOpts []client.ClientOpt  // The options of the clients of TMDB, and AcoustID
	opts       []gometadata.Option // The options of go-metadata, of each file
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// WithTracer traces the enrichment of each file, and go-metadata's handlers
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opt) error {
		o.tracer = tracer
		if tracer != nil {
			o.opts = append(o.opts, gometadata.WithTracer(tracer))
		}
		return nil
	}
}

// WithClientOpts sets the options of the clients of TMDB, and AcoustID,
// such as a user agent, or a tracer, which are set before them
func WithClientOpts(opts ...client.ClientOpt) Opt {
	return func(o *opt) error {
		o.clientOpts = append(o.clientOpts, opts...)
		return nil
	}
}

// WithTMDB looks up the movies, and TV episodes, which videos are, on TMDB,
// with its API key
func WithTMDB(key string) Opt {
	return func(o *opt) error {
		if key == "" {
			return gofiler.ErrBadParameter.With("a TMDB API key is empty")
		}
		c, err := tmdb.New(key, o.clientOpts...)
		if err != nil {
			return err
		}
		o.opts = append(o.opts, gometadata.WithTMDB(c))
		return nil
	}
}

// WithAcoustID looks up the recordings which audio is, on AcoustID, with its
// API key, of the AcoustID identifiers of their tags, or, built with cgo and
// the chromaprint tag, of their fingerprints
func WithAcoustID(key string) Opt {
	return func(o *opt) error {
		if key == "" {
			return gofiler.ErrBadParameter.With("an AcoustID API key is empty")
		}
		c, err := acoustid.New(key, o.clientOpts...)
		if err != nil {
			return err
		}
		o.opts = append(o.opts, gometadata.WithAcoustID(c))
		return nil
	}
}

// WithMaxSize sets the most of a file which is read, in bytes, or zero for
// no limit, which is the default
func WithMaxSize(size int64) Opt {
	return func(o *opt) error {
		o.opts = append(o.opts, gometadata.WithMaxSize(size))
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (o *opt) apply(opts []Opt) error {
	for _, fn := range opts {
		if err := fn(o); err != nil {
			return err
		}
	}
	return nil
}
