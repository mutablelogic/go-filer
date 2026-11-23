package rss

import (
	"context"
	_ "embed"
	"net/url"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	icon "github.com/mutablelogic/go-filer/backend/internal/icon"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// backend is the backend of feeds, of rss:// URLs
type backend struct{}

var _ gofiler.Backend = backend{}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed icon.svg
var iconSVG []byte

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Backend returns the backend of RSS feeds, of URLs of
// rss://<name>/<host>/<path>, which isn't built into the registry, but is
// added to it, as another package's, or program's, would be
func Backend() gofiler.Backend {
	return backend{}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (backend) Scheme() string      { return Scheme }
func (backend) Description() string { return Description }
func (backend) Icon() gofiler.Icon  { return icon.SVG(iconSVG) }

// Defaults returns the options of a feed which aren't set: it's read-only,
// and fetched over https
func (backend) Defaults() url.Values {
	return url.Values{paramReadonly: {"true"}, paramInsecure: {"false"}}
}

// New opens the feed of rss://<name>/<host>/<path>, which is fetched
func (backend) New(ctx context.Context, u *url.URL, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
	return Open(ctx, u, cache, opts)
}
