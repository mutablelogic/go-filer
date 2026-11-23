package webcal

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

// backend is the backend of calendar feeds, of webcal:// URLs
type backend struct{}

var _ gofiler.Backend = backend{}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed icon.svg
var iconSVG []byte

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Backend returns the backend of calendar feeds, of URLs of
// webcal://<name>/<host>/<path>
func Backend() gofiler.Backend {
	return backend{}
}

// FeedURL returns the URL of a feed, of one which is http, https, or
// webcal, such as a calendar's "Subscribe" link, which is fetched as
// https, or an error when it's none of them
func FeedURL(feed string) (*url.URL, error) {
	u, err := url.Parse(feed)
	if err != nil || u.Host == "" {
		return nil, gofiler.ErrBadParameter.Withf("the url of a calendar is http, https or webcal: %q", feed)
	}
	switch u.Scheme {
	case "http", "https":
	case Scheme:
		u.Scheme = "https"
	default:
		return nil, gofiler.ErrBadParameter.Withf("the url of a calendar is http, https or webcal: %q", feed)
	}
	return u, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (backend) Scheme() string      { return Scheme }
func (backend) Description() string { return Description }
func (backend) Icon() gofiler.Icon  { return icon.SVG(iconSVG) }

// Defaults returns the options of a feed which aren't set: it's read-only,
// fetched over https, and polled every five minutes
func (backend) Defaults() url.Values {
	return url.Values{paramReadonly: {"true"}, paramInsecure: {"false"}, paramRefresh: {DefaultRefresh.String()}}
}

// New opens the feed of webcal://<name>/<host>/<path>, which is fetched
func (backend) New(ctx context.Context, u *url.URL, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
	return Open(ctx, u, cache, opts)
}
