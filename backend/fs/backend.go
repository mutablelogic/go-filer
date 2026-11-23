package fs

import (
	"context"
	_ "embed"
	"net/url"
	"path/filepath"
	"runtime"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	icon "github.com/mutablelogic/go-filer/backend/internal/icon"
	schema "github.com/mutablelogic/go-filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// backend is the backend of directories, of file:// URLs
type backend struct{}

var _ filer.Backend = backend{}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed icon.svg
var iconSVG []byte

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// Backend returns the backend of directories on the server, of URLs of
// file://<name>/<path>
func Backend() filer.Backend {
	return backend{}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (backend) Scheme() string      { return schema.SchemeFile }
func (backend) Description() string { return "Server filesystem" }
func (backend) Icon() filer.Icon    { return icon.SVG(iconSVG) }

// Defaults returns the options of a directory which aren't set: the names it
// skips, its packages, and that it's writable
func (backend) Defaults() url.Values {
	o, err := parseOpts(url.Values{})
	if err != nil {
		return url.Values{}
	}
	values := o.values()
	values.Del(schema.OptDescription)
	return values
}

// New opens the directory of file://<name>/<path>, whose path is absolute,
// so a URL without one isn't the directory the server is running in. On
// Windows, it's /C:/media, which is C:/media.
func (backend) New(_ context.Context, u *url.URL, _ filer.CacheStore, opts filer.Opts) (filer.Filer, error) {
	if !types.IsIdentifier(u.Host) {
		return nil, filer.ErrBadParameter.Withf("invalid volume name: %q", u.Host)
	}
	path := u.Path
	if runtime.GOOS == "windows" {
		path = types.FilePath(&url.URL{Scheme: schema.SchemeFile, Path: u.Path})
	}
	if path == "" || !filepath.IsAbs(filepath.FromSlash(path)) {
		return nil, filer.ErrBadParameter.Withf("path of volume %q isn't absolute: %q", u.Host, path)
	}
	return New(u.Host, path, u.Query(), opts)
}
