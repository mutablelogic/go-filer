package filer

import (
	"context"
	"net/url"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Backend is a kind of volume, such as a directory, or a bucket, of the
// scheme of its URLs, which describes itself, and opens a volume of a URL.
// Each backend's package returns its own, of its Backend function, such as
// fs.Backend(), so a backend of another package, or program, is added to the
// registry as one which is built in is.
type Backend interface {
	// Scheme returns the scheme of the URLs of its volumes, such as file, in
	// lower case, of two characters or more
	Scheme() string

	// Description returns what it is, such as "Server filesystem", rather
	// than what one of its volumes is, which is the filer's Description
	Description() string

	// Icon returns the image which represents it, or nil, when it has none
	Icon() Icon

	// Defaults returns the options of a volume which aren't set, as a URL's
	// query, such as readonly=true, which a volume's URL has, unless they're
	// set, apart from those of each volume, such as its name, or
	// description
	Defaults() url.Values

	// New opens the volume of the URL, of its options, apart from the
	// registry's, such as index, with the volume's cache, which the backend
	// uses, or not, and the options every backend is created with: the
	// tracer, the provider of credentials, and the client options. A volume's
	// name is its URL's name option, or its host. A backend which holds
	// resources implements io.Closer, which its owner calls.
	New(ctx context.Context, u *url.URL, cache CacheStore, opts Opts) (Filer, error)
}
