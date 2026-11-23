package registry

import (
	"regexp"
	"slices"
	"strings"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fs "github.com/mutablelogic/go-filer/backend/fs"
	google "github.com/mutablelogic/go-filer/backend/google"
	s3 "github.com/mutablelogic/go-filer/backend/s3"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// scheme is a URL's scheme, of RFC 3986, in lower case, as url.Parse
// returns it, of two characters or more, as one of a single letter is a
// Windows drive, such as C:/media, which ParseURL reads as a path
var scheme = regexp.MustCompile(`^[a-z][a-z0-9+.-]+$`)

// builtin returns the backends which are built in, of their schemes
func builtin() map[string]gofiler.Backend {
	result := make(map[string]gofiler.Backend)
	for _, backend := range []gofiler.Backend{
		fs.Backend(),
		s3.Backend(),
		google.TasksBackend(),
		google.ContactsBackend(),
		google.CalendarBackend(),
		google.DriveBackend(),
	} {
		result[backend.Scheme()] = backend
	}
	return result
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// WithBackend adds a backend, which the volumes whose URLs have its scheme
// are opened with, such as one of another package, or program, which embeds
// the registry, of its package's Backend function, such as rss.Backend(). A
// scheme is of two characters or more, in lower case, and one which another
// backend has, such as one which is built in, is an error.
func WithBackend(backend gofiler.Backend) Opt {
	return func(o *opt) error {
		if backend == nil {
			return gofiler.ErrBadParameter.With("a backend is nil")
		} else if name := backend.Scheme(); !scheme.MatchString(name) {
			return gofiler.ErrBadParameter.Withf("invalid scheme: %q", name)
		} else if _, exists := o.backends[name]; exists {
			return gofiler.ErrConflict.Withf("a backend of scheme %q is already registered", name)
		} else {
			o.backends[name] = backend
		}
		return nil
	}
}

// WithSchemes uses only the backends of the schemes, of those which are
// built in, and those of WithBackend, such as a server's --backend flag. A
// stored volume of another scheme is offline, as its backend isn't
// registered. A scheme which isn't a backend's is an error, which lists
// those which are. It can be repeated, and its schemes are added.
func WithSchemes(schemes ...string) Opt {
	return func(o *opt) error {
		for _, name := range schemes {
			if name = strings.TrimSpace(name); name != "" && !slices.Contains(o.schemes, name) {
				o.schemes = append(o.schemes, name)
			}
		}
		return nil
	}
}
