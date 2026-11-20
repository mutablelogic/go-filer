package registry

import (
	"context"
	"net/url"
	"path/filepath"
	"regexp"
	"runtime"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	fs "github.com/mutablelogic/go-filer/backend/fs"
	google "github.com/mutablelogic/go-filer/backend/google"
	s3 "github.com/mutablelogic/go-filer/backend/s3"
	schema "github.com/mutablelogic/go-filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Backend creates the backend of a volume from its URL, whose scheme it's
// registered with (see WithBackend), and whose options are its URL's query,
// apart from the index option, which the registry does, with the options
// every backend is created with: the registry's tracer, the provider of
// credentials, which expands the placeholders of credentials in the URL,
// such as ${S3_SECRET}, and the client options of a backend which calls
// other services over HTTP. The volume's name is the URL's name option, or
// its host. A backend which holds resources implements io.Closer, which the
// registry calls.
type Backend func(ctx context.Context, u *url.URL, opts gofiler.Opts) (gofiler.Filer, error)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// scheme is a URL's scheme, of RFC 3986, in lower case, as url.Parse
// returns it, of two characters or more, as one of a single letter is a
// Windows drive, such as C:/media, which ParseURL reads as a path
var scheme = regexp.MustCompile(`^[a-z][a-z0-9+.-]+$`)

// builtin returns the backends which are built in, of their schemes
func builtin() map[string]Backend {
	return map[string]Backend{
		schema.SchemeFile:     openFile,
		schema.SchemeS3:       openS3,
		google.TasksScheme:    openGTasks,
		google.ContactsScheme: openGContacts,
		google.CalendarScheme: openGCalendar,
	}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// WithBackend adds a backend, which the volumes whose URLs have its scheme
// are created with, such as one of another package, or program, which
// embeds the registry. A scheme is of two characters or more, in lower
// case, and one which another backend has, such as one which is built in,
// is an error.
func WithBackend(name string, backend Backend) Opt {
	return func(o *opt) error {
		if !scheme.MatchString(name) {
			return gofiler.ErrBadParameter.Withf("invalid scheme: %q", name)
		} else if backend == nil {
			return gofiler.ErrBadParameter.Withf("the backend of scheme %q is nil", name)
		} else if _, exists := o.backends[name]; exists {
			return gofiler.ErrConflict.Withf("a backend of scheme %q is already registered", name)
		}
		o.backends[name] = backend
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// openFile creates the backend of a directory, of file://<name>/<path>,
// whose path is absolute, so a URL without one isn't the directory the
// registry is running in. On Windows, it's /C:/media, which is converted to
// C:/media.
func openFile(_ context.Context, u *url.URL, opts gofiler.Opts) (gofiler.Filer, error) {
	if !types.IsIdentifier(u.Host) {
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", u.Host)
	}
	path := u.Path
	if runtime.GOOS == "windows" {
		path = types.FilePath(&url.URL{Scheme: schema.SchemeFile, Path: u.Path})
	}
	if path == "" || !filepath.IsAbs(filepath.FromSlash(path)) {
		return nil, gofiler.ErrBadParameter.Withf("path of volume %q isn't absolute: %q", u.Host, path)
	}
	return fs.New(u.Host, path, u.Query(), opts)
}

// openS3 creates the backend of a bucket, of s3://<bucket>/<prefix>
func openS3(ctx context.Context, u *url.URL, opts gofiler.Opts) (gofiler.Filer, error) {
	return s3.New(ctx, u, opts)
}

// openGTasks creates the backend of the tasks of a Google account, of its
// sign-in, of the provider of credentials, which is a SignInError, with how
// to sign in, when it's needed
func openGTasks(ctx context.Context, u *url.URL, opts gofiler.Opts) (gofiler.Filer, error) {
	if u.Path != "" && u.Path != "/" {
		return nil, gofiler.ErrBadParameter.Withf("a volume of Google Tasks has no path: %q", u.Path)
	}
	return google.NewTaskFiler(ctx, u.Host, u.Query(), opts)
}

// openGContacts creates the backend of the contacts of a Google account, of
// its sign-in, as for tasks
func openGContacts(ctx context.Context, u *url.URL, opts gofiler.Opts) (gofiler.Filer, error) {
	if u.Path != "" && u.Path != "/" {
		return nil, gofiler.ErrBadParameter.Withf("a volume of Google Contacts has no path: %q", u.Path)
	}
	return google.NewContactFiler(ctx, u.Host, u.Query(), opts)
}

// openGCalendar creates the backend of the events of a Google account's
// calendars, of its sign-in, as for tasks, whose sync tokens are stored with
// its credentials
func openGCalendar(ctx context.Context, u *url.URL, opts gofiler.Opts) (gofiler.Filer, error) {
	if u.Path != "" && u.Path != "/" {
		return nil, gofiler.ErrBadParameter.Withf("a volume of Google Calendar has no path: %q", u.Path)
	}
	return google.NewCalendarFiler(ctx, u.Host, u.Query(), opts)
}
