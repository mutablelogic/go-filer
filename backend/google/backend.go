package google

import (
	"context"
	_ "embed"
	"net/url"
	"strconv"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	icon "github.com/mutablelogic/go-filer/backend/internal/icon"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// backend is a backend of a Google service, of its scheme, what it is, its
// icon, and how a volume of it is opened, of its name, and its options. A
// volume of it has no path, as it's the account's, of its sign-in, of the
// provider of credentials, which is a SignInError, with how to sign in, when
// it's needed.
type backend struct {
	scheme, description string
	icon                []byte
	defaults            url.Values
	open                func(ctx context.Context, name string, values url.Values, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error)
}

var _ gofiler.Backend = backend{}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	//go:embed tasks.svg
	tasksIcon []byte

	//go:embed contacts.svg
	contactsIcon []byte

	//go:embed calendar.svg
	calendarIcon []byte

	//go:embed drive.svg
	driveIcon []byte
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// TasksBackend returns the backend of a Google account's tasks, of URLs of
// gtasks://<name>
func TasksBackend() gofiler.Backend {
	return backend{TasksScheme, "Google Tasks", tasksIcon, defaults(url.Values{paramHidden: {"false"}}), func(ctx context.Context, name string, values url.Values, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
		return NewTaskFiler(ctx, name, values, cache, opts)
	}}
}

// ContactsBackend returns the backend of a Google account's contacts, of
// URLs of gcontacts://<name>
func ContactsBackend() gofiler.Backend {
	return backend{ContactsScheme, "Google Contacts", contactsIcon, defaults(nil), func(ctx context.Context, name string, values url.Values, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
		return NewContactFiler(ctx, name, values, cache, opts)
	}}
}

// CalendarBackend returns the backend of the events of a Google account's
// calendars, of URLs of gcalendar://<name>, whose sync tokens are stored
// with its credentials
func CalendarBackend() gofiler.Backend {
	return backend{CalendarScheme, "Google Calendar", calendarIcon, defaults(url.Values{paramWindow: {formatWindow(DefaultCalendarWindow)}}), func(ctx context.Context, name string, values url.Values, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
		return NewCalendarFiler(ctx, name, values, cache, opts)
	}}
}

// DriveBackend returns the backend of the files of a Google account's My
// Drive, of URLs of gdrive://<name>
func DriveBackend() gofiler.Backend {
	return backend{DriveScheme, "Google Drive", driveIcon, defaults(nil), func(ctx context.Context, name string, values url.Values, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
		return NewDriveFiler(ctx, name, values, cache, opts)
	}}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (b backend) Scheme() string      { return b.scheme }
func (b backend) Description() string { return b.description }
func (b backend) Icon() gofiler.Icon  { return icon.SVG(b.icon) }

// Defaults returns the options of a volume which aren't set, of every Google
// service's, and the service's own, which is a copy
func (b backend) Defaults() url.Values {
	result := make(url.Values, len(b.defaults))
	for key, values := range b.defaults {
		result[key] = append([]string(nil), values...)
	}
	return result
}

// New opens the volume of <scheme>://<name>, which has no path
func (b backend) New(ctx context.Context, u *url.URL, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
	if u.Path != "" && u.Path != "/" {
		return nil, gofiler.ErrBadParameter.Withf("a volume of %s has no path: %q", b.description, u.Path)
	}
	return b.open(ctx, u.Host, u.Query(), cache, opts)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// defaults returns the options of a volume of a Google service which aren't
// set, of every service's: read-only, its quota, how often it's refreshed,
// and its sign-in, with the service's own
func defaults(own url.Values) url.Values {
	result := url.Values{
		paramReadonly:        {"true"},
		paramQuota:           {strconv.FormatUint(DefaultTasksQuota, 10)},
		paramRefreshInterval: {DefaultRefreshInterval.String()},
		paramCredential:      {DefaultCredential},
	}
	for key, values := range own {
		result[key] = values
	}
	return result
}
