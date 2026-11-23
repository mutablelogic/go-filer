package google_test

import (
	"context"
	"net/url"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	google "github.com/mutablelogic/go-filer/backend/google"
	assert "github.com/stretchr/testify/assert"
)

func Test_Backends(t *testing.T) {
	// Each of Google's backends describes itself, with its icon, and the
	// options of a volume which aren't set, every service's, and its own
	for _, test := range []struct {
		backend             gofiler.Backend
		scheme, description string
		own                 url.Values
	}{
		{google.TasksBackend(), "gtasks", "Google Tasks", url.Values{"hidden": {"false"}}},
		{google.ContactsBackend(), "gcontacts", "Google Contacts", nil},
		{google.CalendarBackend(), "gcalendar", "Google Calendar", url.Values{"window": {"365d"}}},
		{google.DriveBackend(), "gdrive", "Google Drive", nil},
	} {
		b := test.backend
		assert.Equal(t, test.scheme, b.Scheme())
		assert.Equal(t, test.description, b.Description())
		if assert.NotNil(t, b.Icon(), test.scheme) {
			assert.Equal(t, "image/svg+xml", b.Icon().Type())
		}
		defaults := b.Defaults()
		assert.Equal(t, "true", defaults.Get("readonly"), test.scheme)
		assert.Equal(t, "120", defaults.Get("quota"), test.scheme)
		assert.Equal(t, "1m0s", defaults.Get("refresh_interval"), test.scheme)
		assert.Equal(t, "GOOGLE_TOKEN", defaults.Get("credential"), test.scheme)
		for key, value := range test.own {
			assert.Equal(t, value, defaults[key], test.scheme)
		}

		// Its defaults are a copy, which a caller can change
		defaults.Set("readonly", "false")
		assert.Equal(t, "true", b.Defaults().Get("readonly"), test.scheme)

		// A volume of it has no path
		_, err := b.New(context.Background(), &url.URL{Scheme: test.scheme, Host: "x", Path: "/a"}, nil, gofiler.Opts{})
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, test.scheme)
	}
}
