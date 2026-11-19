package google_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	gofiler "github.com/mutablelogic/go-filer"
	google "github.com/mutablelogic/go-filer/backend/google"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// fakeCalendar is the Calendar API, of the calendars' events, each of the
// version it was last changed of, and of sync tokens, of the version of a
// calendar, "v<n>"
type fakeCalendar struct {
	sync.Mutex
	calendars []httpclient.Calendar
	events    map[string][]fakeEvent // By the calendar's id
	version   map[string]int
	gone      map[string]bool // Calendars whose tokens are rejected
	syncs     map[string]int  // How many times each calendar is synced without a token
}

type fakeEvent struct {
	httpclient.Event
	version int
}

const (
	work    = "work@example.com"
	holiday = "en.uk#holiday@group.v.calendar.google.com"
)

func newFakeCalendar(t *testing.T) (*fakeCalendar, string) {
	t.Helper()
	fake := &fakeCalendar{
		calendars: []httpclient.Calendar{{Id: work, Summary: "Work", TimeZone: "Europe/Berlin", Primary: true}, {Id: holiday, Summary: "Holidays in the UK"}},
		events:    make(map[string][]fakeEvent),
		version:   make(map[string]int),
		gone:      make(map[string]bool),
		syncs:     make(map[string]int),
	}
	timed := func(id, start, end, tz string) httpclient.Event {
		return httpclient.Event{Id: id, ETag: `"` + id + `-1"`, Status: "confirmed", Summary: "Event " + id, Start: httpclient.EventTime{DateTime: start, TimeZone: tz}, End: httpclient.EventTime{DateTime: end, TimeZone: tz}, Updated: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	}
	allDay := func(id, start, end string) httpclient.Event {
		return httpclient.Event{Id: id, ETag: `"` + id + `-1"`, Status: "confirmed", Summary: "Event " + id, Start: httpclient.EventTime{Date: start}, End: httpclient.EventTime{Date: end}, Updated: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}
	}
	occurrence := timed("rec_20261105T100000Z", "2026-11-05T10:00:00Z", "2026-11-05T11:00:00Z", "")
	occurrence.RecurringEventId = "rec"
	occurrence.ICalUID = "rec@google.com"
	occurrence.HTMLLink = "https://calendar.google.com/event?eid=rec"
	occurrence.OriginalStartTime = httpclient.EventTime{DateTime: "2026-11-05T10:00:00Z"}
	cancelled := timed("gone", "2026-11-21T10:00:00Z", "2026-11-21T11:00:00Z", "")
	cancelled.Status = "cancelled"
	fake.add(work,
		timed("e1", "2026-11-20T09:00:00+01:00", "2026-11-20T10:00:00+01:00", "Europe/Berlin"),
		allDay("e2", "2026-12-25", "2026-12-26"),
		timed("e3", "2026-12-01T07:30:00Z", "2026-12-01T08:30:00Z", "America/Los_Angeles"),
		timed("later", "2027-03-01T09:00:00Z", "2027-03-01T10:00:00Z", ""),
		occurrence, cancelled,
	)
	fake.add(holiday, allDay("h1", "2026-10-31", "2026-11-01"))
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	google.SetCalendarEndpoint(t, server.URL)
	return fake, server.URL
}

// add adds events to a calendar, of its next version
func (f *fakeCalendar) add(calendar string, events ...httpclient.Event) {
	f.version[calendar]++
	for _, event := range events {
		f.events[calendar] = append(f.events[calendar], fakeEvent{Event: event, version: f.version[calendar]})
	}
}

// change changes an event of a calendar, of its next version
func (f *fakeCalendar) change(calendar, id string, fn func(*httpclient.Event)) {
	f.Lock()
	defer f.Unlock()
	f.version[calendar]++
	for i := range f.events[calendar] {
		if f.events[calendar][i].Id == id {
			fn(&f.events[calendar][i].Event)
			f.events[calendar][i].version = f.version[calendar]
		}
	}
}

func (f *fakeCalendar) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.Lock()
	defer f.Unlock()
	if r.Header.Get("Authorization") != "Bearer "+token {
		w.Header().Set("WWW-Authenticate", `Bearer realm="https://accounts.google.com/", error="invalid_token"`)
		writeError(w, http.StatusUnauthorized)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.EscapedPath(), "/"), "/")
	for i := range parts {
		parts[i], _ = url.PathUnescape(parts[i])
	}
	query := r.URL.Query()
	switch {
	case len(parts) == 3 && parts[0] == "users" && parts[1] == "me" && parts[2] == "calendarList":
		writeJSON(w, map[string]any{"items": f.calendars})
	case len(parts) == 3 && parts[0] == "calendars" && parts[2] == "events" && query.Get("singleEvents") == "true":
		// The events between the times
		from, _ := time.Parse(time.RFC3339, query.Get("timeMin"))
		to, _ := time.Parse(time.RFC3339, query.Get("timeMax"))
		var items []httpclient.Event
		for _, e := range f.events[parts[1]] {
			if start := fakeStart(e.Event); !start.Before(from) && start.Before(to) {
				items = append(items, e.Event)
			}
		}
		writeJSON(w, map[string]any{"items": items})
	case len(parts) == 3 && parts[0] == "calendars" && parts[2] == "events":
		// The events which changed since the sync token, or every event
		calendar, since := parts[1], 0
		if token := query.Get("syncToken"); token != "" {
			if f.gone[calendar] {
				writeError(w, http.StatusGone)
				return
			}
			since, _ = strconv.Atoi(strings.TrimPrefix(token, "v"))
		} else {
			f.syncs[calendar]++
		}
		var items []map[string]string
		for _, e := range f.events[calendar] {
			if e.version > since {
				items = append(items, map[string]string{"id": e.Id})
			}
		}
		writeJSON(w, map[string]any{"items": items, "nextSyncToken": fmt.Sprintf("v%d", f.version[calendar])})
	case len(parts) == 4 && parts[0] == "calendars" && parts[2] == "events":
		for _, e := range f.events[parts[1]] {
			if e.Id == parts[3] {
				writeJSON(w, e.Event)
				return
			}
		}
		writeError(w, http.StatusNotFound)
	default:
		writeError(w, http.StatusNotFound)
	}
}

// fakeStart returns when an event starts, of its date, at midnight, in UTC
func fakeStart(e httpclient.Event) time.Time {
	if e.Start.Date != "" {
		t, _ := time.Parse(time.DateOnly, e.Start.Date)
		return t
	}
	t, _ := time.Parse(time.RFC3339, e.Start.DateTime)
	return t
}

// calendarCredentials returns the provider of a sign-in which can read
// calendars
func calendarCredentials(t *testing.T, endpoint string) *credentials {
	return newCredentials(t, endpoint, func(g *authclient.Grant) {
		g.Scopes = []string{httpclient.CalendarReadonlyScope}
	})
}

// newCalendarFiler returns a filer of the fake, of 40 days before, and
// after, 19 November 2026, of the credentials
func newCalendarFiler(t *testing.T, credentials *credentials, params url.Values) *google.CalendarFiler {
	t.Helper()
	if params == nil {
		params = url.Values{}
	}
	params.Set("quota", "0")
	if !params.Has("window") {
		params.Set("window", "40d")
	}
	filer, err := google.NewCalendarFiler(t.Context(), "calendar", params, gofiler.Opts{Credentials: credentials})
	require.NoError(t, err)
	google.SetCalendarNow(filer, func() time.Time { return time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC) })
	return filer
}

// listed returns the paths of a listing
func listed(t *testing.T, filer gofiler.Filer, req schema.ListObjectsRequest) []string {
	t.Helper()
	iter, err := filer.ListObjects(t.Context(), req)
	require.NoError(t, err)
	defer iter.Close()
	var result []string
	for {
		object, err := iter.Next(t.Context())
		if err == io.EOF {
			return result
		}
		require.NoError(t, err)
		result = append(result, object.Path)
	}
}

func Test_NewCalendarFiler(t *testing.T) {
	ctx := t.Context()
	_, endpoint := newFakeCalendar(t)

	// A sign-in without the calendar's scope signs in again, asking for it,
	// and keeping those it has, such as of tasks
	_, err := google.NewCalendarFiler(ctx, "calendar", url.Values{"quota": {"0"}}, gofiler.Opts{Credentials: newCredentials(t, endpoint, nil)})
	require.Equal(t, http.StatusUnauthorized, status(err))
	signIn := authclient.SignInOf(err)
	require.NotNil(t, signIn)
	assert.Equal(t, []string{httpclient.CalendarReadonlyScope, httpclient.EmailScope, httpclient.TasksScope}, signIn.Scopes)

	// Writing isn't implemented, so far, and parameters which aren't known,
	// or can't be parsed, are bad
	_, err = google.NewCalendarFiler(ctx, "calendar", url.Values{"readonly": {"false"}}, gofiler.Opts{Credentials: calendarCredentials(t, endpoint)})
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	for _, params := range []url.Values{{"hidden": {"true"}}, {"window": {"-1d"}}, {"window": {"3651d"}}, {"window": {"a year"}}, {"credential": {"S3_SECRET"}}} {
		_, err := google.NewCalendarFiler(ctx, "calendar", params, gofiler.Opts{Credentials: calendarCredentials(t, endpoint)})
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, params)
	}

	// Its URL has each of its parameters
	filer := newCalendarFiler(t, calendarCredentials(t, endpoint), nil)
	assert.Equal(t, "gcalendar://calendar?credential=GOOGLE_TOKEN&description=Google+Calendar&quota=0&readonly=true&refresh_interval=1m0s&window=40d", filer.URL().String())
	assert.True(t, filer.Readonly())
}

func Test_CalendarObjects(t *testing.T) {
	ctx := t.Context()
	_, endpoint := newFakeCalendar(t)
	filer := newCalendarFiler(t, calendarCredentials(t, endpoint), nil)

	// The years and months which are listed are directories, and each event
	// is of the day it starts on, in its own zone, and its calendar, and an
	// occurrence of one which repeats is an event of its own, but not one
	// which is cancelled, or isn't listed
	assert.Equal(t, []string{"/2026"}, listed(t, filer, schema.ListObjectsRequest{Path: "/"}))
	assert.Equal(t, []string{"/2026/2026-10", "/2026/2026-11", "/2026/2026-12"}, listed(t, filer, schema.ListObjectsRequest{Path: "/2026"}))
	all := []string{
		"/2026",
		"/2026/2026-10",
		"/2026/2026-10/31-en.uk_holiday@group.v.calendar.google.com-h1.ics",
		"/2026/2026-11",
		"/2026/2026-11/05-work@example.com-rec_20261105T100000Z.ics",
		"/2026/2026-11/20-work@example.com-e1.ics",
		"/2026/2026-11/30-work@example.com-e3.ics",
		"/2026/2026-12",
		"/2026/2026-12/25-work@example.com-e2.ics",
	}
	assert.Equal(t, all, listed(t, filer, schema.ListObjectsRequest{Path: "/", Recursive: true}))
	assert.Equal(t, all[4:], listed(t, filer, schema.ListObjectsRequest{Path: "/", Recursive: true, StartAfter: all[3] + "/"}), "after a path")
	assert.Equal(t, all[4:7], listed(t, filer, schema.ListObjectsRequest{Path: "/2026/2026-11"}))

	// A directory has the count of what's in it
	for p, count := range map[string]string{"/": "1", "/2026": "3", "/2026/2026-11": "3", "/2026/2026-12": "1"} {
		object, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: p}})
		require.NoError(t, err, p)
		assert.True(t, object.IsDir, p)
		assert.Contains(t, metaValues(object, "folder"), count, p)
	}

	// An event is read as an iCalendar, of its zone, or a date, with its
	// metadata
	object, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: all[5]}})
	require.NoError(t, err)
	assert.Equal(t, `"e1-1"`, *object.ETag)
	assert.Contains(t, metaValues(object, "dc"), `"Event e1"`)
	assert.Contains(t, metaValues(object, google.CalendarNamespace), `"work@example.com"`)
	assert.Contains(t, read(t, filer, all[5]), "DTSTART;TZID=Europe/Berlin:20261120T090000")
	assert.Contains(t, read(t, filer, all[8]), "DTSTART;VALUE=DATE:20261225")
	assert.Contains(t, read(t, filer, all[2]), "X-WR-CALNAME:Holidays in the UK")

	// An event's UID is Google's iCalendar UID, as an invitation's is, or
	// its id, and a time an event which repeats occurs is of when it was to
	// start, and an event is of the day of its zone, rather than its offset,
	// which can be another's
	assert.Contains(t, read(t, filer, all[5]), "UID:e1")
	object, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: all[4]}})
	require.NoError(t, err)
	assert.Contains(t, metaValues(object, "dc"), `"rec@google.com"`, "its dc:identifier is its UID")
	assert.Contains(t, metaValues(object, google.CalendarNamespace), `"https://calendar.google.com/event?eid=rec"`, "its link")
	occurrence := read(t, filer, all[4])
	assert.Contains(t, occurrence, "UID:rec@google.com")
	assert.Contains(t, occurrence, "RECURRENCE-ID:20261105T100000Z")
	assert.Contains(t, read(t, filer, all[6]), "DTSTART;TZID=America/Los_Angeles:20261130T233000")

	// An event which is cancelled, isn't listed, isn't of its day, or its
	// calendar, or a directory which isn't listed, isn't there
	for _, p := range []string{
		"/2026/2026-11/21-work@example.com-gone.ics",
		"/2027/2027-03/01-work@example.com-later.ics",
		"/2026/2026-11/21-work@example.com-e1.ics",
		"/2026/2026-11/20-other@example.com-e1.ics",
		"/2026/2026-11/20-work@example.com-missing.ics",
		"/2027", "/2026/2026-09", "/2026/2025-11", "/2026/2026-13", "/other",
	} {
		_, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: p}})
		assert.ErrorIs(t, err, fs.ErrNotExist, p)
	}

	// A directory can't be read, and it can't be written
	_, _, err = filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/2026"}})
	assert.ErrorIs(t, err, fs.ErrInvalid)
	_, err = filer.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/2026/2026-11/new.ics"}})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
}

func Test_CalendarWatch(t *testing.T) {
	fake, endpoint := newFakeCalendar(t)
	credentials := calendarCredentials(t, endpoint)
	filer := newCalendarFiler(t, credentials, url.Values{"refresh_interval": {"10ms"}})
	watch := func() (chan schema.Event, func()) {
		ch, done := make(chan schema.Event, 16), make(chan error, 1)
		ctx, cancel := context.WithCancel(t.Context())
		go func() { done <- filer.WatchObjects(ctx, ch) }()
		return ch, func() {
			cancel()
			assert.ErrorIs(t, <-done, context.Canceled)
		}
	}
	next := func(ch chan schema.Event) *schema.Event {
		select {
		case event := <-ch:
			return &event
		case <-time.After(2 * time.Second):
			return nil
		}
	}
	quiet := func(ch chan schema.Event) {
		t.Helper()
		select {
		case event := <-ch:
			t.Fatalf("unexpected %v", event)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// The first change is a rescan of the root, once each calendar is synced,
	// for its token, which is stored
	ch, stop := watch()
	event := next(ch)
	require.NotNil(t, event)
	assert.Equal(t, schema.EventTypeRescan, event.EventType)
	var tokens map[string]string
	require.Eventually(t, func() bool {
		data, err := credentials.Get(t.Context(), "GOOGLE_TOKEN_CALENDAR_SYNC")
		return err == nil && json.Unmarshal(data, &tokens) == nil && len(tokens) == 2
	}, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, map[string]string{work: "v1", holiday: "v1"}, tokens)

	// Nothing changes, so nothing is sent, until an event does, which is a
	// rescan
	quiet(ch)
	fake.change(work, "e1", func(e *httpclient.Event) { e.Summary = "Changed" })
	event = next(ch)
	require.NotNil(t, event)
	assert.Equal(t, schema.EventTypeRescan, event.EventType)
	quiet(ch)

	// A token which is rejected is synced again, for another, which is a
	// rescan, as changes may have been missed, and isn't synced again for a
	// while, when it's rejected again
	fake.Lock()
	fake.gone[holiday] = true
	fake.Unlock()
	event = next(ch)
	require.NotNil(t, event)
	assert.Equal(t, schema.EventTypeRescan, event.EventType)
	stop()
	fake.Lock()
	assert.Equal(t, 2, fake.syncs[holiday], "once, for its first token, and once again")
	assert.Equal(t, 1, fake.syncs[work])
	fake.gone[holiday] = false
	fake.Unlock()

	// When it's watched again, the tokens which are stored are synced from,
	// rather than every event again
	ch, stop = watch()
	defer stop()
	event = next(ch)
	require.NotNil(t, event)
	assert.Equal(t, schema.EventTypeRescan, event.EventType)
	quiet(ch)
	fake.Lock()
	assert.Equal(t, 1, fake.syncs[work], "its stored token is used")
	fake.Unlock()
}

// metaValues returns the values of the metadata of a namespace
func metaValues(object *schema.Object, ns string) []string {
	var result []string
	for _, m := range object.Meta {
		if m.Namespace == ns {
			result = append(result, string(m.Value))
		}
	}
	return result
}

// read returns the content of a file
func read(t *testing.T, filer gofiler.Filer, p string) string {
	t.Helper()
	fn, _, err := filer.ReadObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: p}})
	require.NoError(t, err)
	var result string
	require.NoError(t, fn(t.Context(), func(r io.Reader) error {
		data, err := io.ReadAll(r)
		result = string(data)
		return err
	}))
	return result
}

func Test_CalendarWindow(t *testing.T) {
	_, endpoint := newFakeCalendar(t)

	// The days of the window, before today, and after it, which are of 30
	// October to 9 December, are listed, of the months they're in, whose
	// events of other days aren't
	filer := newCalendarFiler(t, calendarCredentials(t, endpoint), url.Values{"window": {"20d"}})
	assert.Equal(t, []string{
		"/2026",
		"/2026/2026-10",
		"/2026/2026-10/31-en.uk_holiday@group.v.calendar.google.com-h1.ics",
		"/2026/2026-11",
		"/2026/2026-11/05-work@example.com-rec_20261105T100000Z.ics",
		"/2026/2026-11/20-work@example.com-e1.ics",
		"/2026/2026-11/30-work@example.com-e3.ics",
		"/2026/2026-12",
	}, listed(t, filer, schema.ListObjectsRequest{Path: "/", Recursive: true}))
	_, err := filer.GetObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/2026/2026-12/25-work@example.com-e2.ics"}})
	assert.ErrorIs(t, err, fs.ErrNotExist, "it isn't of a day which is listed")

	// Weeks, and durations, are of whole days
	filer = newCalendarFiler(t, calendarCredentials(t, endpoint), url.Values{"window": {"1w36h"}})
	assert.Equal(t, "8d", filer.URL().Query().Get("window"))
}
