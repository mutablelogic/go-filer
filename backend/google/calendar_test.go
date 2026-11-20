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
	calendars     []httpclient.Calendar
	events        map[string][]fakeEvent // By the calendar's id
	version       map[string]int
	gone          map[string]bool            // Calendars whose tokens are rejected
	syncs         map[string]int             // How many times each calendar is synced without a token
	lists         int                        // How many times events are listed
	gets          int                        // How many times an event is got
	calendarLists int                        // How many times the calendars are listed
	endless       bool                       // Every listing has another page
	fields        map[string]json.RawMessage // The fields of the last write
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
		calendars: []httpclient.Calendar{{Id: work, ETag: `"w1"`, Summary: "Work", TimeZone: "Europe/Berlin", Primary: true}, {Id: holiday, ETag: `"h1"`, Summary: "Holidays in the UK"}},
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

// put adds an event to a calendar, of its next version
func (f *fakeCalendar) put(calendar string, event httpclient.Event) {
	f.Lock()
	defer f.Unlock()
	f.add(calendar, event)
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
	if r.Method != http.MethodGet {
		f.write(w, r, parts)
		return
	}
	switch {
	case len(parts) == 3 && parts[0] == "users" && parts[1] == "me" && parts[2] == "calendarList":
		f.calendarLists++
		page := map[string]any{"items": f.calendars}
		if f.endless {
			page["nextPageToken"] = "more"
		}
		writeJSON(w, page)
	case len(parts) == 3 && parts[0] == "calendars" && parts[2] == "events" && query.Get("singleEvents") == "true":
		// The events between the times
		from, _ := time.Parse(time.RFC3339, query.Get("timeMin"))
		to, _ := time.Parse(time.RFC3339, query.Get("timeMax"))
		f.lists++
		var items []httpclient.Event
		for _, e := range f.events[parts[1]] {
			if start := fakeStart(e.Event); len(e.Recurrence) == 0 && e.Status != "cancelled" && !start.Before(from) && start.Before(to) {
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
		var items []any
		for _, e := range f.events[calendar] {
			if e.version > since && since > 0 {
				items = append(items, httpclient.Event{Id: e.Id, ETag: e.ETag, Status: e.Status, Start: e.Start, Recurrence: e.Recurrence, RecurringEventId: e.RecurringEventId})
			} else if e.version > since {
				items = append(items, map[string]string{"id": e.Id})
			}
		}
		writeJSON(w, map[string]any{"items": items, "nextSyncToken": fmt.Sprintf("v%d", f.version[calendar])})
	case len(parts) == 4 && parts[0] == "calendars" && parts[2] == "events":
		f.gets++
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

// write creates, changes, or deletes, which cancels, an event, as the
// Calendar API does, of the calendar's next version
func (f *fakeCalendar) write(w http.ResponseWriter, r *http.Request, parts []string) {
	if len(parts) < 3 || parts[0] != "calendars" || parts[2] != "events" {
		writeError(w, http.StatusNotFound)
		return
	}
	calendar := parts[1]
	var fields map[string]json.RawMessage
	if r.URL.Query().Get("sendUpdates") != "none" {
		// Those who are invited aren't sent emails
		writeError(w, http.StatusBadRequest)
		return
	} else if r.Method != http.MethodDelete {
		if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
			writeError(w, http.StatusBadRequest)
			return
		}
	}
	f.fields = fields
	apply := func(e *httpclient.Event) {
		for key, raw := range fields {
			switch key {
			case "summary":
				_ = json.Unmarshal(raw, &e.Summary)
			case "description":
				_ = json.Unmarshal(raw, &e.Description)
			case "location":
				_ = json.Unmarshal(raw, &e.Location)
			case "start":
				e.Start = httpclient.EventTime{}
				_ = json.Unmarshal(raw, &e.Start)
			case "end":
				e.End = httpclient.EventTime{}
				_ = json.Unmarshal(raw, &e.End)
			case "attendees":
				e.Attendees = nil
				_ = json.Unmarshal(raw, &e.Attendees)
			}
		}
	}
	f.version[calendar]++
	switch {
	case r.Method == http.MethodPost && len(parts) == 3:
		id := "n" + strconv.Itoa(len(f.events[calendar])+1)
		event := httpclient.Event{Id: id, ETag: `"` + id + `-1"`, Status: "confirmed", Updated: time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)}
		apply(&event)
		f.events[calendar] = append(f.events[calendar], fakeEvent{Event: event, version: f.version[calendar]})
		writeJSON(w, event)
		return
	case len(parts) == 4:
		for i := range f.events[calendar] {
			e := &f.events[calendar][i]
			if e.Id != parts[3] || e.Status == "cancelled" {
				continue
			}
			e.version = f.version[calendar]
			e.ETag = `"` + e.Id + "-" + strconv.Itoa(e.version) + `"`
			if r.Method == http.MethodDelete {
				e.Status = "cancelled"
				w.WriteHeader(http.StatusNoContent)
				return
			}
			apply(&e.Event)
			writeJSON(w, e.Event)
			return
		}
	}
	writeError(w, http.StatusNotFound)
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
	filer, err := google.NewCalendarFiler(t.Context(), "calendar", params, testCache(), gofiler.Opts{Credentials: credentials})
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
	_, err := google.NewCalendarFiler(ctx, "calendar", url.Values{"quota": {"0"}}, testCache(), gofiler.Opts{Credentials: newCredentials(t, endpoint, nil)})
	require.Equal(t, http.StatusUnauthorized, status(err))
	signIn := authclient.SignInOf(err)
	require.NotNil(t, signIn)
	assert.Equal(t, []string{httpclient.CalendarReadonlyScope, httpclient.EmailScope, httpclient.TasksScope}, signIn.Scopes)

	// A writable volume of a sign-in which can only read calendars signs in
	// again, asking for the scope of writing their events
	_, err = google.NewCalendarFiler(ctx, "calendar", url.Values{"readonly": {"false"}, "quota": {"0"}}, testCache(), gofiler.Opts{Credentials: calendarCredentials(t, endpoint)})
	require.Equal(t, http.StatusUnauthorized, status(err))
	signIn = authclient.SignInOf(err)
	require.NotNil(t, signIn)
	assert.Contains(t, signIn.Scopes, httpclient.CalendarScope)

	// Parameters which aren't known, or can't be parsed, are bad
	for _, params := range []url.Values{{"readonly": {"maybe"}}, {"hidden": {"true"}}, {"window": {"-1d"}}, {"window": {"3651d"}}, {"window": {"a year"}}, {"credential": {"S3_SECRET"}}, {"credential": {"GOOGLE_TOKEN_CALENDAR_SYNC"}}} {
		_, err := google.NewCalendarFiler(ctx, "calendar", params, testCache(), gofiler.Opts{Credentials: calendarCredentials(t, endpoint)})
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
	assert.Equal(t, `"e1-1"/"w1"`, *object.ETag, "of its calendar's too")
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
	assert.Equal(t, []string{`"work@example.com"`}, metaValues(object, google.CalendarNamespace), "only what its iCalendar can't have")
	assert.Contains(t, read(t, filer, all[4]), "URL:https://calendar.google.com/event?eid=rec")
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

func Test_CalendarChanges(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFakeCalendar(t)
	filer := newCalendarFiler(t, calendarCredentials(t, endpoint), nil)
	const p = "/2026/2026-11/20-work@example.com-e1.ics"

	// The root's years are of the window alone, so its events aren't
	// listed, unless it's recursive
	assert.Equal(t, []string{"/2026"}, listed(t, filer, schema.ListObjectsRequest{Path: "/"}))
	fake.Lock()
	assert.Zero(t, fake.lists)
	fake.Unlock()

	// An event's entity tag, and content, are of its calendar's, so one which
	// is renamed changes them
	before, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: p}})
	require.NoError(t, err)
	fake.Lock()
	fake.calendars[0].Summary, fake.calendars[0].ETag = "Home", `"w2"`
	fake.Unlock()
	google.ClearCalendarCache(filer) // As a watch does
	after, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: p}})
	require.NoError(t, err)
	assert.NotEqual(t, *before.ETag, *after.ETag)
	assert.Contains(t, read(t, filer, p), "X-WR-CALNAME:Home")

	// A listing which has more pages than are read is an error, rather than
	// what's been read, which would be the deletion of what isn't
	fake.Lock()
	fake.endless = true
	fake.Unlock()
	google.ClearCalendarCache(filer)
	_, err = filer.ListObjects(ctx, schema.ListObjectsRequest{Path: "/", Recursive: true})
	assert.Error(t, err)
}

func Test_CalendarWatchRename(t *testing.T) {
	fake, endpoint := newFakeCalendar(t)
	filer := newCalendarFiler(t, calendarCredentials(t, endpoint), url.Values{"refresh_interval": {"10ms"}})
	ch, done := make(chan schema.Event, 16), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { done <- filer.WatchObjects(ctx, ch) }()
	defer func() {
		cancel()
		assert.ErrorIs(t, <-done, context.Canceled)
	}()
	next := func() *schema.Event {
		select {
		case event := <-ch:
			return &event
		case <-time.After(2 * time.Second):
			return nil
		}
	}

	// A calendar which is renamed, without its events changing, is a rescan,
	// as their content is of its name
	require.NotNil(t, next(), "the first rescan")
	select {
	case event := <-ch:
		t.Fatalf("unexpected %v", event)
	case <-time.After(100 * time.Millisecond):
	}
	fake.Lock()
	fake.calendars[0].Summary, fake.calendars[0].ETag = "Home", `"w2"`
	fake.Unlock()
	event := next()
	require.NotNil(t, event)
	assert.Equal(t, schema.EventTypeRescan, event.EventType)
}

func Test_CalendarWatchHints(t *testing.T) {
	fake, endpoint := newFakeCalendar(t)
	filer := newCalendarFiler(t, calendarCredentials(t, endpoint), url.Values{"refresh_interval": {"10ms"}})
	ch, done := make(chan schema.Event, 16), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { done <- filer.WatchObjects(ctx, ch) }()
	defer func() {
		cancel()
		assert.ErrorIs(t, <-done, context.Canceled)
	}()
	// events returns the events which are sent, until none are, for a while
	events := func() []string {
		t.Helper()
		var result []string
		for {
			select {
			case event := <-ch:
				result = append(result, event.EventType.String()+" "+event.Path)
			case <-time.After(150 * time.Millisecond):
				return result
			}
		}
	}
	const e1 = "/2026/2026-11/20-work@example.com-e1.ics"

	// Before anything is listed, where an event was isn't known, so a change
	// is a rescan
	assert.Equal(t, []string{"rescan /"}, events(), "the first rescan")
	fake.change(work, "e1", func(e *httpclient.Event) { e.Summary = "Changed" })
	assert.Equal(t, []string{"rescan /"}, events())

	// Once it's listed, an event which changes is updated, one which moves
	// is deleted where it was, and created where it is, and one which is
	// created is, but not one which isn't of the days which are listed
	listed(t, filer, schema.ListObjectsRequest{Path: "/", Recursive: true})
	fake.change(work, "e1", func(e *httpclient.Event) { e.Summary = "Changed again" })
	assert.Equal(t, []string{"updated " + e1}, events())
	fake.change(work, "e1", func(e *httpclient.Event) {
		e.Start.DateTime, e.End.DateTime = "2026-11-22T09:00:00+01:00", "2026-11-22T10:00:00+01:00"
	})
	moved := "/2026/2026-11/22-work@example.com-e1.ics"
	assert.Equal(t, []string{"deleted " + e1, "created " + moved}, events())
	fake.put(work, httpclient.Event{Id: "new", Status: "confirmed", Start: httpclient.EventTime{Date: "2026-12-03"}, End: httpclient.EventTime{Date: "2026-12-04"}})
	fake.put(work, httpclient.Event{Id: "far", Status: "confirmed", Start: httpclient.EventTime{Date: "2027-06-01"}, End: httpclient.EventTime{Date: "2027-06-02"}})
	assert.Equal(t, []string{"created /2026/2026-12/03-work@example.com-new.ics"}, events())

	// An event which is cancelled is deleted, as is each time one which
	// repeats occurs, when it's cancelled
	fake.change(work, "e2", func(e *httpclient.Event) { e.Status = "cancelled" })
	assert.Equal(t, []string{"deleted /2026/2026-12/25-work@example.com-e2.ics"}, events())
	fake.put(work, httpclient.Event{Id: "rec", Status: "cancelled"})
	assert.Equal(t, []string{"deleted /2026/2026-11/05-work@example.com-rec_20261105T100000Z.ics"}, events())

	// An event which moves, which a listing of where it is now has, before
	// the move is synced, is still deleted where it was, as a listing doesn't
	// replace where an event is known to be
	fake.change(work, "e3", func(e *httpclient.Event) {
		e.Start.DateTime, e.End.DateTime = "2026-12-10T09:00:00Z", "2026-12-10T10:00:00Z"
	})
	listed(t, filer, schema.ListObjectsRequest{Path: "/2026/2026-12"})
	assert.Equal(t, []string{"deleted /2026/2026-11/30-work@example.com-e3.ics", "created /2026/2026-12/10-work@example.com-e3.ics"}, events())

	// An event which repeats, which changed, is a rescan, as the times it
	// occurs aren't said
	fake.put(work, httpclient.Event{Id: "series", Status: "confirmed", Recurrence: []string{"RRULE:FREQ=WEEKLY"}, Start: httpclient.EventTime{DateTime: "2026-11-02T09:00:00Z"}})
	assert.Equal(t, []string{"rescan /"}, events())
}

func Test_CalendarListedPaths(t *testing.T) {
	fake, endpoint := newFakeCalendar(t)
	filer := newCalendarFiler(t, calendarCredentials(t, endpoint), url.Values{"refresh_interval": {"0"}})

	// A listing of where an event is now, which moved, doesn't replace
	// where it's known to be, so a watch deletes it there, as the index has it
	listed(t, filer, schema.ListObjectsRequest{Path: "/", Recursive: true})
	assert.Equal(t, "/2026/2026-11/30-work@example.com-e3.ics", google.CalendarPath(filer, work, "e3"))
	fake.change(work, "e3", func(e *httpclient.Event) {
		e.Start.DateTime, e.End.DateTime = "2026-12-10T09:00:00Z", "2026-12-10T10:00:00Z"
	})
	listed(t, filer, schema.ListObjectsRequest{Path: "/2026/2026-12"})
	assert.Equal(t, "/2026/2026-11/30-work@example.com-e3.ics", google.CalendarPath(filer, work, "e3"))

	// But one which isn't known is added
	fake.put(work, httpclient.Event{Id: "added", Status: "confirmed", Start: httpclient.EventTime{Date: "2026-12-11"}, End: httpclient.EventTime{Date: "2026-12-12"}})
	listed(t, filer, schema.ListObjectsRequest{Path: "/2026/2026-12"})
	assert.Equal(t, "/2026/2026-12/11-work@example.com-added.ics", google.CalendarPath(filer, work, "added"))
}

func Test_CalendarCache(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFakeCalendar(t)
	filer := newCalendarFiler(t, calendarCredentials(t, endpoint), nil)
	const p = "/2026/2026-11/20-work@example.com-e1.ics"
	counts := func() (int, int, int) {
		fake.Lock()
		defer fake.Unlock()
		return fake.calendarLists, fake.lists, fake.gets
	}

	// Once a month is listed, its events, and they're read, of the listing,
	// rather than got again, as is listing it again, for the refresh interval
	checked, _, _ := counts() // Of the check of the sign-in
	listed(t, filer, schema.ListObjectsRequest{Path: "/2026/2026-11"})
	calendarLists, lists, gets := counts()
	assert.Equal(t, checked+1, calendarLists)
	assert.Equal(t, 2, lists, "of each calendar")
	assert.Zero(t, gets)
	listed(t, filer, schema.ListObjectsRequest{Path: "/2026/2026-11"})
	_, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: p}})
	require.NoError(t, err)
	assert.Contains(t, read(t, filer, p), "SUMMARY:Event e1")
	_, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/2026/2026-11/20-work@example.com-missing.ics"}})
	assert.ErrorIs(t, err, fs.ErrNotExist, "it isn't in the listing")
	c, l, g := counts()
	assert.Equal(t, []int{calendarLists, lists, gets}, []int{c, l, g}, "nothing is got again")

	// Once it's cleared, as a watch does when what's got changes, it's got
	// again, as is an event of a month which wasn't listed
	fake.change(work, "e1", func(e *httpclient.Event) { e.Summary = "Changed" })
	google.ClearCalendarCache(filer)
	assert.Contains(t, read(t, filer, p), "SUMMARY:Changed")
	_, _, g = counts()
	assert.Equal(t, gets+1, g)
	assert.Contains(t, read(t, filer, "/2026/2026-12/25-work@example.com-e2.ics"), "DTSTART;VALUE=DATE:20261225")

	// A volume which isn't watched, of a refresh interval of zero, keeps
	// nothing, as nothing would clear it
	unwatched := newCalendarFiler(t, calendarCredentials(t, endpoint), url.Values{"refresh_interval": {"0"}})
	listed(t, unwatched, schema.ListObjectsRequest{Path: "/2026/2026-11"})
	_, _, g = counts()
	_, err = unwatched.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: p}})
	require.NoError(t, err)
	_, _, after := counts()
	assert.Equal(t, g+1, after, "it's got again")
}

func Test_CalendarWatchClearsCache(t *testing.T) {
	fake, endpoint := newFakeCalendar(t)
	filer := newCalendarFiler(t, calendarCredentials(t, endpoint), url.Values{"refresh_interval": {"1h"}})
	const p = "/2026/2026-11/20-work@example.com-e1.ics"

	// What's listed before it's watched, which changes, is got again of the
	// first rescan, as the watch doesn't send what it found changed
	listed(t, filer, schema.ListObjectsRequest{Path: "/2026/2026-11"})
	fake.change(work, "e1", func(e *httpclient.Event) { e.Summary = "Changed before" })
	ch, done := make(chan schema.Event, 4), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { done <- filer.WatchObjects(ctx, ch) }()
	defer func() {
		cancel()
		<-done
	}()
	select {
	case event := <-ch:
		require.Equal(t, schema.EventTypeRescan, event.EventType)
	case <-time.After(2 * time.Second):
		t.Fatal("no rescan")
	}
	assert.Contains(t, read(t, filer, p), "SUMMARY:Changed before")
}

// A writable volume's event action creates an event, in the primary
// calendar, or another, updates the fields of one which are given, or of
// one time one which repeats occurs, and deletes one
func Test_EventAction(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFakeCalendar(t)
	assert.Nil(t, newCalendarFiler(t, calendarCredentials(t, endpoint), nil).Actions(), "a read-only volume has none")
	_, err := google.NewCalendarFiler(ctx, "calendar", url.Values{"readonly": {"false"}, "quota": {"0"}}, testCache(), gofiler.Opts{Credentials: calendarCredentials(t, endpoint)})
	assert.Error(t, err, "a sign-in which can only read calendars can't write events")

	writer := newCredentials(t, endpoint, func(g *authclient.Grant) { g.Scopes = []string{httpclient.CalendarScope} })
	filer := newCalendarFiler(t, writer, url.Values{"readonly": {"false"}})
	assert.False(t, filer.Readonly())
	actions := filer.Actions()
	require.Len(t, actions, 1)
	assert.Equal(t, "event", actions[0].Name())
	run := func(input string) (*schema.Object, error) {
		return actions[0].Run(ctx, json.RawMessage(input))
	}
	event := func(calendar, id string) httpclient.Event {
		fake.Lock()
		defer fake.Unlock()
		for _, e := range fake.events[calendar] {
			if e.Id == id {
				return e.Event
			}
		}
		t.Fatalf("no event %q", id)
		return httpclient.Event{}
	}
	pathOf := func(id string) string {
		for _, p := range listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}) {
			if strings.HasSuffix(p, "-"+id+".ics") {
				return p
			}
		}
		t.Fatalf("no path of %q", id)
		return ""
	}

	// An event is created in the primary calendar, of its zone, which is an
	// hour long, and one of a day, and one in another calendar
	created, err := run(`{"op":"create","title":" Lunch ","start":"2026-11-25T12:30","location":"Canteen","attendees":["ada@example.com"]}`)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(created.Path, "/2026/2026-11/25-"), created.Path)
	lunch := event(work, "n7")
	assert.Equal(t, "Lunch", lunch.Summary)
	assert.Equal(t, httpclient.EventTime{DateTime: "2026-11-25T12:30:00+01:00", TimeZone: "Europe/Berlin"}, lunch.Start)
	assert.Equal(t, httpclient.EventTime{DateTime: "2026-11-25T13:30:00+01:00", TimeZone: "Europe/Berlin"}, lunch.End)
	assert.Equal(t, []httpclient.EventPerson{{Email: "ada@example.com"}}, lunch.Attendees)
	_, err = run(`{"op":"create","title":"Away","start":"2026-11-27"}`)
	require.NoError(t, err)
	assert.Equal(t, httpclient.EventTime{Date: "2026-11-28"}, event(work, "n8").End, "a day long")
	_, err = run(`{"op":"create","calendar":"` + holiday + `","title":"Bank holiday","start":"2026-12-28","end":"2026-12-29"}`)
	require.NoError(t, err)
	assert.Equal(t, "Bank holiday", event(holiday, "n2").Summary)

	// It's updated, of the fields which are given, and a start alone keeps
	// how long it is
	_, err = run(`{"op":"update","path":"` + pathOf("e1") + `","title":"Stand-up"}`)
	require.NoError(t, err)
	e1 := event(work, "e1")
	assert.Equal(t, "Stand-up", e1.Summary)
	assert.Equal(t, "2026-11-20T09:00:00+01:00", e1.Start.DateTime, "its start is kept")
	_, err = run(`{"op":"update","path":"` + pathOf("e1") + `","start":"2026-11-20T11:00:00+01:00"}`)
	require.NoError(t, err)
	e1 = event(work, "e1")
	assert.Equal(t, "2026-11-20T11:00:00+01:00", e1.Start.DateTime)
	assert.Equal(t, "2026-11-20T12:00:00+01:00", e1.End.DateTime, "it's an hour long, as it was")

	// One time an event which repeats occurs is updated, alone
	_, err = run(`{"op":"update","path":"` + pathOf("rec_20261105T100000Z") + `","title":"Moved"}`)
	require.NoError(t, err)
	assert.Equal(t, "Moved", event(work, "rec_20261105T100000Z").Summary)

	// And deleted
	path := pathOf("e2")
	deleted, err := run(`{"op":"delete","path":"` + path + `"}`)
	require.NoError(t, err)
	assert.Equal(t, path, deleted.Path)
	assert.Equal(t, "cancelled", event(work, "e2").Status)

	// Input which isn't valid for its op
	e1Path := pathOf("e1")
	for _, input := range []string{
		`{"op":"move","path":"` + e1Path + `"}`,
		`{"op":"create","start":"2026-11-25"}`,
		`{"op":"create","title":"x"}`,
		`{"op":"create","title":"x","start":"2026-11-25","path":"` + e1Path + `"}`,
		`{"op":"create","title":"x","start":"2026-11-25","calendar":"nowhere"}`,
		`{"op":"create","title":"x","start":"soon"}`,
		`{"op":"create","title":"x","start":"2026-11-25","end":"2026-11-24"}`,
		`{"op":"create","title":"x","start":"2026-11-25","end":"2026-11-25T10:00:00Z"}`,
		`{"op":"create","title":"x","start":"2026-11-25T10:00","timezone":"Mars/Olympus"}`,
		`{"op":"update","title":"x"}`,
		`{"op":"update","path":"` + e1Path + `"}`,
		`{"op":"update","path":"` + e1Path + `","calendar":"` + work + `","title":"x"}`,
		`{"op":"update","path":"` + e1Path + `","timezone":"Europe/Paris"}`,
		`{"op":"update","path":"` + e1Path + `","start":"2026-11-21"}`,
		`{"op":"update","path":"/2026/","title":"x"}`,
		`{"op":"delete","path":"` + e1Path + `","title":"x"}`,
		`{"op":"delete","path":"` + e1Path + `"} garbage`,
	} {
		_, err := run(input)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, input)
	}
	_, err = run(`{"op":"update","path":"` + strings.Replace(e1Path, "-e1.ics", "-zz.ics", 1) + `","title":"x"}`)
	assert.ErrorIs(t, err, fs.ErrNotExist)

	// An event which repeats, rather than a time it occurs, isn't listed,
	// so isn't changed, as that would change each time it occurs
	fake.put(work, httpclient.Event{Id: "series", ETag: `"series-1"`, Status: "confirmed", Recurrence: []string{"RRULE:FREQ=WEEKLY"}, Start: httpclient.EventTime{DateTime: "2026-11-02T09:00:00Z"}, End: httpclient.EventTime{DateTime: "2026-11-02T10:00:00Z"}})
	for _, input := range []string{
		`{"op":"update","path":"/2026/2026-11/02-` + work + `-series.ics","title":"x"}`,
		`{"op":"delete","path":"/2026/2026-11/02-` + work + `-series.ics"}`,
	} {
		_, err = run(input)
		assert.ErrorIs(t, err, fs.ErrNotExist, input)
	}
	assert.Empty(t, event(work, "series").Summary, "it isn't changed")

	// Its end alone is written, rather than its start, which isn't changed
	_, err = run(`{"op":"update","path":"` + e1Path + `","end":"2026-11-20T12:30:00+01:00"}`)
	require.NoError(t, err)
	fake.Lock()
	assert.NotContains(t, fake.fields, "start")
	assert.Contains(t, fake.fields, "end")
	fake.Unlock()
	assert.Equal(t, "2026-11-20T12:30:00+01:00", event(work, "e1").End.DateTime)

	// A time which becomes a day clears its time, and zone, as a patch keeps
	// what isn't written, and a day which becomes a time clears its day
	_, err = run(`{"op":"update","path":"` + e1Path + `","start":"2026-11-21","end":"2026-11-22"}`)
	require.NoError(t, err)
	fake.Lock()
	assert.JSONEq(t, `{"date":"2026-11-21","dateTime":null,"timeZone":null}`, string(fake.fields["start"]))
	assert.JSONEq(t, `{"date":"2026-11-22","dateTime":null,"timeZone":null}`, string(fake.fields["end"]))
	fake.Unlock()
	_, err = run(`{"op":"update","path":"` + pathOf("e1") + `","start":"2026-11-21T09:00","end":"2026-11-21T10:00"}`)
	require.NoError(t, err)
	fake.Lock()
	assert.JSONEq(t, `{"date":null,"dateTime":"2026-11-21T09:00:00+01:00","timeZone":"Europe/Berlin"}`, string(fake.fields["start"]))
	fake.Unlock()

	// An event outside the days which are listed is created, and can be
	// changed, and deleted, of its path, though it isn't listed
	far, err := run(`{"op":"create","title":"Far away","start":"2027-06-01"}`)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(far.Path, "/2027/2027-06/01-"), far.Path)
	_, err = run(`{"op":"update","path":"` + far.Path + `","title":"Further"}`)
	require.NoError(t, err)
	_, err = run(`{"op":"delete","path":"` + far.Path + `"}`)
	require.NoError(t, err)
}

// Test_EventActionWatch tests what's written is sent as it's written, of a
// watch which doesn't sync until long after, and isn't sent again, when it's
// synced, of a watch which syncs often
func Test_EventActionWatch(t *testing.T) {
	for _, interval := range []string{"1h", "10ms"} {
		t.Run(interval, func(t *testing.T) { testEventActionWatch(t, interval) })
	}
}

func testEventActionWatch(t *testing.T, interval string) {
	_, endpoint := newFakeCalendar(t)
	writer := newCredentials(t, endpoint, func(g *authclient.Grant) { g.Scopes = []string{httpclient.CalendarScope} })
	filer := newCalendarFiler(t, writer, url.Values{"readonly": {"false"}, "refresh_interval": {interval}})
	run := func(input string) *schema.Object {
		t.Helper()
		object, err := filer.Actions()[0].Run(t.Context(), json.RawMessage(input))
		require.NoError(t, err)
		return object
	}
	ch, done := make(chan schema.Event, 16), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { done <- filer.WatchObjects(ctx, ch) }()
	defer func() {
		cancel()
		assert.ErrorIs(t, <-done, context.Canceled)
	}()
	events := func() []string {
		t.Helper()
		var result []string
		for {
			select {
			case event := <-ch:
				result = append(result, event.EventType.String()+" "+event.Path)
			case <-time.After(150 * time.Millisecond):
				return result
			}
		}
	}
	assert.Equal(t, []string{"rescan /"}, events())
	listed(t, filer, schema.ListObjectsRequest{Path: "/", Recursive: true})

	// What's written is sent as it's written, and not again when it's synced
	created := run(`{"op":"create","title":"Lunch","start":"2026-11-25T12:30"}`)
	assert.Equal(t, []string{"created " + created.Path}, events())
	run(`{"op":"update","path":"` + created.Path + `","title":"Late lunch"}`)
	assert.Equal(t, []string{"updated " + created.Path}, events())
	moved := run(`{"op":"update","path":"` + created.Path + `","start":"2026-11-26T12:30"}`)
	assert.Equal(t, []string{"deleted " + created.Path, "created " + moved.Path}, events())
	run(`{"op":"delete","path":"` + moved.Path + `"}`)
	assert.Equal(t, []string{"deleted " + moved.Path}, events())

	// Nor is an event outside the days which are listed
	run(`{"op":"create","title":"Far away","start":"2027-06-01"}`)
	assert.Empty(t, events())
}
