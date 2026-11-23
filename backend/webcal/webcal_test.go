package webcal_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	webcal "github.com/mutablelogic/go-filer/backend/webcal"
	schema "github.com/mutablelogic/go-filer/schema"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	ical "github.com/mutablelogic/go-metadata/pkg/ical"
	types "github.com/mutablelogic/go-server/pkg/types"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// calendarICS is a feed: a meeting which repeats, weekly, with a change to
// one of its times, and two holidays of the same title and day, and its
// DTSTAMPs are of when it's fetched, which @NOW@ is replaced with
const calendarICS = "\ufeffBEGIN:VCALENDAR\r\n" +
	"VERSION:2.0\r\n" +
	"PRODID:-//Example//Feed//EN\r\n" +
	"X-WR-CALNAME:Team\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:meeting@example.com\r\n" +
	"DTSTAMP:@NOW@\r\n" +
	"DTSTART;TZID=Europe/London:20261102T090000\r\n" +
	"DTEND;TZID=Europe/London:20261102T100000\r\n" +
	"RRULE:FREQ=WEEKLY;COUNT=4\r\n" +
	"SUMMARY:Team Meeting\r\n" +
	"LAST-MODIFIED:20261020T120000Z\r\n" +
	"END:VEVENT\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:meeting@example.com\r\n" +
	"DTSTAMP:@NOW@\r\n" +
	"RECURRENCE-ID;TZID=Europe/London:20261109T090000\r\n" +
	"DTSTART;TZID=Europe/London:20261109T140000\r\n" +
	"DTEND;TZID=Europe/London:20261109T150000\r\n" +
	"SUMMARY:Team Meeting (moved)\r\n" +
	"LAST-MODIFIED:20261101T120000Z\r\n" +
	"END:VEVENT\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:christmas@example.com\r\n" +
	"DTSTAMP:@NOW@\r\n" +
	"DTSTART;VALUE=DATE:20261225\r\n" +
	"DTEND;VALUE=DATE:20261226\r\n" +
	"SUMMARY:Holiday\r\n" +
	"END:VEVENT\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:boxing@example.com\r\n" +
	"DTSTAMP:@NOW@\r\n" +
	"DTSTART;VALUE=DATE:20261225\r\n" +
	"SUMMARY:Holiday\r\n" +
	"END:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

// stamped returns the feed, of when it's fetched
func stamped(body string) string {
	return strings.ReplaceAll(body, "@NOW@", time.Now().UTC().Format("20060102T150405.000000000Z")[:15]+"Z")
}

// newFeed returns a server of the feed, which is stamped, each time it's
// fetched, of its entity tag, which is of the feed without its stamps, and
// how many times it's fetched, and isn't modified
func newFeed(t *testing.T, body *atomic.Pointer[string]) (*httptest.Server, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	var full, notModified atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing" {
			http.NotFound(w, r)
			return
		}
		b := *body.Load()
		sum := sha256.Sum256([]byte(b))
		etag := `"` + hex.EncodeToString(sum[:8]) + `"`
		if r.Header.Get("If-None-Match") == etag {
			notModified.Add(1)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		full.Add(1)
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "text/calendar; charset=utf-8")
		io.WriteString(w, stamped(b))
	}))
	t.Cleanup(server.Close)
	return server, &full, &notModified
}

func feed(body string) *atomic.Pointer[string] {
	var p atomic.Pointer[string]
	p.Store(&body)
	return &p
}

// feedURL returns the volume's URL of a feed of a test server, which is
// http, so it's insecure, with its options
func feedURL(t *testing.T, feed string, params url.Values) *url.URL {
	t.Helper()
	u, err := url.Parse(feed)
	require.NoError(t, err)
	query := u.Query()
	for key, values := range params {
		query[key] = values
	}
	if !query.Has("insecure") {
		query.Set("insecure", "true")
	}
	return &url.URL{Scheme: webcal.Scheme, Host: "team", Path: "/" + u.Host + u.Path, RawQuery: query.Encode()}
}

func open(t *testing.T, u *url.URL, cache gofiler.CacheStore) (gofiler.Filer, error) {
	t.Helper()
	return webcal.Open(context.Background(), u, cache, gofiler.Opts{})
}

func list(t *testing.T, f gofiler.Filer, path string, recursive bool) []string {
	t.Helper()
	it, err := f.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: path}, Recursive: recursive})
	require.NoError(t, err)
	defer it.Close()
	var result []string
	for {
		object, err := it.Next(context.Background())
		if err == io.EOF {
			return result
		}
		require.NoError(t, err)
		assert.Equal(t, "team", object.Name)
		result = append(result, object.Path)
	}
}

func read(t *testing.T, f gofiler.Filer, path string) (*schema.Object, string) {
	t.Helper()
	fn, object, err := f.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: path}})
	require.NoError(t, err)
	var content string
	require.NoError(t, fn(context.Background(), func(r io.Reader) error {
		data, err := io.ReadAll(r)
		content = string(data)
		return err
	}))
	return object, content
}

// testCache returns a cache of a volume, in memory, as the registry passes
// a volume's backend its own
func testCache() gofiler.CacheStore {
	cache, err := sqlite.NewCacheStore(context.Background(), gosqlite.Memory)
	if err != nil {
		panic(err)
	}
	return cache
}

func Test_Backend(t *testing.T) {
	b := webcal.Backend()
	assert.Equal(t, "webcal", b.Scheme())
	assert.Equal(t, "Calendar subscription", b.Description())
	if assert.NotNil(t, b.Icon()) {
		assert.Equal(t, "image/svg+xml", b.Icon().Type())
	}
	assert.Equal(t, "true", b.Defaults().Get("readonly"))
	assert.Equal(t, "5m0s", b.Defaults().Get("refresh_interval"))
}

func Test_FeedURL(t *testing.T) {
	// A feed is http, https, or webcal, which is fetched as https
	for in, want := range map[string]string{
		"webcal://example.com/cal.ics?key=1": "https://example.com/cal.ics?key=1",
		"https://example.com/cal.ics":        "https://example.com/cal.ics",
		"http://example.com/cal.ics":         "http://example.com/cal.ics",
	} {
		u, err := webcal.FeedURL(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, u.String())
	}
	for _, in := range []string{"ftp://example.com/cal.ics", "example.com/cal.ics", "webcal:///cal.ics"} {
		_, err := webcal.FeedURL(in)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, in)
	}
}

func Test_Feed(t *testing.T) {
	server, full, _ := newFeed(t, feed(calendarICS))
	f, err := open(t, feedURL(t, server.URL+"/cal.ics?key=secret", nil), testCache())
	require.NoError(t, err)

	// Its URL has its options, and a description of the calendar's name
	u := f.URL()
	assert.Equal(t, "webcal", u.Scheme)
	assert.Equal(t, "team", u.Host)
	assert.Equal(t, "/"+strings.TrimPrefix(server.URL, "http://")+"/cal.ics", u.Path)
	assert.Equal(t, "secret", u.Query().Get("key"), "the feed's own parameters are kept")
	assert.Equal(t, "true", u.Query().Get("insecure"))
	assert.Equal(t, "5m0s", u.Query().Get("refresh_interval"))
	assert.Equal(t, "Calendar Team", u.Query().Get("description"))
	assert.Equal(t, "Calendar Team", f.Description())

	// The events are of their days, in their zones, and a repeating event
	// is one file, of when it first starts, and those of the same title,
	// and day, are numbered, of their UIDs
	assert.Equal(t, []string{
		"/2026/", "/2026/2026-11/", "/2026/2026-11/02-team-meeting.ics",
		"/2026/2026-12/", "/2026/2026-12/25-holiday-2.ics", "/2026/2026-12/25-holiday.ics",
	}, list(t, f, "/", true))
	assert.Equal(t, []string{"/2026/"}, list(t, f, "/", false))
	assert.Equal(t, int32(1), full.Load(), "it's fetched once, within its refresh interval")

	// An event is its iCalendar, with its change to one of its times
	object, content := read(t, f, "/2026/2026-11/02-team-meeting.ics")
	assert.Equal(t, "text/calendar; component=VEVENT", object.ContentType)
	assert.Equal(t, int64(len(content)), types.Value(object.Size))
	assert.Equal(t, "2026-11-01T12:00:00Z", object.ModTime.UTC().Format(time.RFC3339), "when it, or a change, was last modified")
	cal, err := ical.Decode(strings.NewReader(content))
	require.NoError(t, err)
	assert.Equal(t, "Team", cal.Name)
	require.Len(t, cal.Events, 2)
	assert.True(t, cal.Events[0].RecurrenceID.IsZero(), "the event, first")
	assert.Equal(t, "FREQ=WEEKLY;COUNT=4", cal.Events[0].RRule)
	assert.Equal(t, "Team Meeting (moved)", cal.Events[1].Summary)
	meta := make(map[string]string)
	for _, m := range object.Meta {
		meta[m.Namespace+":"+m.Key] = string(m.Value)
	}
	assert.Equal(t, `"Team Meeting"`, meta["dc:title"])
	assert.Equal(t, `"meeting@example.com"`, meta["dc:identifier"])
	assert.Equal(t, `"Team"`, meta["webcal:calendar"])

	// An event which wasn't modified, or created, has the feed's DTSTAMP
	_, content = read(t, f, "/2026/2026-12/25-holiday.ics")
	assert.Contains(t, content, "DTSTAMP:"+time.Now().UTC().Format("20060102"))

	// A folder is got, but not read, a path which isn't there isn't found,
	// and one which isn't modified isn't returned
	folder, err := f.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/2026"}})
	require.NoError(t, err)
	assert.True(t, folder.IsDir)
	_, _, err = f.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/2026/"}})
	assert.ErrorIs(t, err, fs.ErrInvalid)
	_, err = f.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/2026/missing.ics"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)
	_, err = f.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: object.Path}, IfNoneMatch: []string{*object.ETag}})
	assert.ErrorIs(t, err, gofiler.ErrNotModified)

	// It's read-only
	_, err = f.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.ics"}, Body: strings.NewReader("a")})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	_, err = f.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: object.Path}})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)

	// A clone has its options, changed
	clone, err := f.Clone(context.Background(), url.Values{"refresh_interval": {"1m"}})
	require.NoError(t, err)
	assert.Equal(t, "1m0s", clone.URL().Query().Get("refresh_interval"))
	assert.Equal(t, "Calendar Team", clone.URL().Query().Get("description"))
}

func Test_FeedErrors(t *testing.T) {
	server, _, _ := newFeed(t, feed(calendarICS))
	page, _, _ := newFeed(t, feed("<html><body>Sign in</body></html>"))

	// Options which aren't valid, a feed which isn't found, or isn't a
	// calendar, are bad parameters, and one which can't be reached is
	// unavailable
	for _, params := range []url.Values{
		{"refresh_interval": {"soon"}},
		{"readonly": {"false"}},
		{"insecure": {"maybe"}},
		{"name": {"not a name"}},
	} {
		_, err := open(t, feedURL(t, server.URL, params), testCache())
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, params)
	}
	_, err := open(t, &url.URL{Scheme: webcal.Scheme, Host: "team"}, testCache())
	assert.ErrorIs(t, err, gofiler.ErrBadParameter, "it has no host")
	_, err = open(t, feedURL(t, server.URL+"/missing", nil), testCache())
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = open(t, feedURL(t, page.URL, nil), testCache())
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()
	_, err = open(t, feedURL(t, down.URL, nil), testCache())
	assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
}

// A feed which is watched is polled, of its refresh interval, and isn't
// fetched again when it isn't modified, of its entity tag, and the changes
// to its events, of their UIDs, are sent
func Test_FeedWatch(t *testing.T) {
	body := feed(calendarICS)
	server, full, _ := newFeed(t, body)
	f, err := open(t, feedURL(t, server.URL, url.Values{"refresh_interval": {"10ms"}}), testCache())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan schema.Event, 64)
	done := make(chan error, 1)
	go func() { done <- f.WatchObjects(ctx, ch) }()
	select {
	case event := <-ch:
		assert.Equal(t, schema.Rescan("team"), event, "a rescan of the root, first")
	case <-time.After(5 * time.Second):
		t.Fatal("no rescan")
	}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), full.Load(), "it's fetched once, and then isn't modified")
	assert.Empty(t, ch)

	// The feed is fetched again, of its stamps only, and nothing changes,
	// as an event's DTSTAMP is when it was modified
	restamped := calendarICS + "\r\n"
	body.Store(&restamped)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(2), full.Load())
	assert.Empty(t, ch, "the stamps aren't a change")

	// The meeting's change is gone, one holiday is renamed, and the other is
	// gone, and an event is new, in a month which wasn't there
	changed := strings.NewReplacer(
		"SUMMARY:Team Meeting (moved)\r\n", "SUMMARY:Team Meeting (moved again)\r\n",
		"UID:christmas@example.com\r\nDTSTAMP:@NOW@\r\nDTSTART;VALUE=DATE:20261225\r\nDTEND;VALUE=DATE:20261226\r\nSUMMARY:Holiday", "UID:christmas@example.com\r\nDTSTAMP:@NOW@\r\nDTSTART;VALUE=DATE:20261225\r\nDTEND;VALUE=DATE:20261226\r\nSUMMARY:Christmas",
		"UID:boxing@example.com\r\nDTSTAMP:@NOW@\r\nDTSTART;VALUE=DATE:20261225", "UID:newyear@example.com\r\nDTSTAMP:@NOW@\r\nDTSTART;VALUE=DATE:20270101",
	).Replace(calendarICS)
	body.Store(&changed)
	want := []string{
		"created /2026/2026-12/25-christmas.ics",
		"created /2027/",
		"created /2027/2027-01/",
		"created /2027/2027-01/01-holiday.ics",
		"deleted /2026/2026-12/25-holiday-2.ics",
		"deleted /2026/2026-12/25-holiday.ics",
		"updated /2026/2026-11/02-team-meeting.ics",
	}
	var got []string
	for len(got) < len(want) {
		select {
		case event := <-ch:
			assert.Equal(t, "team", event.Name)
			got = append(got, event.EventType.String()+" "+event.Path)
		case <-time.After(5 * time.Second):
			t.Fatalf("got %v", got)
		}
	}
	slices.Sort(got)
	assert.Equal(t, want, got)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

// Test_FeedRestart tests a feed which was fetched is saved in the volume's
// cache, so a filer which is created again lists it without fetching it,
// until it would be fetched again, when it isn't modified
func Test_FeedRestart(t *testing.T) {
	server, full, notModified := newFeed(t, feed(calendarICS))
	cache := testCache()
	first, err := open(t, feedURL(t, server.URL, url.Values{"refresh_interval": {"1h"}}), cache)
	require.NoError(t, err)
	want := list(t, first, "/", true)
	assert.Equal(t, int32(1), full.Load())

	second, err := open(t, feedURL(t, server.URL, url.Values{"refresh_interval": {"1h"}}), cache)
	require.NoError(t, err)
	assert.Equal(t, want, list(t, second, "/", true))
	assert.Equal(t, int32(1), full.Load(), "it isn't fetched")

	third, err := open(t, feedURL(t, server.URL, url.Values{"refresh_interval": {"10ms"}}), cache)
	require.NoError(t, err)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, want, list(t, third, "/", true))
	assert.Equal(t, int32(1), full.Load())
	assert.GreaterOrEqual(t, notModified.Load(), int32(1), "it's fetched of its entity tag")
}

// zonedICS is a feed with a time zone of its own, which an event is in, and
// another, which none is in, and an event with extra dates, and an alarm,
// which aren't decoded, so they're kept as they are
const zonedICS = "BEGIN:VCALENDAR\r\n" +
	"VERSION:2.0\r\n" +
	"PRODID:-//Example//Feed//EN\r\n" +
	"X-WR-CALNAME:Club\r\n" +
	"BEGIN:VTIMEZONE\r\n" +
	"TZID:Club Time\r\n" +
	"BEGIN:STANDARD\r\n" +
	"DTSTART:19700101T000000\r\n" +
	"TZOFFSETFROM:+0100\r\n" +
	"TZOFFSETTO:+0100\r\n" +
	"END:STANDARD\r\n" +
	"END:VTIMEZONE\r\n" +
	"BEGIN:VTIMEZONE\r\n" +
	"TZID:Unused\r\n" +
	"BEGIN:STANDARD\r\n" +
	"DTSTART:19700101T000000\r\n" +
	"TZOFFSETFROM:+0200\r\n" +
	"TZOFFSETTO:+0200\r\n" +
	"END:STANDARD\r\n" +
	"END:VTIMEZONE\r\n" +
	"BEGIN:VEVENT\r\n" +
	"UID:practice@example.com\r\n" +
	"DTSTAMP:@NOW@\r\n" +
	"DTSTART;TZID=\"Club Time\":20261105T180000\r\n" +
	"RDATE;TZID=\"Club Time\":20261112T180000\r\n" +
	"SUMMARY:Practice\r\n" +
	"DESCRIPTION:A long description which is folded\r\n" +
	"  over two lines\r\n" +
	"BEGIN:VALARM\r\n" +
	"ACTION:DISPLAY\r\n" +
	"TRIGGER:-PT15M\r\n" +
	"END:VALARM\r\n" +
	"END:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

// Test_FeedSource tests an event is its lines, as they are, with the time
// zones it's in, apart from its DTSTAMP, and a change to what isn't
// decoded, such as its extra dates, is a change of it
func Test_FeedSource(t *testing.T) {
	body := feed(zonedICS)
	server, _, _ := newFeed(t, body)
	f, err := open(t, feedURL(t, server.URL, url.Values{"refresh_interval": {"1ms"}}), testCache())
	require.NoError(t, err)
	assert.Equal(t, []string{"/2026/", "/2026/2026-11/", "/2026/2026-11/05-practice.ics"}, list(t, f, "/", true))
	object, content := read(t, f, "/2026/2026-11/05-practice.ics")
	for _, want := range []string{
		"X-WR-CALNAME:Club\r\n",
		"BEGIN:VTIMEZONE\r\nTZID:Club Time\r\n",
		"RDATE;TZID=\"Club Time\":20261112T180000\r\n",
		"DESCRIPTION:A long description which is folded\r\n  over two lines\r\n",
		"BEGIN:VALARM\r\nACTION:DISPLAY\r\nTRIGGER:-PT15M\r\nEND:VALARM\r\n",
	} {
		assert.Contains(t, content, want)
	}
	assert.NotContains(t, content, "TZID:Unused", "a time zone which it isn't in")
	assert.True(t, strings.HasPrefix(content, "BEGIN:VCALENDAR\r\n") && strings.HasSuffix(content, "END:VCALENDAR\r\n"))
	cal, err := ical.Decode(strings.NewReader(content))
	require.NoError(t, err)
	require.Len(t, cal.Events, 1)
	assert.Equal(t, "A long description which is folded over two lines", cal.Events[0].Description)

	// Its extra dates change, which is a change of its entity tag
	changed := strings.Replace(zonedICS, "20261112T180000", "20261119T180000", 1)
	body.Store(&changed)
	time.Sleep(5 * time.Millisecond)
	updated, err := f.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: object.Path}})
	require.NoError(t, err)
	assert.NotEqual(t, types.Value(object.ETag), types.Value(updated.ETag))
}

// Test_FeedTruncated tests a feed which is cut off, or whose components
// don't end as they began, isn't a calendar, and the feed which was saved
// is kept
func Test_FeedTruncated(t *testing.T) {
	body := feed(calendarICS)
	server, _, _ := newFeed(t, body)
	cache := testCache()
	_, err := open(t, feedURL(t, server.URL, url.Values{"refresh_interval": {"1h"}}), cache)
	require.NoError(t, err)
	for _, bad := range []string{
		"BEGIN:VCALENDAR\r\n",
		calendarICS[:strings.Index(calendarICS, "UID:boxing")],
		strings.TrimSuffix(calendarICS, "END:VCALENDAR\r\n"),
		strings.Replace(calendarICS, "END:VEVENT\r\nBEGIN:VEVENT\r\nUID:boxing", "END:VTODO\r\nBEGIN:VEVENT\r\nUID:boxing", 1),
		"BEGIN:VEVENT\r\nEND:VEVENT\r\n",
	} {
		body.Store(&bad)
		_, err := open(t, feedURL(t, server.URL, url.Values{"refresh_interval": {"1h"}}), testCache())
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, bad)
		_, err = open(t, feedURL(t, server.URL, url.Values{"refresh_interval": {"1ns"}}), cache)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, bad)
	}
	f, err := open(t, feedURL(t, server.URL, url.Values{"refresh_interval": {"1h"}}), cache)
	require.NoError(t, err, "the feed which was saved is read")
	assert.Contains(t, list(t, f, "/", true), "/2026/2026-12/25-holiday-2.ics")
}

// Test_FeedCache tests an event's lines are read from the feed, in the
// volume's cache, as its size, and entity tag, say, and a feed which isn't
// there, such as of a cache which was cleared, is fetched again
func Test_FeedCache(t *testing.T) {
	server, full, _ := newFeed(t, feed(calendarICS+zonedICS))
	cache := testCache()
	f, err := open(t, feedURL(t, server.URL, url.Values{"refresh_interval": {"1h"}}), cache)
	require.NoError(t, err)
	var files []string
	for _, p := range list(t, f, "/", true) {
		if strings.HasSuffix(p, ".ics") {
			files = append(files, p)
		}
	}
	require.Len(t, files, 4)
	for _, p := range files {
		object, content := read(t, f, p)
		assert.Equal(t, types.Value(object.Size), int64(len(content)), p)
		_, err := ical.Decode(strings.NewReader(content))
		assert.NoError(t, err, p)
	}
	assert.Equal(t, int32(1), full.Load())

	// The cache is cleared, so the feed is fetched again, when it's read
	_, before := read(t, f, files[0])
	require.NoError(t, cache.Delete(context.Background(), "feed/"))
	_, after := read(t, f, files[0])
	assert.Equal(t, int32(2), full.Load())
	assert.Equal(t, strings.Split(before, "DTSTAMP")[0], strings.Split(after, "DTSTAMP")[0])
}
