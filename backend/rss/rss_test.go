package rss_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	rss "github.com/mutablelogic/go-filer/backend/rss"
	schema "github.com/mutablelogic/go-filer/schema"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	types "github.com/mutablelogic/go-server/pkg/types"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

const feedXML = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel>
  <title>The Example Podcast</title>
  <link>https://example.com/</link>
  <language>en-gb</language>
  <copyright>© 2026 Example</copyright>
  <image><url>https://example.com/cover.jpg</url></image>
  <item>
    <title>Episode One: Hello, World!</title>
    <author>ada@example.com (Ada)</author>
    <link>https://example.com/1</link>
    <comments>https://example.com/1#comments</comments>
    <category>Science</category>
    <category>Space</category>
    <category>Science</category>
    <guid>ep-1</guid>
    <pubDate>Fri, 20 Nov 2026 09:00:00 GMT</pubDate>
    <description>&lt;p&gt;The first episode.&lt;/p&gt;</description>
    <enclosure url="https://example.com/1.mp3" length="1234" type="audio/mpeg"/>
    <itunes:duration>1:02:03</itunes:duration>
    <itunes:episode>1</itunes:episode>
    <itunes:episodeType>full</itunes:episodeType>
    <itunes:image href="https://example.com/1.jpg"/>
  </item>
  <item>
    <title>Episode One: Hello, World!</title>
    <guid>ep-1b</guid>
    <pubDate>Fri, 20 Nov 2026 18:00:00 +0000</pubDate>
  </item>
  <item>
    <title>Late</title>
    <guid>ep-2</guid>
    <pubDate>Mon, 30 Nov 2026 23:30:00 -0500</pubDate>
  </item>
  <item>
    <title>No Date</title>
    <guid>ep-3</guid>
  </item>
</channel>
</rss>`

// newFeed returns a server of the feed, and how many times it's fetched
func newFeed(t *testing.T, body string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var fetches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		if r.URL.Path == "/missing" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/rss+xml")
		io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	return server, &fetches
}

func open(t *testing.T, feed string) (gofiler.Filer, error) {
	t.Helper()
	return rss.Open(context.Background(), feedURL(t, feed, nil), testCache(), gofiler.Opts{})
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
	return &url.URL{Scheme: rss.Scheme, Host: "podcast", Path: "/" + u.Host + u.Path, RawQuery: query.Encode()}
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
		assert.Equal(t, "podcast", object.Name)
		result = append(result, object.Path)
	}
}

func Test_Feed(t *testing.T) {
	server, fetches := newFeed(t, feedXML)
	f, err := open(t, server.URL+"/feed?format=rss")
	require.NoError(t, err)

	// Its URL has its options, and a description of its title
	u := f.URL()
	assert.Equal(t, "rss", u.Scheme)
	assert.Equal(t, "podcast", u.Host)
	assert.Equal(t, "/"+strings.TrimPrefix(server.URL, "http://")+"/feed", u.Path)
	assert.Equal(t, "rss", u.Query().Get("format"), "the feed's own parameters are kept")
	assert.Equal(t, "true", u.Query().Get("insecure"))
	assert.False(t, u.Query().Has("refresh_interval"), "it isn't set")
	assert.Equal(t, "RSS feed The Example Podcast", u.Query().Get("description"))
	assert.Equal(t, "true", u.Query().Get("readonly"))

	// The items are of their days, in UTC, of the folders of their years
	// and months, and those of the same title, and day, are numbered
	assert.Equal(t, []string{"/2026/", "/undated/"}, list(t, f, "/", false))
	assert.Equal(t, []string{
		"/2026/", "/2026/2026-11/",
		"/2026/2026-11/20-episode-one-hello-world-2.html",
		"/2026/2026-11/20-episode-one-hello-world.html",
		"/2026/2026-12/", "/2026/2026-12/01-late.html",
		"/undated/", "/undated/no-date.html",
	}, list(t, f, "/", true))
	assert.Equal(t, []string{"/2026/2026-12/01-late.html"}, list(t, f, "/2026/2026-12", false))
	assert.Equal(t, []string{"/undated/no-date.html"}, list(t, f, "/undated/no-date.html", false))
	assert.Equal(t, int32(1), fetches.Load(), "it's fetched once, within its refresh interval")

	// An item has its metadata, and its page
	read, object, err := f.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/2026/2026-11/20-episode-one-hello-world.html"}})
	require.NoError(t, err)
	assert.Equal(t, "text/html; charset=utf-8", object.ContentType)
	assert.NotNil(t, object.ETag)
	assert.Equal(t, "2026-11-20T09:00:00Z", object.ModTime.Format("2006-01-02T15:04:05Z07:00"))
	meta := make(map[string]string)
	for _, m := range object.Meta {
		meta[m.Namespace+":"+m.Key] = string(m.Value)
	}
	assert.Equal(t, `"Episode One: Hello, World!"`, meta["dc:title"])
	assert.Equal(t, `"ada@example.com (Ada)"`, meta["dc:creator"])
	assert.Equal(t, `"ep-1"`, meta["dc:identifier"], "it's the guid")
	assert.Equal(t, `"https://example.com/1"`, meta["rss:url"])
	assert.Equal(t, `["Science","Space"]`, meta["dc:subject"])
	assert.Equal(t, `"en-gb"`, meta["dc:language"])
	assert.Equal(t, `"© 2026 Example"`, meta["dc:rights"])
	assert.Equal(t, `"https://example.com/1#comments"`, meta["rss:comments"])
	assert.Equal(t, `"https://example.com/1.jpg"`, meta["rss:image"])
	assert.Equal(t, `"full"`, meta["rss:episode_type"])
	assert.NotContains(t, meta, "rss:guid")
	assert.Equal(t, `"The Example Podcast"`, meta["rss:feed"])
	assert.Equal(t, `"1"`, meta["rss:episode"])
	assert.Equal(t, `3723`, meta["rss:duration"])
	var enclosure map[string]any
	require.NoError(t, json.Unmarshal([]byte(meta["rss:enclosure"]), &enclosure))
	assert.Equal(t, map[string]any{"url": "https://example.com/1.mp3", "type": "audio/mpeg", "length": float64(1234)}, enclosure)
	var page string
	require.NoError(t, read(context.Background(), func(r io.Reader) error {
		data, err := io.ReadAll(r)
		page = string(data)
		return err
	}))
	assert.Contains(t, page, "<title>Episode One: Hello, World!</title>")
	assert.Contains(t, page, "<p>The first episode.</p>")
	assert.Contains(t, page, `href="https://example.com/1.mp3"`)
	assert.Equal(t, int64(len(page)), *object.Size)

	// A folder is got, but not read, a path which isn't there isn't found, and
	// one which isn't modified isn't returned
	folder, err := f.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/2026"}})
	require.NoError(t, err)
	assert.True(t, folder.IsDir)
	_, _, err = f.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/2026/"}})
	assert.ErrorIs(t, err, fs.ErrInvalid)
	_, err = f.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/2026/missing.html"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)
	_, err = f.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: object.Path}, IfNoneMatch: []string{*object.ETag}})
	assert.ErrorIs(t, err, gofiler.ErrNotModified)
	_, err = f.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: object.Path}, IfNoneMatch: []string{"*"}})
	assert.ErrorIs(t, err, gofiler.ErrNotModified, "of any entity tag")
	_, _, err = f.ReadObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: object.Path}, IfModifiedSince: object.ModTime})
	assert.ErrorIs(t, err, gofiler.ErrNotModified, "since it was modified")
	_, err = f.GetObject(context.Background(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: object.Path}, IfModifiedSince: object.ModTime.Add(-time.Hour)})
	assert.NoError(t, err, "it was modified since")

	// It's read-only
	_, err = f.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.html"}, Body: strings.NewReader("a")})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	_, err = f.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: object.Path}})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)

	// A clone has its options, changed
	clone, err := f.Clone(context.Background(), url.Values{"refresh_interval": {"1m"}})
	require.NoError(t, err)
	assert.Equal(t, "1m0s", clone.URL().Query().Get("refresh_interval"))
	assert.Equal(t, "RSS feed The Example Podcast", clone.URL().Query().Get("description"))
}

func Test_FeedErrors(t *testing.T) {
	server, _ := newFeed(t, feedXML)
	notFeed, _ := newFeed(t, "<html><body>not a feed</body></html>")

	// Options which aren't valid, a feed which isn't found, or isn't a feed,
	// are bad parameters, and one which can't be reached is unavailable
	for _, params := range []url.Values{
		{"refresh_interval": {"soon"}},
		{"readonly": {"false"}},
		{"insecure": {"maybe"}},
		{"name": {"not a name"}},
	} {
		_, err := rss.Open(context.Background(), feedURL(t, server.URL, params), testCache(), gofiler.Opts{})
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, params)
	}
	_, err := rss.Open(context.Background(), &url.URL{Scheme: rss.Scheme, Host: "podcast"}, testCache(), gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter, "it has no host")
	_, err = open(t, server.URL+"/missing")
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = open(t, notFeed.URL)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()
	_, err = open(t, down.URL)
	assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
}

// A feed which is watched is polled, of its refresh interval, and isn't
// fetched again when it isn't modified, of its entity tag, and the changes
// to its items, of their guids, are sent
func Test_FeedWatch(t *testing.T) {
	var body atomic.Pointer[string]
	var full atomic.Int32
	body.Store(types.Ptr(feedXML))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := *body.Load()
		sum := sha256.Sum256([]byte(b))
		etag := `"` + hex.EncodeToString(sum[:8]) + `"`
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		full.Add(1)
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/rss+xml")
		io.WriteString(w, b)
	}))
	defer server.Close()
	f, err := rss.Open(context.Background(), feedURL(t, server.URL, url.Values{"refresh_interval": {"10ms"}}), testCache(), gofiler.Opts{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan schema.Event, 64)
	done := make(chan error, 1)
	go func() { done <- f.WatchObjects(ctx, ch) }()
	select {
	case event := <-ch:
		assert.Equal(t, schema.Rescan("podcast"), event, "a rescan of the root, first")
	case <-time.After(5 * time.Second):
		t.Fatal("no rescan")
	}

	// It isn't fetched again, while it isn't modified, and nothing is sent
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), full.Load(), "it's fetched once, and then isn't modified")
	assert.Empty(t, ch)

	// One item's description changes, one is renamed, one is gone, and
	// another is new, in a month which wasn't there
	changed := strings.NewReplacer(
		"The first episode.", "The first episode, again.",
		"<title>Late</title>", "<title>Later</title>",
		"<item>\n    <title>No Date</title>\n    <guid>ep-3</guid>\n  </item>", "<item>\n    <title>New</title>\n    <guid>ep-4</guid>\n    <pubDate>Tue, 05 Jan 2027 12:00:00 GMT</pubDate>\n  </item>",
	).Replace(feedXML)
	require.NotEqual(t, feedXML, changed)
	body.Store(&changed)
	want := []string{
		"created /2026/2026-12/01-later.html",
		"created /2027/",
		"created /2027/2027-01/",
		"created /2027/2027-01/05-new.html",
		"deleted /2026/2026-12/01-late.html",
		"deleted /undated/",
		"deleted /undated/no-date.html",
		"updated /2026/2026-11/20-episode-one-hello-world.html",
	}
	var got []string
	for len(got) < len(want) {
		select {
		case event := <-ch:
			assert.Equal(t, "podcast", event.Name)
			got = append(got, event.EventType.String()+" "+event.Path)
		case <-time.After(5 * time.Second):
			t.Fatalf("got %v", got)
		}
	}
	slices.Sort(got)
	assert.Equal(t, want, got)

	// And the listing is of the feed which changed
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
	assert.Contains(t, list(t, f, "/", true), "/2027/2027-01/05-new.html")
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

// Test_FeedRestart tests a feed which was fetched is saved in the volume's
// cache, so a filer which is created again, such as after a restart, lists
// it without fetching it, until it would be fetched again, when it's
// fetched of its entity tag, which isn't modified
func Test_FeedRestart(t *testing.T) {
	var full, notModified atomic.Int32
	sum := sha256.Sum256([]byte(feedXML))
	etag := `"` + hex.EncodeToString(sum[:8]) + `"`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == etag {
			notModified.Add(1)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		full.Add(1)
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/rss+xml")
		io.WriteString(w, feedXML)
	}))
	defer server.Close()
	cache := testCache()
	paths := func(f gofiler.Filer) []string {
		t.Helper()
		iter, err := f.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true})
		require.NoError(t, err)
		var result []string
		for {
			object, err := iter.Next(context.Background())
			if err == io.EOF {
				return result
			}
			require.NoError(t, err)
			result = append(result, object.Path)
		}
	}

	// It's fetched, and saved
	first, err := rss.Open(context.Background(), feedURL(t, server.URL, url.Values{"refresh_interval": {"1h"}}), cache, gofiler.Opts{})
	require.NoError(t, err)
	want := paths(first)
	require.NotEmpty(t, want)
	assert.Equal(t, int32(1), full.Load())

	// Another, of the same cache, lists it, without fetching it
	second, err := rss.Open(context.Background(), feedURL(t, server.URL, url.Values{"refresh_interval": {"1h"}}), cache, gofiler.Opts{})
	require.NoError(t, err)
	assert.Equal(t, want, paths(second))
	assert.Equal(t, int32(1), full.Load(), "it isn't fetched")

	// And, once it would be fetched again, it's fetched of its entity tag,
	// which isn't modified
	third, err := rss.Open(context.Background(), feedURL(t, server.URL, url.Values{"refresh_interval": {"10ms"}}), cache, gofiler.Opts{})
	require.NoError(t, err)
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, want, paths(third))
	assert.Equal(t, int32(1), full.Load())
	assert.GreaterOrEqual(t, notModified.Load(), int32(1), "it's fetched of its entity tag")
}

// Test_FeedRestartChecked tests a feed which was found not to be modified,
// isn't fetched again after a restart, until it would have been, of when it
// was checked, rather than when it was saved
func Test_FeedRestartChecked(t *testing.T) {
	var requests atomic.Int32
	sum := sha256.Sum256([]byte(feedXML))
	etag := `"` + hex.EncodeToString(sum[:8]) + `"`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/rss+xml")
		io.WriteString(w, feedXML)
	}))
	defer server.Close()
	cache := testCache()
	open := func(interval string) gofiler.Filer {
		t.Helper()
		f, err := rss.Open(context.Background(), feedURL(t, server.URL, url.Values{"refresh_interval": {interval}}), cache, gofiler.Opts{})
		require.NoError(t, err)
		return f
	}

	// It's fetched, and then, after its interval, isn't modified
	open("50ms")
	time.Sleep(60 * time.Millisecond)
	open("50ms")
	assert.Equal(t, int32(2), requests.Load())

	// So one which is created at once isn't fetched, as it was checked now
	open("1h")
	assert.Equal(t, int32(2), requests.Load())
}

// Test_FeedEmpty tests an empty response, which can't be read, doesn't
// replace the feed which was saved
func Test_FeedEmpty(t *testing.T) {
	var empty atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		if !empty.Load() {
			io.WriteString(w, feedXML)
		}
	}))
	defer server.Close()
	cache := testCache()
	_, err := rss.Open(context.Background(), feedURL(t, server.URL, url.Values{"refresh_interval": {"1h"}}), cache, gofiler.Opts{})
	require.NoError(t, err)
	empty.Store(true)
	_, err = rss.Open(context.Background(), feedURL(t, server.URL, url.Values{"refresh_interval": {"1ms"}}), cache, gofiler.Opts{})
	time.Sleep(5 * time.Millisecond)
	f, err := rss.Open(context.Background(), feedURL(t, server.URL, url.Values{"refresh_interval": {"1h"}}), cache, gofiler.Opts{})
	require.NoError(t, err, "the feed which was saved is read")
	iter, err := f.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true})
	require.NoError(t, err)
	object, err := iter.Next(context.Background())
	require.NoError(t, err)
	assert.NotEmpty(t, object.Path)
}

// Test_FeedWatchRestart tests what changed while it wasn't watched is
// sent, after the rescan, of the items which were saved
func Test_FeedWatchRestart(t *testing.T) {
	var body atomic.Pointer[string]
	body.Store(types.Ptr(feedXML))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml")
		io.WriteString(w, *body.Load())
	}))
	defer server.Close()
	cache := testCache()
	_, err := rss.Open(context.Background(), feedURL(t, server.URL, url.Values{"refresh_interval": {"1h"}}), cache, gofiler.Opts{})
	require.NoError(t, err)

	// A new item, while it isn't watched, of a filer which fetches it as
	// it's created, is sent, after the rescan
	body.Store(types.Ptr(strings.Replace(feedXML, "<title>Late</title>", "<title>Later</title>", 1)))
	f, err := rss.Open(context.Background(), feedURL(t, server.URL, url.Values{"refresh_interval": {"1ms"}}), cache, gofiler.Opts{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan schema.Event, 64)
	go func() { _ = f.WatchObjects(ctx, ch) }()
	var got []string
	timeout := time.After(5 * time.Second)
	for len(got) < 3 {
		select {
		case event := <-ch:
			got = append(got, event.EventType.String())
		case <-timeout:
			t.Fatalf("got %v", got)
		}
	}
	assert.Equal(t, "rescan", got[0])
	assert.Contains(t, got[1:], "deleted")
	assert.Contains(t, got[1:], "created")
}
