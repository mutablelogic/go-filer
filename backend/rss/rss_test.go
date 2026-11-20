package rss_test

import (
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	rss "github.com/mutablelogic/go-filer/backend/rss"
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

const feedXML = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
<channel>
  <title>The Example Podcast</title>
  <link>https://example.com/</link>
  <item>
    <title>Episode One: Hello, World!</title>
    <author>ada@example.com (Ada)</author>
    <link>https://example.com/1</link>
    <guid>ep-1</guid>
    <pubDate>Fri, 20 Nov 2026 09:00:00 GMT</pubDate>
    <description>&lt;p&gt;The first episode.&lt;/p&gt;</description>
    <enclosure url="https://example.com/1.mp3" length="1234" type="audio/mpeg"/>
    <itunes:duration>1:02:03</itunes:duration>
    <itunes:episode>1</itunes:episode>
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
	return rss.Open(context.Background(), &url.URL{Scheme: rss.Scheme, Host: "podcast", RawQuery: url.Values{"url": {feed}}.Encode()}, gofiler.Opts{})
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
	assert.Equal(t, server.URL+"/feed?format=rss", u.Query().Get("url"))
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
	assert.Equal(t, `"https://example.com/1"`, meta["rss:link"])
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

	// It's read-only, and isn't watched
	_, err = f.CreateObject(context.Background(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.html"}, Body: strings.NewReader("a")})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	_, err = f.DeleteObjects(context.Background(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: object.Path}})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	assert.ErrorIs(t, f.WatchObjects(context.Background(), make(chan schema.Event)), gofiler.ErrNotImplemented)

	// A clone has its options, changed
	clone, err := f.Clone(context.Background(), url.Values{"refresh-interval": {"1m"}})
	require.NoError(t, err)
	assert.Equal(t, "1m0s", clone.URL().Query().Get("refresh-interval"))
	assert.Equal(t, "RSS feed The Example Podcast", clone.URL().Query().Get("description"))
}

func Test_FeedErrors(t *testing.T) {
	server, _ := newFeed(t, feedXML)
	notFeed, _ := newFeed(t, "<html><body>not a feed</body></html>")

	// Options which aren't valid, a feed which isn't found, or isn't a feed,
	// are bad parameters, and one which can't be reached is unavailable
	for _, query := range []url.Values{
		{},
		{"url": {"ftp://example.com/feed"}},
		{"url": {server.URL}, "refresh-interval": {"soon"}},
		{"url": {server.URL}, "readonly": {"false"}},
		{"url": {server.URL}, "unknown": {"1"}},
	} {
		_, err := rss.Open(context.Background(), &url.URL{Scheme: rss.Scheme, Host: "podcast", RawQuery: query.Encode()}, gofiler.Opts{})
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, query)
	}
	_, err := rss.Open(context.Background(), &url.URL{Scheme: rss.Scheme, Host: "podcast", Path: "/a", RawQuery: "url=" + url.QueryEscape(server.URL)}, gofiler.Opts{})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = open(t, server.URL+"/missing")
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = open(t, notFeed.URL)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()
	_, err = open(t, down.URL)
	assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable)
}
