package rss

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	rss "github.com/mutablelogic/go-rss"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// The interval a feed is kept, and polled, is its refresh interval, but not
// less than its ttl, or its ttl, or five minutes, without either, and the
// refresh interval is only in its URL when it's set
func Test_Interval(t *testing.T) {
	serve := func(ttl string) string {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `<rss version="2.0"><channel><title>T</title>`+ttl+`<item><title>A</title><guid>a</guid></item></channel></rss>`)
		}))
		t.Cleanup(server.Close)
		return strings.TrimPrefix(server.URL, "http://")
	}
	open := func(host, refresh string) *Filer {
		t.Helper()
		query := url.Values{"insecure": {"true"}}
		if refresh != "" {
			query.Set("refresh_interval", refresh)
		}
		f, err := Open(context.Background(), &url.URL{Scheme: Scheme, Host: "feed", Path: "/" + host + "/", RawQuery: query.Encode()}, memoryCache(t), gofiler.Opts{})
		require.NoError(t, err)
		return f.(*Filer)
	}
	withTTL, withoutTTL := serve("<ttl>60</ttl>"), serve("")

	for _, test := range []struct {
		host, refresh string
		want          time.Duration
	}{
		{withTTL, "", time.Hour},
		{withTTL, "2h", 2 * time.Hour},
		{withTTL, "1m", time.Hour},
		{withoutTTL, "", DefaultRefresh},
		{withoutTTL, "1m", time.Minute},
	} {
		f := open(test.host, test.refresh)
		assert.Equal(t, test.want, f.interval(), "ttl %v, refresh %q", test.host == withTTL, test.refresh)
		assert.Equal(t, test.refresh != "", f.URL().Query().Has(paramRefresh))
	}
}

// Items of the same title, and day, are numbered, of the first number which
// isn't another item's path, such as one whose title ends with a number
func Test_ItemsNumbered(t *testing.T) {
	channel := mustChannel(t, `<rss version="2.0"><channel><title>T</title>
<item><title>Episode</title><guid>a</guid><pubDate>Fri, 20 Nov 2026 09:00:00 GMT</pubDate></item>
<item><title>Episode</title><guid>b</guid><pubDate>Fri, 20 Nov 2026 10:00:00 GMT</pubDate></item>
<item><title>Episode 2</title><guid>c</guid><pubDate>Fri, 20 Nov 2026 11:00:00 GMT</pubDate></item>
</channel></rss>`)
	items := newItems(channel)
	assert.Len(t, items.keys, 3)
	paths := map[string]bool{}
	for _, p := range items.keys {
		paths[p] = true
		assert.NotNil(t, items.objects[p], p)
	}
	assert.Len(t, paths, 3, "each item has its own path: %v", items.keys)
}

// An item's entity tag is of its metadata, too, so a change of it alone,
// such as of a podcast's episode, is a change of the file
func Test_ItemsETag(t *testing.T) {
	feed := func(episode string) string {
		return `<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"><channel><title>T</title>
<item><title>Episode</title><guid>a</guid><pubDate>Fri, 20 Nov 2026 09:00:00 GMT</pubDate><itunes:episode>` + episode + `</itunes:episode></item>
</channel></rss>`
	}
	before, after := newItems(mustChannel(t, feed("1"))), newItems(mustChannel(t, feed("2")))
	p := before.keys["a"]
	require.Equal(t, p, after.keys["a"])
	assert.Equal(t, before.content[p], after.content[p], "the page is the same")
	assert.NotEqual(t, *before.objects[p].ETag, *after.objects[p].ETag)
}

func mustChannel(t *testing.T, xml string) *rss.Channel {
	t.Helper()
	f, err := rss.Read(strings.NewReader(xml))
	require.NoError(t, err)
	return f.Channel
}

// memoryCache returns a cache of a volume, in memory, which is closed when
// the test ends
func memoryCache(t *testing.T) gofiler.CacheStore {
	t.Helper()
	cache, err := sqlite.NewCacheStore(t.Context(), gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close() })
	return cache
}
