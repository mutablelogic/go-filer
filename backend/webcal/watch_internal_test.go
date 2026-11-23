package webcal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

const watchICS = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
	"BEGIN:VEVENT\r\nUID:a\r\nDTSTART;VALUE=DATE:20261201\r\nSUMMARY:First\r\nEND:VEVENT\r\n" +
	"END:VCALENDAR\r\n"

// Test_WatchAfterRead tests a feed which a read fetched, between a watch's
// polls, isn't modified, of its entity tag, when the watch polls, but what
// changed, of the events the watch last sent, is sent
func Test_WatchAfterRead(t *testing.T) {
	var body atomic.Pointer[string]
	body.Store(&[]string{watchICS}[0])
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := *body.Load()
		sum := sha256.Sum256([]byte(b))
		etag := `"` + hex.EncodeToString(sum[:8]) + `"`
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		io.WriteString(w, b)
	}))
	defer server.Close()
	cache, err := sqlite.NewCacheStore(context.Background(), gosqlite.Memory)
	require.NoError(t, err)
	u := &url.URL{Scheme: Scheme, Host: "cal", Path: "/" + strings.TrimPrefix(server.URL, "http://") + "/", RawQuery: "insecure=true&refresh_interval=300ms"}
	filer, err := Open(context.Background(), u, cache, gofiler.Opts{})
	require.NoError(t, err)
	f := filer.(*Filer)

	// A clock which a read is of, which is an hour later, so a read fetches
	// the feed, whenever it's made
	var mu sync.Mutex
	offset := time.Duration(0)
	f.mu.Lock()
	f.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return time.Now().Add(offset) }
	f.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan schema.Event, 16)
	go func() { _ = f.WatchObjects(ctx, ch) }()
	require.Equal(t, schema.EventTypeRescan, (<-ch).EventType)

	// The feed changes, and a read fetches it, before the watch polls
	changed := strings.Replace(watchICS, "SUMMARY:First", "SUMMARY:Second", 1)
	body.Store(&changed)
	mu.Lock()
	offset = time.Hour
	mu.Unlock()
	_, err = f.ListObjects(context.Background(), schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true})
	require.NoError(t, err)

	var got []string
	timeout := time.After(5 * time.Second)
	for len(got) < 2 {
		select {
		case event := <-ch:
			got = append(got, event.EventType.String()+" "+event.Path)
		case <-timeout:
			t.Fatalf("got %v", got)
		}
	}
	assert.ElementsMatch(t, []string{"created /2026/2026-12/01-second.ics", "deleted /2026/2026-12/01-first.ics"}, got)
}
