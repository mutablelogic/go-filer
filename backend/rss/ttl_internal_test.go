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
		f, err := Open(context.Background(), &url.URL{Scheme: Scheme, Host: "feed", Path: "/" + host + "/", RawQuery: query.Encode()}, gofiler.Opts{})
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
