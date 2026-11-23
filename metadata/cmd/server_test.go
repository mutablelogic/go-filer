//go:build !client

package cmd

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	// Packages
	assert "github.com/stretchr/testify/assert"
)

func Test_logLookups(t *testing.T) {
	logs := func(flags Flags, fingerprints bool) []string {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		}}))
		flags.logLookups(context.Background(), logger, fingerprints)
		return strings.Split(strings.TrimSpace(buf.String()), "\n")
	}

	// Each lookup which is off is a warning, of the variable which turns it on
	got := logs(Flags{}, true)
	if assert.Len(t, got, 2) {
		assert.Contains(t, got[0], "level=WARN")
		assert.Contains(t, got[0], "TMDB_TOKEN")
		assert.Contains(t, got[1], "level=WARN")
		assert.Contains(t, got[1], "ACOUSTID_KEY")
	}

	// Each which is on is information, and AcoustID without Chromaprint is
	// a warning, even with its key
	got = logs(Flags{TMDBKey: "t", AcoustIDKey: "a"}, true)
	if assert.Len(t, got, 2) {
		assert.Contains(t, got[0], "level=INFO")
		assert.Contains(t, got[0], "TMDB lookups are on")
		assert.Contains(t, got[1], "level=INFO")
		assert.Contains(t, got[1], "fingerprints")
	}
	got = logs(Flags{AcoustIDKey: "a"}, false)
	if assert.Len(t, got, 2) {
		assert.Contains(t, got[1], "level=WARN")
		assert.Contains(t, got[1], "Chromaprint isn't built in")
	}

	// The keys themselves are never logged
	for _, line := range logs(Flags{TMDBKey: "secret-tmdb", AcoustIDKey: "secret-acoustid"}, true) {
		assert.NotContains(t, line, "secret")
	}
}
