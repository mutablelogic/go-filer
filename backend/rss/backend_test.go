package rss_test

import (
	"testing"

	// Packages
	rss "github.com/mutablelogic/go-filer/backend/rss"
	assert "github.com/stretchr/testify/assert"
)

func Test_Backend(t *testing.T) {
	// The backend of feeds describes itself, with its icon, and the options
	// of a volume which aren't set
	b := rss.Backend()
	assert.Equal(t, "rss", b.Scheme())
	assert.Equal(t, "RSS feed", b.Description())
	if assert.NotNil(t, b.Icon()) {
		assert.Equal(t, "image/svg+xml", b.Icon().Type())
	}
	assert.Equal(t, "true", b.Defaults().Get("readonly"))
}
