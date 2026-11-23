package s3_test

import (
	"testing"

	// Packages
	s3 "github.com/mutablelogic/go-filer/backend/s3"
	assert "github.com/stretchr/testify/assert"
)

func Test_Backend(t *testing.T) {
	// The backend of buckets describes itself, with its icon, and the
	// options of a volume which aren't set, without a volume's own
	b := s3.Backend()
	assert.Equal(t, "s3", b.Scheme())
	assert.Equal(t, "S3 bucket", b.Description())
	if assert.NotNil(t, b.Icon()) {
		assert.Equal(t, "image/svg+xml", b.Icon().Type())
	}
	defaults := b.Defaults()
	assert.Equal(t, "false", defaults.Get("readonly"))
	assert.Equal(t, "false", defaults.Get("anonymous"))
	assert.False(t, defaults.Has("name"))
	assert.False(t, defaults.Has("description"))
}
