package metadata

import (
	"image"
	"testing"

	// Packages
	gometadata "github.com/mutablelogic/go-metadata"
	assert "github.com/stretchr/testify/assert"
)

// entry is an entry of go-metadata's
type entry struct {
	ns, key string
	value   any
}

func (e entry) NS() string         { return e.ns }
func (e entry) Key() string        { return e.key }
func (e entry) Value() string      { return "" }
func (e entry) Bytes() []byte      { return nil }
func (e entry) Image() image.Image { return nil }
func (e entry) Any() any           { return e.value }

func Test_meta(t *testing.T) {
	// A key with more than one value, such as a recording's artists, is an
	// array of its values, in their order, without those repeated, where
	// its first value is
	result := meta([]gometadata.Metadata{
		entry{"musicbrainz", "artist-id", "a"},
		entry{"dc", "title", "Song"},
		entry{"musicbrainz", "artist-id", "b"},
		entry{"musicbrainz", "artist-id", "a"},
		entry{"dc", "identifier", 1},
		entry{"dc", "identifier", 1},
	})
	if assert.Len(t, result, 3) {
		assert.Equal(t, "artist-id", result[0].Key)
		assert.JSONEq(t, `["a","b"]`, string(result[0].Value))
		assert.Equal(t, "title", result[1].Key)
		assert.JSONEq(t, `"Song"`, string(result[1].Value))
		assert.Equal(t, "identifier", result[2].Key)
		assert.JSONEq(t, `1`, string(result[2].Value))
	}
}
