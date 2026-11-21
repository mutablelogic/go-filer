package metadata

import (
	"image"
	"testing"
	"time"

	// Packages
	gometadata "github.com/mutablelogic/go-metadata"
	assert "github.com/stretchr/testify/assert"
)

// entry is an entry of go-metadata's, without text
type entry struct {
	ns, key string
	value   any
}

// textEntry is an entry of go-metadata's, of its text
type textEntry struct {
	entry
	text string
}

func (e entry) NS() string         { return e.ns }
func (e entry) Key() string        { return e.key }
func (e entry) Value() string      { return "" }
func (e textEntry) Value() string  { return e.text }
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

func Test_metaText(t *testing.T) {
	when := time.Date(2009, 9, 2, 23, 17, 0, 0, time.UTC)
	result := meta([]gometadata.Metadata{
		textEntry{entry{"audio", "duration", 5 * time.Second}, "5s"},
		textEntry{entry{"dc", "title", "Song"}, "Song"},
		textEntry{entry{"audio", "channels", 2}, "2"},
		textEntry{entry{"dc", "date", when}, when.Format(time.RFC3339)},
		textEntry{entry{"exif", "exposure", 0.004}, "1/250 s"},
		textEntry{entry{"metadata", "hint", []string{"artwork", "markdown"}}, "artwork,markdown"},
		textEntry{entry{"audio", "bitrate", 128000}, "128 kb/s"},
		textEntry{entry{"audio", "bitrate", 256000}, ""},
	})
	got := make(map[string][2]string, len(result))
	for _, m := range result {
		got[m.Namespace+":"+m.Key] = [2]string{string(m.Value), m.Text}
	}

	// A duration's value is its nanoseconds, and its text says what it is
	assert.Equal(t, [2]string{"5000000000", "5s"}, got["audio:duration"])

	// A string, a number, a list and a time read as their values, so have
	// no text, as a time is shown in the reader's own time
	assert.Equal(t, [2]string{`"Song"`, ""}, got["dc:title"])
	assert.Equal(t, [2]string{"2", ""}, got["audio:channels"])
	assert.Equal(t, "", got["dc:date"][1])
	assert.Equal(t, "", got["metadata:hint"][1])

	// An entry's own text, such as of a unit, is kept
	assert.Equal(t, [2]string{"0.004", "1/250 s"}, got["exif:exposure"])

	// Of more than one value, the texts are of each, of its value when it
	// has none
	assert.Equal(t, [2]string{"[128000,256000]", "128 kb/s, 256000"}, got["audio:bitrate"])
}
