package schema

import (
	"encoding/json"
	"testing"
	"time"

	// Packages
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_When(t *testing.T) {
	// A date is of its day, and a time is in UTC, to the second, of the same
	// width, so they sort as text as they do in time
	assert.Equal(t, "2026-12-25", Date(2026, 12, 25).String())
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	assert.Equal(t, "2026-11-20T08:00:00Z", At(time.Date(2026, 11, 20, 9, 0, 0, 999, berlin)).String())

	// They're of JSON, as strings, which are read again
	for _, w := range []When{Date(2026, 12, 25), At(time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC))} {
		data, err := json.Marshal(w)
		require.NoError(t, err)
		var read When
		require.NoError(t, json.Unmarshal(data, &read))
		assert.Equal(t, w, read)
	}
	for _, value := range []string{"2026-13-01", "tomorrow", ""} {
		_, err := ParseWhen(value)
		assert.Error(t, err, value)
	}

	// An occurrence ends with the same kind it starts with, after it
	start, end := Date(2026, 12, 25), Date(2026, 12, 26)
	assert.NoError(t, Occurrence{UID: "h", Kind: OccurrenceEvent, Start: start, End: &end}.Validate())
	assert.Error(t, Occurrence{UID: "h", Kind: OccurrenceEvent, Start: end, End: &start}.Validate())
}
