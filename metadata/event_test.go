package metadata

import (
	"testing"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	gometadata "github.com/mutablelogic/go-metadata"
	calendar "github.com/mutablelogic/go-metadata/text/calendar"
	assert "github.com/stretchr/testify/assert"
)

func Test_occurrences(t *testing.T) {
	hint := func(hints ...string) gometadata.Metadata {
		return entry{gometadata.MetadataNamespace, gometadata.HintKey, hints}
	}
	events := []calendar.Event{
		{UID: "m", Kind: "event", Title: "Meeting", Start: "2026-11-20T08:00:00Z", End: "2026-11-20T09:00:00Z", TZ: "Europe/Berlin"},
		{UID: "w", Kind: "event", Title: "Weekly", Start: "2026-11-02T08:00:00Z", End: "2026-11-02T08:30:00Z", TZ: "UTC", RRule: "FREQ=WEEKLY;COUNT=2"},
		{UID: "t", Kind: "task", Title: "Pay", Due: "2026-11-21"},
	}

	// The times of the events of a file with the event hint, each time one
	// which repeats occurs, and a task, of when it's due
	got := occurrences([]gometadata.Metadata{hint(gometadata.HintEvent), entry{"ical", "event", events}})
	end := schema.At(time.Date(2026, 11, 2, 8, 30, 0, 0, time.UTC))
	assert.Len(t, got, 4)
	assert.Equal(t, schema.Occurrence{UID: "w", RecurrenceID: "2026-11-02T08:00:00Z", Kind: "event", Title: "Weekly", Start: schema.At(time.Date(2026, 11, 2, 8, 0, 0, 0, time.UTC)), End: &end, TZ: "UTC"}, got[0])
	assert.Equal(t, "w", got[1].UID)
	assert.Equal(t, "m", got[2].UID)
	assert.Equal(t, schema.Occurrence{UID: "t", Kind: "task", Title: "Pay", Start: schema.Date(2026, 11, 21)}, got[3])

	// Without the hint, it has none
	assert.Nil(t, occurrences([]gometadata.Metadata{entry{"ical", "event", events}}))
}
