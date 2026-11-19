package schema

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	// Packages
	geo "github.com/mutablelogic/go-metadata/pkg/geo"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Occurrence is a time an event, or a task, of a calendar, is: when it
// starts, and, when it isn't an instant, ends, which are dates, of an
// all-day event, or times, of its zone, its title, and its UID, and, of an
// event which repeats, its recurrence id, of when it was to start, which
// identify it, as an event can be in more than one object, such as a
// calendar, and an invitation
type Occurrence struct {
	UID          string `json:"uid"`
	RecurrenceID string `json:"recurrence_id,omitempty"`
	Kind         string `json:"kind"`
	Title        string `json:"title,omitempty"`
	Start        When   `json:"start"`
	End          *When  `json:"end,omitempty"` // Exclusive, or nil, of an instant, or a task, which is due
	TZ           string `json:"tz,omitempty"`  // The zone of its times, such as Europe/Berlin, or empty, of an all-day event
}

// When is a date, without a time, of an all-day event, which is midnight,
// in UTC, of its day, or a time, which is in UTC
type When struct {
	Time time.Time
	Date bool
}

// TaskMeta is what a task found of an object: its metadata, which a read
// of it merges with the backend's, the shapes of where it is, and the times
// of its events
type TaskMeta struct {
	Meta   []Meta
	Geo    []geo.Geometry
	Events []Occurrence
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	OccurrenceEvent = "event" // Of a VEVENT
	OccurrenceTask  = "task"  // Of a VTODO, which starts when it's due

	// whenDate and whenTime are the formats of a date, and a time, which are
	// of the same length as each other's, so they sort
	whenDate = "2006-01-02"
	whenTime = "2006-01-02T15:04:05Z"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Date returns the when of a date
func Date(year int, month time.Month, day int) When {
	return When{Time: time.Date(year, month, day, 0, 0, 0, 0, time.UTC), Date: true}
}

// At returns the when of a time, in UTC, to the second
func At(t time.Time) When {
	return When{Time: t.UTC().Truncate(time.Second)}
}

// ParseWhen returns the when of a date, such as 2026-11-20, or a time, such
// as 2026-11-20T09:00:00Z, of RFC 3339
func ParseWhen(value string) (When, error) {
	value = strings.TrimSpace(value)
	if len(value) == len(whenDate) {
		t, err := time.Parse(whenDate, value)
		if err != nil {
			return When{}, fmt.Errorf("invalid date %q", value)
		}
		return Date(t.Year(), t.Month(), t.Day()), nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return When{}, fmt.Errorf("invalid time %q", value)
	}
	return At(t), nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// IsZero returns true when there's no when
func (w When) IsZero() bool {
	return w.Time.IsZero()
}

// String returns a date as 2026-11-20, or a time as 2026-11-20T09:00:00Z,
// which sort, as text, as they do in time, of the same kind
func (w When) String() string {
	if w.Date {
		return w.Time.Format(whenDate)
	}
	return w.Time.UTC().Format(whenTime)
}

// Validate returns an error when an occurrence has no UID, isn't of an
// event, or task, has no start, or ends before it starts, or ends with a
// date, of a time, or a time, of a date
func (o Occurrence) Validate() error {
	switch {
	case strings.TrimSpace(o.UID) == "":
		return fmt.Errorf("an occurrence without a UID")
	case o.Kind != OccurrenceEvent && o.Kind != OccurrenceTask:
		return fmt.Errorf("an occurrence of %q, rather than an event or task", o.Kind)
	case o.Start.IsZero():
		return fmt.Errorf("an occurrence of %q without a start", o.UID)
	case o.End != nil && o.End.Date != o.Start.Date:
		return fmt.Errorf("an occurrence of %q whose start and end aren't both dates, or times", o.UID)
	case o.End != nil && o.End.Time.Before(o.Start.Time):
		return fmt.Errorf("an occurrence of %q which ends before it starts", o.UID)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// JSON

// MarshalJSON returns the when as a string, of a date, or a time
func (w When) MarshalJSON() ([]byte, error) {
	return json.Marshal(w.String())
}

// UnmarshalJSON reads the when of a string, of a date, or a time
func (w *When) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	when, err := ParseWhen(value)
	if err != nil {
		return err
	}
	*w = when
	return nil
}
