package metadata

import (
	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	gometadata "github.com/mutablelogic/go-metadata"
	ical "github.com/mutablelogic/go-metadata/pkg/ical"
	calendar "github.com/mutablelogic/go-metadata/text/calendar"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// maxOccurrences is the most times a file's events occur which are stored,
// such as of a calendar of many events, which repeat
const maxOccurrences = 1000

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// occurrences returns the times a file's events occur, of go-metadata's
// events, such as a calendar's, of whatever namespace, and key, they're of,
// when its hints say it has events (gometadata.HintEvent), and none
// otherwise: each event, and task, which starts when it's due, and each
// time one which repeats occurs, when its rule ends (see
// calendar.Occurrences)
func occurrences(entries []gometadata.Metadata) []schema.Occurrence {
	if !hinted(entries, gometadata.HintEvent) {
		return nil
	}
	var events []calendar.Event
	for _, entry := range entries {
		switch v := entry.Any().(type) {
		case []calendar.Event:
			events = append(events, v...)
		case calendar.Event:
			events = append(events, v)
		}
	}
	var result []schema.Occurrence
	for _, o := range calendar.Occurrences(events, maxOccurrences) {
		occurrence := schema.Occurrence{UID: o.UID, RecurrenceID: o.RecurrenceID, Kind: o.Kind, Title: o.Title, Start: when(o.Start), TZ: o.TZ}
		if !o.End.IsZero() {
			end := when(o.End)
			occurrence.End = &end
		}
		if occurrence.Validate() == nil {
			result = append(result, occurrence)
		}
	}
	return result
}

// when returns the when of a date, or a time, of go-metadata's
func when(t ical.Time) schema.When {
	if t.Date {
		return schema.Date(t.Time.Year(), t.Time.Month(), t.Time.Day())
	}
	return schema.At(t.Time)
}
