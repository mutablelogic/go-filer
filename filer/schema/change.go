package schema

import (
	"strings"

	// Packages
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// ChangeSchema is the schema of the changes of a filer's objects, which are
// published to a go-server broadcaster, whose table is the volume, and whose
// row is the object's path
const ChangeSchema = "filer"

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Change returns the event as a change, of ChangeSchema: its action, the
// volume as its table, and its path as its row. A rescan is an update, of
// the volume, without a row, or of a directory, whose row ends with "/", so
// the objects beneath it are read again, as a broadcaster sends a watcher
// which misses a change of a volume.
func (e Event) Change() changes.Change {
	change := changes.Change{Schema: ChangeSchema, Table: e.Name, Row: e.Path}
	switch e.EventType {
	case EventTypeCreated:
		change.Action = changes.ChangeCreate
	case EventTypeDeleted:
		change.Action = changes.ChangeDelete
	case EventTypeRescan:
		change.Action = changes.ChangeUpdate
		if dir := strings.Trim(e.Path, "/"); dir == "" {
			change.Row = ""
		} else {
			change.Row = "/" + dir + "/"
		}
	default:
		change.Action = changes.ChangeUpdate
	}
	return change
}

// ChangeEvent returns the event of a change of ChangeSchema, or false when
// it's of another schema: a change without a row, or whose row ends with
// "/", is a rescan, of the volume, or of the directory
func ChangeEvent(change changes.Change) (Event, bool) {
	if change.Schema != ChangeSchema || change.Table == "" {
		return Event{}, false
	}
	event := Event{ObjectKey: ObjectKey{Name: change.Table, Path: change.Row}}
	switch {
	case change.Row == "" || strings.HasSuffix(change.Row, "/"):
		event.EventType = EventTypeRescan
		if dir := strings.Trim(change.Row, "/"); dir == "" {
			event.Path = "/"
		} else {
			event.Path = "/" + dir
		}
	case change.Action == changes.ChangeCreate:
		event.EventType = EventTypeCreated
	case change.Action == changes.ChangeDelete:
		event.EventType = EventTypeDeleted
	default:
		event.EventType = EventTypeUpdated
	}
	return event, true
}
