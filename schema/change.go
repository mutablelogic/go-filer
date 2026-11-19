package schema

import (
	"encoding/json"
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

// StoreChangeSchema and VolumeChangeTable are of the changes of a store's
// volumes, which are published to a go-server broadcaster, whose row is the
// volume's name, and whose data is the volume, unless it's deleted
const (
	StoreChangeSchema = "store"
	VolumeChangeTable = "volume"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Change returns the event as a change, of ChangeSchema: its action, the
// volume as its table, and its path as its row. A rescan is an update, of
// the volume, without a row, or of a directory, whose row ends with "/", so
// the objects beneath it are read again, as a broadcaster sends a watcher
// which misses a change of a volume. A change of a folder, whose path is its
// key, with a "/" after it, has its path as its row, without it, and the
// data {"folder":true}.
func (e Event) Change() changes.Change {
	change := changes.Change{Schema: ChangeSchema, Table: e.Name, Row: e.Path}
	if e.EventType != EventTypeRescan && len(e.Path) > 1 && strings.HasSuffix(e.Path, "/") {
		change.Row, change.Data = strings.TrimSuffix(e.Path, "/"), json.RawMessage(folderData)
	}
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

// VolumeChange returns the change of a store's volume: its action, its name
// as its row, and the volume as its data, unless it's deleted
func VolumeChange(action changes.ChangeAction, volume *Volume) changes.Change {
	change := changes.Change{Action: action, Schema: StoreChangeSchema, Table: VolumeChangeTable, Row: volume.Name}
	if action != changes.ChangeDelete {
		if data, err := json.Marshal(volume); err == nil {
			change.Data = data
		}
	}
	return change
}

// ChangeEvent returns the event of a change of ChangeSchema, or false when
// it's of another schema: a change without a row, or whose row ends with
// "/", is a rescan, of the volume, or of the directory, and a change whose
// data is {"folder":true} is of a folder, whose path is its key
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
		event.Path = folderKey(event.Path, change.Data)
	case change.Action == changes.ChangeDelete:
		event.EventType = EventTypeDeleted
		event.Path = folderKey(event.Path, change.Data)
	default:
		event.EventType = EventTypeUpdated
		event.Path = folderKey(event.Path, change.Data)
	}
	return event, true
}

// folderData is the data of a change of a folder
const folderData = `{"folder":true}`

// folderKey returns the path of a change, with a "/" after it, when its data
// says it's a folder's
func folderKey(p string, data json.RawMessage) string {
	var folder struct {
		Folder bool `json:"folder"`
	}
	if len(data) > 0 && json.Unmarshal(data, &folder) == nil && folder.Folder {
		return p + "/"
	}
	return p
}
