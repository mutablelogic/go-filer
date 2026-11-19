package schema_test

import (
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_Change(t *testing.T) {
	// An event is a change of the filer schema, of the volume, as the table,
	// and the path, as the row, and back. A rescan is an update without a
	// row, of the volume, or whose row ends with "/", of a directory.
	for _, test := range []struct {
		event  schema.Event
		change changes.Change
	}{
		{event(schema.EventTypeCreated, "/a.txt"), changes.Change{Action: changes.ChangeCreate, Schema: "filer", Table: "media", Row: "/a.txt"}},
		{event(schema.EventTypeUpdated, "/a.txt"), changes.Change{Action: changes.ChangeUpdate, Schema: "filer", Table: "media", Row: "/a.txt"}},
		{event(schema.EventTypeDeleted, "/dir"), changes.Change{Action: changes.ChangeDelete, Schema: "filer", Table: "media", Row: "/dir"}},
		{event(schema.EventTypeRescan, "/"), changes.Change{Action: changes.ChangeUpdate, Schema: "filer", Table: "media"}},
		{event(schema.EventTypeRescan, "/photos"), changes.Change{Action: changes.ChangeUpdate, Schema: "filer", Table: "media", Row: "/photos/"}},

		// A change of a folder, of its key, is of its path, and says it's a
		// folder's, as a row which ends with "/" is a rescan
		{event(schema.EventTypeCreated, "/dir/"), changes.Change{Action: changes.ChangeCreate, Schema: "filer", Table: "media", Row: "/dir", Data: []byte(`{"folder":true}`)}},
		{event(schema.EventTypeUpdated, "/dir/"), changes.Change{Action: changes.ChangeUpdate, Schema: "filer", Table: "media", Row: "/dir", Data: []byte(`{"folder":true}`)}},
		{event(schema.EventTypeDeleted, "/dir/"), changes.Change{Action: changes.ChangeDelete, Schema: "filer", Table: "media", Row: "/dir", Data: []byte(`{"folder":true}`)}},
	} {
		assert.Equal(t, test.change, test.event.Change(), test.event)
		back, ok := schema.ChangeEvent(test.change)
		assert.True(t, ok)
		assert.Equal(t, test.event, back, test.change)
	}

	// A change of another schema, or without a table, isn't an event
	_, ok := schema.ChangeEvent(changes.Change{Action: changes.ChangeCreate, Schema: "llm", Table: "session", Row: "1"})
	assert.False(t, ok)
	_, ok = schema.ChangeEvent(changes.Change{Action: changes.ChangeCreate, Schema: "filer"})
	assert.False(t, ok)
}

func event(eventType schema.EventType, path string) schema.Event {
	return schema.Event{EventType: eventType, ObjectKey: schema.ObjectKey{Name: "media", Path: path}}
}
