package filer

import (
	"testing"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
)

// eventOf returns a change of the type, to the object at the path
func eventOf(eventType schema.EventType, volume, path string) schema.Event {
	return schema.Event{EventType: eventType, ObjectKey: schema.ObjectKey{Name: volume, Path: path}}
}

func Test_Debouncer_Merge(t *testing.T) {
	const (
		created = schema.EventTypeCreated
		updated = schema.EventTypeUpdated
		deleted = schema.EventTypeDeleted
	)

	// The changes of an object, which are held, are merged, or cancel
	for _, test := range []struct {
		changes []schema.EventType
		want    []schema.Event
	}{
		{[]schema.EventType{created}, []schema.Event{eventOf(created, "media", "/a")}},
		{[]schema.EventType{created, updated, updated}, []schema.Event{eventOf(created, "media", "/a")}},
		{[]schema.EventType{updated, updated}, []schema.Event{eventOf(updated, "media", "/a")}},
		{[]schema.EventType{created, deleted}, []schema.Event{}},
		{[]schema.EventType{created, updated, deleted}, []schema.Event{}},
		{[]schema.EventType{updated, deleted}, []schema.Event{eventOf(deleted, "media", "/a")}},
		{[]schema.EventType{deleted, created}, []schema.Event{eventOf(updated, "media", "/a")}},
		{[]schema.EventType{created, deleted, created}, []schema.Event{eventOf(created, "media", "/a")}},
	} {
		d, now := newDebouncer(time.Second), time.Now()
		for _, eventType := range test.changes {
			assert.Empty(t, d.add(eventOf(eventType, "media", "/a"), now))
		}
		assert.Empty(t, d.due(now, false), test.changes)
		assert.Equal(t, test.want, d.due(now.Add(time.Second), false), test.changes)
	}
}

func Test_Debouncer_Due(t *testing.T) {
	d, now := newDebouncer(time.Second), time.Now()

	// A change is held until its object is quiet for the delay, in the order
	// of the first changes, and objects are apart
	d.add(eventOf(schema.EventTypeCreated, "media", "/b"), now)
	d.add(eventOf(schema.EventTypeCreated, "media", "/a"), now.Add(time.Millisecond))
	d.add(eventOf(schema.EventTypeCreated, "other", "/a"), now.Add(2*time.Millisecond))
	d.add(eventOf(schema.EventTypeUpdated, "media", "/b"), now.Add(500*time.Millisecond))
	assert.Equal(t, []schema.Event{eventOf(schema.EventTypeCreated, "media", "/a"), eventOf(schema.EventTypeCreated, "other", "/a")}, d.due(now.Add(time.Second+2*time.Millisecond), false))
	assert.Equal(t, []schema.Event{eventOf(schema.EventTypeCreated, "media", "/b")}, d.due(now.Add(1500*time.Millisecond), false))

	// An object which changes all the time is sent once it's held for the
	// most it can be
	for i := range 10 {
		d.add(eventOf(schema.EventTypeUpdated, "media", "/c"), now.Add(time.Duration(i)*500*time.Millisecond))
		if at := now.Add(time.Duration(i) * 500 * time.Millisecond); at.Sub(now) < debounceMax {
			assert.Empty(t, d.due(at, false))
		}
	}
	assert.Equal(t, []schema.Event{eventOf(schema.EventTypeUpdated, "media", "/c")}, d.due(now.Add(debounceMax), false))

	// All the changes which are held, when it's asked for
	d.add(eventOf(schema.EventTypeCreated, "media", "/d"), now)
	assert.Equal(t, []schema.Event{eventOf(schema.EventTypeCreated, "media", "/d")}, d.due(now, true))
	assert.Empty(t, d.pending)
}

func Test_Debouncer_Rescan(t *testing.T) {
	d, now := newDebouncer(time.Second), time.Now()

	// A rescan is sent at once, in place of its volume's changes which are
	// held, but not other volumes'
	d.add(eventOf(schema.EventTypeCreated, "media", "/a"), now)
	d.add(eventOf(schema.EventTypeCreated, "other", "/a"), now)
	assert.Equal(t, []schema.Event{rescanEvent("media")}, d.add(rescanEvent("media"), now))
	assert.Equal(t, []schema.Event{eventOf(schema.EventTypeCreated, "other", "/a")}, d.due(now, true))

	// Without a delay, a change is sent at once
	d = newDebouncer(0)
	assert.Equal(t, []schema.Event{eventOf(schema.EventTypeCreated, "media", "/a")}, d.add(eventOf(schema.EventTypeCreated, "media", "/a"), now))
	assert.Empty(t, d.pending)
}
