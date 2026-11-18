package google

import (
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_taskWatchTake(t *testing.T) {
	filer := &TaskFiler{}
	watch := filer.watch()
	defer filer.unwatch(watch)
	paths := func(changes []taskChange) []string {
		var result []string
		for _, change := range changes {
			result = append(result, change.eventType.String()+" "+change.path+" "+change.etag)
		}
		return result
	}

	// Two writes of a task, while a refresh reads the second, are only the
	// second, which the snapshot has, so the first can't roll it back
	filer.changed(taskChange{eventType: schema.EventTypeUpdated, path: "/L1/A.ics", etag: "E1"})
	filer.changed(taskChange{eventType: schema.EventTypeUpdated, path: "/L1/A.ics", etag: "E2"})
	assert.Equal(t, []string{"updated /L1/A.ics E2"}, paths(watch.take(0)))

	// A change made before a snapshot is read is in it, so it isn't sent,
	// and those after it are, in the order they were made
	filer.changed(taskChange{eventType: schema.EventTypeCreated, path: "/L1/B.ics", etag: "B1"})
	seen := filer.sequence()
	filer.deleted([]string{"/L2/X.ics", "/L2"})
	filer.changed(taskChange{eventType: schema.EventTypeCreated, path: "/L1/C.ics", etag: "C1"})
	assert.Equal(t, []string{"deleted /L2/X.ics ", "deleted /L2 ", "created /L1/C.ics C1"}, paths(watch.take(seen)))
	assert.Empty(t, watch.take(0), "what's taken isn't taken again")
}
