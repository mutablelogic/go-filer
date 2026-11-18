package google

import (
	"context"

	// Packages
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

// WatchObjectsReady watches the changes, as WatchObjects does, and calls
// ready once what's there is read, so a test makes its changes after it
func (self *TaskFiler) WatchObjectsReady(ctx context.Context, ch chan<- schema.Event, ready func()) error {
	return self.watchObjects(ctx, ch, ready)
}
