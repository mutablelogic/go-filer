package registry

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/indexer"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// stateInterval is how often the states of the volumes' indexes are read,
// and a change published of each which changed, such as while it's scanned,
// or its files' metadata is indexed
var stateInterval = 5 * time.Second

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// watchStates publishes a change of what the registry knows of each volume
// whose index's state changed, every stateInterval, until the context is
// cancelled, when there's a broadcaster
func (self *Registry) watchStates(ctx context.Context) {
	if self.broadcaster == nil {
		return
	}
	ticker := time.NewTicker(stateInterval)
	defer ticker.Stop()
	last := make(map[string][]byte)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			self.publishStates(ctx, last)
		}
	}
}

// publishStates reads the state of the index of each volume which is
// mounted, and indexed, and publishes a change of each whose state isn't
// the last one, which it keeps
func (self *Registry) publishStates(ctx context.Context, last map[string][]byte) {
	// The volumes which are mounted
	self.RLock()
	volumes := make(map[string]bool, len(self.backend))
	for name, e := range self.backend {
		if e.state == stateReady {
			volumes[name] = true
		}
	}
	self.RUnlock()

	// Those which aren't any more are forgotten, so their state is published
	// again when they're mounted again
	for name := range last {
		if !volumes[name] {
			delete(last, name)
		}
	}

	// The state of each, which is read while it's in use, so it isn't closed
	// meanwhile
	for _, name := range slices.Sorted(maps.Keys(volumes)) {
		var state []byte
		if err := self.With(ctx, name, func(ctx context.Context, b gofiler.Filer) error {
			if i, ok := b.(*indexer.Indexer); ok {
				state, _ = json.Marshal(indexState(ctx, i))
			}
			return nil
		}); err != nil || state == nil || ctx.Err() != nil {
			continue
		}
		if previous, exists := last[name]; exists && string(previous) == string(state) {
			continue
		}
		last[name] = state
		self.publishState(name)
	}
}
