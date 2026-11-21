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

var (
	// stateInterval is how often the states of the volumes' indexes are
	// checked, and a change published of each which changed, such as while
	// it's scanned, or its files' metadata is indexed. A state is read only
	// when its indexer's revision changed, or at least every stateRefresh,
	// as it can be changed of another process, such as another server of
	// the store, which doesn't revise it.
	stateInterval = 5 * time.Second
	stateRefresh  = time.Minute
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// published is the state of a volume's index which was published last, of
// the indexer's revision, and when it was read
type published struct {
	state    []byte
	revision uint64
	read     time.Time
}

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
	last := make(map[string]published)
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
// mounted, and indexed, whose revision changed, or which wasn't read for a
// while, and publishes a change of each whose state isn't the last one,
// which it keeps
func (self *Registry) publishStates(ctx context.Context, last map[string]published) {
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
		previous, exists := last[name]
		next := previous
		if err := self.With(ctx, name, func(ctx context.Context, b gofiler.Filer) error {
			i, ok := b.(*indexer.Indexer)
			if !ok {
				return nil
			}
			// Its revision is read before its state, so a change while it's
			// read is read again, of the next revision
			revision := i.Revision()
			if exists && revision == previous.revision && time.Since(previous.read) < stateRefresh {
				return nil
			}
			state, err := json.Marshal(indexState(ctx, i))
			if err == nil {
				next = published{state: state, revision: revision, read: time.Now()}
			}
			return err
		}); err != nil || next.state == nil || ctx.Err() != nil {
			continue
		}
		last[name] = next
		if !exists || string(previous.state) != string(next.state) {
			self.publishState(name)
		}
	}
}
