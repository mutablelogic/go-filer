package registry

import (
	"context"
	"sync"
	"testing"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_Registry_IndexStateChanges(t *testing.T) {
	interval := stateInterval
	stateInterval = 10 * time.Millisecond
	t.Cleanup(func() { stateInterval = interval })

	// A registry, whose changes of what it knows of its volumes are counted
	s, err := sqlite.New(context.Background(), gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	b := newBroadcaster(t)
	r, err := New(context.Background(), WithDebounce(0), WithBroadcaster(b), WithStore(s))
	require.NoError(t, err)
	t.Cleanup(func() { r.close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch := make(chan changes.Change, 1000)
	_, err = b.Watch(ctx, changes.ChangeFilter{Schema: schema.RegistryChangeSchema, Table: schema.VolumeChangeTable}, ch)
	require.NoError(t, err)
	var mu sync.Mutex
	published := 0
	go func() {
		for change := range ch {
			if change.Row == "media" {
				mu.Lock()
				published++
				mu.Unlock()
			}
		}
	}()
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return published
	}
	runRegistry(t, r)

	// An indexed volume's state is published, once it's indexed, and not
	// again while it doesn't change
	_, err = add(t, r, "media", indexedURL(t, t.TempDir(), "media", "folders"))
	require.NoError(t, err)
	ready(t, r, "media")
	require.Eventually(t, func() bool { return count() > 0 }, 5*time.Second, time.Millisecond)
	var quiet int
	require.Eventually(t, func() bool {
		before := count()
		time.Sleep(5 * stateInterval)
		quiet = count()
		return quiet == before
	}, 5*time.Second, time.Millisecond, "its state is published while it doesn't change")
	time.Sleep(10 * stateInterval)
	assert.Equal(t, quiet, count(), "its state is published while it doesn't change")

	// When its state changes, it's published again
	state, err := s.State(context.Background(), "media")
	require.NoError(t, err)
	state.Folders += 10
	require.NoError(t, s.SetState(context.Background(), "media", state))
	require.Eventually(t, func() bool { return count() > quiet }, 5*time.Second, time.Millisecond, "its changed state isn't published")
}
