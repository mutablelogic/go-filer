package registry

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// readCounter is a store which counts how often the states of indexes are
// read
type readCounter struct {
	store.Store
	reads atomic.Int64
}

func (s *readCounter) State(ctx context.Context, volume string) (schema.IndexState, error) {
	s.reads.Add(1)
	return s.Store.State(ctx, volume)
}

// stateRegistry returns a running registry of an indexed volume, media, of
// a store which counts its reads, which is ready, the volume's directory,
// and how many changes of what the registry knows of it are published
func stateRegistry(t *testing.T, interval, refresh time.Duration) (*Registry, *readCounter, string, func() int) {
	t.Helper()
	intervals := [2]time.Duration{stateInterval, stateRefresh}
	stateInterval, stateRefresh = interval, refresh
	t.Cleanup(func() { stateInterval, stateRefresh = intervals[0], intervals[1] })

	sq, err := sqlite.New(context.Background(), gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { sq.Close() })
	s := &readCounter{Store: sq}
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
	runRegistry(t, r)
	u := indexedURL(t, t.TempDir(), "media", "folders")
	_, err = add(t, r, "media", u)
	require.NoError(t, err)
	ready(t, r, "media")
	return r, s, filepath.FromSlash(u.Path), func() int {
		mu.Lock()
		defer mu.Unlock()
		return published
	}
}

// quiet waits until the function's count doesn't change for a while, and
// returns it
func quiet(t *testing.T, count func() int, interval time.Duration) int {
	t.Helper()
	var n int
	require.Eventually(t, func() bool {
		before := count()
		time.Sleep(5 * interval)
		n = count()
		return n == before
	}, 5*time.Second, time.Millisecond, "it doesn't stop changing")
	return n
}

func Test_Registry_IndexStateChanges(t *testing.T) {
	// An indexed volume's state is published, once it's indexed, and not
	// again while it doesn't change, when it isn't read, as its indexer's
	// revision doesn't change
	_, s, dir, published := stateRegistry(t, 10*time.Millisecond, time.Hour)
	require.Eventually(t, func() bool { return published() > 0 }, 5*time.Second, time.Millisecond)
	n := quiet(t, published, stateInterval)
	reads := quiet(t, func() int { return int(s.reads.Load()) }, stateInterval)
	time.Sleep(10 * stateInterval)
	assert.Equal(t, n, published(), "its state is published while it doesn't change")
	assert.Equal(t, int64(reads), s.reads.Load(), "its state is read while its revision doesn't change")

	// When it's changed, of its indexer, its state is read again
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "c"), 0o755))
	require.Eventually(t, func() bool { return s.reads.Load() > int64(reads) }, 5*time.Second, time.Millisecond, "its state isn't read once it's revised")
}

func Test_Registry_IndexStateRefresh(t *testing.T) {
	// A state which is changed of another process, which doesn't revise it,
	// is read, and published, once a while has passed
	_, s, _, published := stateRegistry(t, 10*time.Millisecond, 50*time.Millisecond)
	require.Eventually(t, func() bool { return published() > 0 }, 5*time.Second, time.Millisecond)
	n := quiet(t, published, stateRefresh)
	state, err := s.State(context.Background(), "media")
	require.NoError(t, err)
	state.Folders += 10
	require.NoError(t, s.Store.SetState(context.Background(), "media", state))
	require.Eventually(t, func() bool { return published() > n }, 5*time.Second, time.Millisecond, "its changed state isn't published")
}
