package registry

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	broadcaster "github.com/mutablelogic/go-server/broadcaster"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_Registry_Run(t *testing.T) {
	dir := t.TempDir()
	r, err := New(context.Background(), WithDebounce(0), WithBroadcaster(newBroadcaster(t)), WithStore(newStore(t)))
	require.NoError(t, err)
	defer r.close()
	assert.False(t, r.Running())

	// A volume created before it runs, which it mounts once it runs, and a
	// watch made before it runs, which lasts until it stops
	before := fileURL(t, dir, "before")
	_, err = r.CreateVolume(context.Background(), schema.VolumeMeta{Name: "before", URL: before.String(), Enabled: true})
	require.NoError(t, err)
	assert.Empty(t, r.Keys(), "it isn't mounted until it runs")
	ch, done := make(chan schema.Event, 100), make(chan error, 1)
	go func() { done <- r.WatchObjects(context.Background(), ch) }()
	watching(t, r, ch)

	// It runs once at a time
	ctx, cancel := context.WithCancel(context.Background())
	running := make(chan error, 1)
	go func() { running <- r.Run(ctx, nil) }()
	require.Eventually(t, r.Running, 5*time.Second, time.Millisecond)
	assert.ErrorIs(t, r.Run(context.Background(), nil), gofiler.ErrConflict)
	_, err = mounted(t, r, "before", nil)
	require.NoError(t, err)

	// The volumes are watched, which are created before it runs, and after
	waitFor(t, ch, filepath.FromSlash(before.Path), func(event schema.Event) bool {
		return event.Name == "before" && event.EventType != schema.EventTypeRescan
	})
	after := fileURL(t, dir, "after")
	_, err = add(t, r, "after", after)
	require.NoError(t, err)
	waitFor(t, ch, filepath.FromSlash(after.Path), func(event schema.Event) bool {
		return event.Name == "after" && event.EventType != schema.EventTypeRescan
	})

	// A volume which is removed isn't watched, as its watch ends before it's
	// removed
	require.NoError(t, del(t, r, "after"))
	require.NoError(t, os.WriteFile(filepath.Join(filepath.FromSlash(after.Path), "gone.txt"), nil, 0o600))
	time.Sleep(100 * time.Millisecond)
	for len(ch) > 0 {
		assert.NotEqual(t, "/gone.txt", (<-ch).Path)
	}

	// When it's cancelled, it closes the registry: its volumes are removed,
	// and closed, and its watches end, and those made later
	cancel()
	select {
	case err := <-running:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("run didn't stop")
	}
	assert.False(t, r.Running())
	assert.Empty(t, r.Keys())
	assert.ErrorIs(t, ended(t, done), fs.ErrClosed)
	assert.ErrorIs(t, r.WatchObjects(context.Background(), ch), fs.ErrClosed)

	// And it can't be run again
	assert.ErrorIs(t, r.Run(context.Background(), nil), fs.ErrClosed)
}

func Test_Registry_RunDebounce(t *testing.T) {
	dir := t.TempDir()
	r, err := New(context.Background(), WithBroadcaster(newBroadcaster(t)), WithStore(newStore(t)))
	require.NoError(t, err)
	t.Cleanup(func() { r.close() })
	assert.Equal(t, debounceDelay, r.debounce)
	runRegistry(t, r)
	u := fileURL(t, dir, "media")
	_, err = add(t, r, "media", u)
	require.NoError(t, err)
	events := watchAll(t, r, 100)

	// A file which is written many times, quickly, is one change, once it's
	// quiet
	path := filepath.FromSlash(u.Path)
	start := time.Now()
	for i := range 10 {
		require.NoError(t, os.WriteFile(filepath.Join(path, "a.txt"), []byte{byte(i)}, 0o600))
	}
	event := next(t, events)
	for event.EventType == schema.EventTypeRescan {
		// The rescans of the volume, once it's watched
		event = next(t, events)
	}
	assert.Equal(t, "/a.txt", event.Path)
	assert.Equal(t, schema.EventTypeCreated, event.EventType)
	assert.GreaterOrEqual(t, time.Since(start), debounceDelay)
	time.Sleep(debounceDelay + 2*rescanInterval)
	for len(events) > 0 {
		event := <-events
		assert.NotEqual(t, "/a.txt", event.Path, event)
	}
}

// watching waits until the call to WatchObjects of the channel is watching,
// by publishing a change of a volume which doesn't exist, until it's sent
func watching(t *testing.T, r *Registry, ch chan schema.Event) {
	t.Helper()
	probe := schema.Event{EventType: schema.EventTypeUpdated, ObjectKey: schema.ObjectKey{Name: "probe", Path: "/probe"}}
	require.Eventually(t, func() bool {
		r.broadcaster.Publish(probe.Change())
		select {
		case event := <-ch:
			return event == probe
		case <-time.After(10 * time.Millisecond):
			return false
		}
	}, 5*time.Second, time.Millisecond)
}

// next returns the next change sent to the channel, or fails the test
func next(t *testing.T, ch chan schema.Event) schema.Event {
	t.Helper()
	select {
	case event := <-ch:
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("no change")
		return schema.Event{}
	}
}

// ended returns the error of a watch which ends, or fails the test
func ended(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the watch didn't end")
		return nil
	}
}

func Test_Registry_Broadcaster(t *testing.T) {
	dir := t.TempDir()
	b := newBroadcaster(t)
	r, err := New(context.Background(), WithDebounce(0), WithBroadcaster(b), WithStore(newStore(t)))
	require.NoError(t, err)

	// The changes to the volumes are published to it, as changes of the
	// filer schema, of the volume, as the table, and the path, as the row
	ch := make(chan changes.Change, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = b.Watch(ctx, changes.ChangeFilter{Schema: schema.ChangeSchema, Table: "media"}, ch)
	require.NoError(t, err)
	runRegistry(t, r)
	u := fileURL(t, dir, "media")
	_, err = add(t, r, "media", u)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_ = os.WriteFile(filepath.Join(filepath.FromSlash(u.Path), "a.txt"), nil, 0o600)
		select {
		case change := <-ch:
			return change.Row == "/a.txt"
		case <-time.After(50 * time.Millisecond):
			return false
		}
	}, 10*time.Second, time.Millisecond)

	// Closing the registry doesn't stop the broadcaster, which is its owner's
	require.NoError(t, r.close())
	other := make(chan changes.Change, 1)
	_, err = b.Watch(ctx, changes.ChangeFilter{}, other)
	assert.NoError(t, err)

	// A broadcaster which is nil isn't one, and without one, the changes
	// aren't published, so they can't be watched
	_, err = New(context.Background(), WithBroadcaster(nil), WithStore(newStore(t)))
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	without, err := New(context.Background(), WithStore(newStore(t)))
	require.NoError(t, err)
	defer without.close()
	assert.ErrorIs(t, without.WatchObjects(context.Background(), make(chan schema.Event)), gofiler.ErrNotImplemented)
}

// newBroadcaster returns a broadcaster, which runs until the test ends
func newBroadcaster(t *testing.T) *broadcaster.Broadcaster {
	t.Helper()
	b := broadcaster.New()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = b.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return b
}
