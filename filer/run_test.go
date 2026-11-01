package filer

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_Registry_Run(t *testing.T) {
	dir := t.TempDir()
	r, err := New(context.Background(), WithDebounce(0))
	require.NoError(t, err)
	defer r.Close()
	assert.False(t, r.Running())

	// A volume added before it runs, and a watch made before it runs, which
	// lasts until it stops
	before := fileURL(t, dir, "before")
	_, err = r.Add(context.Background(), "before", before)
	require.NoError(t, err)
	ch, done := make(chan schema.Event, 100), make(chan error, 1)
	go func() { done <- r.WatchObjects(context.Background(), ch) }()
	require.Eventually(t, func() bool { return r.broadcaster.len() == 1 }, 5*time.Second, time.Millisecond)

	// It runs once at a time
	ctx, cancel := context.WithCancel(context.Background())
	running := make(chan error, 1)
	go func() { running <- r.Run(ctx, nil) }()
	require.Eventually(t, r.Running, 5*time.Second, time.Millisecond)
	assert.ErrorIs(t, r.Run(context.Background(), nil), gofiler.ErrConflict)

	// The volumes are watched, which are added before it runs, and after
	waitFor(t, ch, filepath.FromSlash(before.Path), func(event schema.Event) bool { return event.Name == "before" })
	after := fileURL(t, dir, "after")
	_, err = r.Add(context.Background(), "after", after)
	require.NoError(t, err)
	waitFor(t, ch, filepath.FromSlash(after.Path), func(event schema.Event) bool { return event.Name == "after" })

	// A volume which is removed isn't watched, as its watch ends before it's
	// removed
	require.NoError(t, r.Del(context.Background(), "after"))
	require.NoError(t, os.WriteFile(filepath.Join(filepath.FromSlash(after.Path), "gone.txt"), nil, 0o600))
	time.Sleep(100 * time.Millisecond)
	for len(ch) > 0 {
		assert.NotEqual(t, "/gone.txt", (<-ch).Path)
	}

	// When it stops, every watch ends
	cancel()
	assert.NoError(t, <-running)
	assert.ErrorIs(t, ended(t, done), fs.ErrClosed)
	assert.Equal(t, 0, r.broadcaster.len())
	assert.False(t, r.Running())

	// And when the registry is closed, it stops
	go func() { running <- r.Run(context.Background(), nil) }()
	require.Eventually(t, r.Running, 5*time.Second, time.Millisecond)
	require.NoError(t, r.Close())
	select {
	case err := <-running:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("run didn't stop")
	}
	assert.ErrorIs(t, r.WatchObjects(context.Background(), ch), fs.ErrClosed)
}

func Test_Registry_RunDebounce(t *testing.T) {
	dir := t.TempDir()
	r, err := New(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { r.Close() })
	assert.Equal(t, debounceDelay, r.debounce)
	runRegistry(t, r)
	u := fileURL(t, dir, "media")
	_, err = r.Add(context.Background(), "media", u)
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
	assert.Equal(t, "/a.txt", event.Path)
	assert.Equal(t, schema.EventTypeCreated, event.EventType)
	assert.GreaterOrEqual(t, time.Since(start), debounceDelay)
	time.Sleep(debounceDelay + 2*rescanInterval)
	for len(events) > 0 {
		event := <-events
		assert.NotEqual(t, "/a.txt", event.Path, event)
	}
}
