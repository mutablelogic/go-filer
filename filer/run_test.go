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
	r, err := New(context.Background())
	require.NoError(t, err)
	defer r.Close()
	assert.False(t, r.Running())

	// A volume added before it runs, and a subscription made before it runs,
	// which lasts until it stops
	before := fileURL(t, dir, "before")
	_, err = r.Add(context.Background(), "before", before)
	require.NoError(t, err)
	ch := make(chan schema.Event, 100)
	done, err := r.Subscribe(context.Background(), schema.EventFilter{}, ch)
	require.NoError(t, err)
	assert.Equal(t, 1, r.Subscribers())

	// It runs once at a time, and is listening once it's running
	ctx, cancel := context.WithCancel(context.Background())
	running := make(chan error, 1)
	go func() { running <- r.Run(ctx, nil) }()
	require.NoError(t, r.WaitListening(context.Background()))
	assert.True(t, r.Running())
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

	// When it stops, every subscription ends, and it isn't listening
	cancel()
	assert.NoError(t, <-running)
	assert.ErrorIs(t, ended(t, done), fs.ErrClosed)
	assert.Equal(t, 0, r.Subscribers())
	assert.False(t, r.Running())
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer waitCancel()
	assert.ErrorIs(t, r.WaitListening(waitCtx), context.DeadlineExceeded)

	// And when the registry is closed, it stops
	go func() { running <- r.Run(context.Background(), nil) }()
	require.NoError(t, r.WaitListening(context.Background()))
	require.NoError(t, r.Close())
	select {
	case err := <-running:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("run didn't stop")
	}
	_, err = r.Subscribe(context.Background(), schema.EventFilter{}, ch)
	assert.ErrorIs(t, err, fs.ErrClosed)
}
