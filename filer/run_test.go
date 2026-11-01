package filer

import (
	"context"
	"io/fs"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// change returns a change to the object at the path of the volume
func change(volume, path string) schema.Event {
	return schema.Event{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Name: volume, Path: path}}
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

// ended returns the error of a subscription which ends, or fails the test
func ended(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription didn't end")
		return nil
	}
}

func Test_Registry_Run(t *testing.T) {
	r, err := New(context.Background())
	require.NoError(t, err)
	defer r.Close()
	assert.False(t, r.Running())

	// A subscription made before it runs lasts until it stops
	ch := make(chan schema.Event, 1)
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

	// Changes are sent, and when it stops, every subscription ends
	r.events <- change("media", "/a.txt")
	assert.Equal(t, change("media", "/a.txt"), next(t, ch))
	cancel()
	assert.NoError(t, <-running)
	assert.ErrorIs(t, ended(t, done), fs.ErrClosed)
	assert.Equal(t, 0, r.Subscribers())
	assert.False(t, r.Running())
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer waitCancel()
	assert.ErrorIs(t, r.WaitListening(waitCtx), context.DeadlineExceeded)
}

func Test_Registry_Subscribe(t *testing.T) {
	r, _ := newRegistry(t)

	// A channel is needed, which is subscribed once at a time
	_, err := r.Subscribe(context.Background(), schema.EventFilter{}, nil)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	all, other := make(chan schema.Event, 10), make(chan schema.Event, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done, err := r.Subscribe(ctx, schema.EventFilter{}, all)
	require.NoError(t, err)
	_, err = r.Subscribe(context.Background(), schema.EventFilter{}, all)
	assert.ErrorIs(t, err, gofiler.ErrConflict)

	// The changes of the filter's volumes are sent, or every volume's
	otherDone, err := r.Subscribe(context.Background(), schema.EventFilter{Name: []string{"other"}}, other)
	require.NoError(t, err)
	r.events <- change("media", "/a.txt")
	r.events <- change("other", "/b.txt")
	assert.Equal(t, change("media", "/a.txt"), next(t, all))
	assert.Equal(t, change("other", "/b.txt"), next(t, all))
	assert.Equal(t, change("other", "/b.txt"), next(t, other))
	assert.Empty(t, other)

	// A subscription which is cancelled ends, without an error, and isn't
	// sent changes
	cancel()
	assert.NoError(t, ended(t, done))
	assert.Equal(t, 1, r.Subscribers())

	// Once the registry is closed, the subscriptions end, and can't be made
	require.NoError(t, r.Close())
	assert.ErrorIs(t, ended(t, otherDone), fs.ErrClosed)
	_, err = r.Subscribe(context.Background(), schema.EventFilter{}, make(chan schema.Event))
	assert.ErrorIs(t, err, fs.ErrClosed)
}

func Test_Registry_SubscribeSlow(t *testing.T) {
	r, _ := newRegistry(t)

	// A change which a subscriber's channel is too full for is dropped, as
	// are its volume's others, but not other volumes', and it's sent a rescan
	// of the volume, once there's room, even without another change
	ch := make(chan schema.Event, 1)
	_, err := r.Subscribe(context.Background(), schema.EventFilter{}, ch)
	require.NoError(t, err)
	r.events <- change("media", "/a.txt")
	r.events <- change("media", "/b.txt")
	require.Eventually(t, func() bool {
		r.RLock()
		defer r.RUnlock()
		return r.subscribers[ch].rescan["media"]
	}, 5*time.Second, time.Millisecond)
	assert.Equal(t, change("media", "/a.txt"), next(t, ch))
	assert.Equal(t, rescanEvent("media"), next(t, ch))

	// And then its changes are sent again
	r.events <- change("media", "/d.txt")
	assert.Equal(t, change("media", "/d.txt"), next(t, ch))
}
