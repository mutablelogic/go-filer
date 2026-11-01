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

func Test_Broadcaster_Subscribe(t *testing.T) {
	b := newBroadcaster()

	// A channel is needed, which is subscribed once at a time
	_, err := b.subscribe(context.Background(), nil)
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	all, other := make(chan schema.Event, 10), make(chan schema.Event, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done, err := b.subscribe(ctx, all)
	require.NoError(t, err)
	_, err = b.subscribe(context.Background(), all)
	assert.ErrorIs(t, err, gofiler.ErrConflict)

	// The changes are sent to each subscription
	otherDone, err := b.subscribe(context.Background(), other)
	require.NoError(t, err)
	assert.Equal(t, 2, b.len())
	assert.Zero(t, b.publish(change("media", "/a.txt")))
	assert.Zero(t, b.publish(change("other", "/b.txt")))
	for _, ch := range []chan schema.Event{all, other} {
		assert.Equal(t, change("media", "/a.txt"), next(t, ch))
		assert.Equal(t, change("other", "/b.txt"), next(t, ch))
	}

	// A subscription which is cancelled ends, without an error
	cancel()
	assert.NoError(t, ended(t, done))
	require.Eventually(t, func() bool { return b.len() == 1 }, 5*time.Second, time.Millisecond)

	// Ending the subscriptions ends them, with fs.ErrClosed, and later ones
	// are made, unless it's final
	b.end(false)
	assert.ErrorIs(t, ended(t, otherDone), fs.ErrClosed)
	done, err = b.subscribe(context.Background(), all)
	require.NoError(t, err)
	b.end(true)
	assert.ErrorIs(t, ended(t, done), fs.ErrClosed)
	_, err = b.subscribe(context.Background(), all)
	assert.ErrorIs(t, err, fs.ErrClosed)
}

func Test_Broadcaster_Slow(t *testing.T) {
	b := newBroadcaster()

	// A change which a subscriber's channel is too full for is dropped, as
	// are its volume's others, but not other volumes', and it's sent a rescan
	// of the volume, once there's room, when it's flushed, or with another
	// change
	ch := make(chan schema.Event, 1)
	_, err := b.subscribe(context.Background(), ch)
	require.NoError(t, err)
	assert.Zero(t, b.publish(change("media", "/a.txt")))
	assert.Equal(t, 1, b.publish(change("media", "/b.txt")))
	assert.Equal(t, 1, b.publish(change("other", "/c.txt")))
	assert.Equal(t, change("media", "/a.txt"), next(t, ch))
	b.flush()
	assert.Equal(t, rescanEvent("media"), next(t, ch))

	// Another change sends the rescan which is owed first, so when that fills
	// the channel, the change is dropped, and its volume is owed one again
	assert.Equal(t, 1, b.publish(change("media", "/d.txt")))
	assert.Equal(t, rescanEvent("other"), next(t, ch))
	b.flush()
	assert.Equal(t, rescanEvent("media"), next(t, ch))
	assert.Zero(t, b.publish(change("media", "/e.txt")))
	assert.Equal(t, change("media", "/e.txt"), next(t, ch))
}

func Test_Broadcaster_Watch(t *testing.T) {
	b := newBroadcaster()

	// Watching waits for the channel to receive each change, until it's
	// cancelled, or the subscriptions end
	ch, done := make(chan schema.Event), make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- b.watch(ctx, ch, 10) }()
	require.Eventually(t, func() bool { return b.len() == 1 }, 5*time.Second, time.Millisecond)
	b.publish(change("media", "/a.txt"))
	b.publish(change("media", "/b.txt"))
	assert.Equal(t, change("media", "/a.txt"), next(t, ch))
	assert.Equal(t, change("media", "/b.txt"), next(t, ch))
	cancel()
	assert.NoError(t, ended(t, done))
	go func() { done <- b.watch(context.Background(), ch, 10) }()
	require.Eventually(t, func() bool { return b.len() == 1 }, 5*time.Second, time.Millisecond)
	b.end(true)
	assert.ErrorIs(t, ended(t, done), fs.ErrClosed)
}
