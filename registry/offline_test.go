package registry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// failing is the failure of a volume's backends, whose watches fail with
// its error, and which can't be opened again, once they're opened, until
// it's nil
type failing struct {
	err     atomic.Pointer[error]
	opens   atomic.Int32
	watches atomic.Int32
}

func (f *failing) failWith(err error) { f.err.Store(&err) }

func (f *failing) failure() error {
	if err := f.err.Load(); err != nil {
		return *err
	}
	return nil
}

// failingFiler is a backend whose watch fails with its failure
type failingFiler struct {
	gofiler.Filer
	f *failing
}

// Close closes the backend, which the registry closes, when it's replaced,
// or closed, as Close isn't of the Filer interface, which it embeds
func (f *failingFiler) Close() error {
	return closeFiler(f.Filer)
}

func (f *failingFiler) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	f.f.watches.Add(1)
	if err := f.f.failure(); err != nil {
		return err
	}
	return f.Filer.WatchObjects(ctx, ch)
}

// addFailing adds a volume, of a directory, whose backends fail with the
// error: its watch fails, and it can't be opened again, until it's nil. Its
// first backend is opened, so it's mounted, and then goes offline.
func addFailing(t *testing.T, r *Registry, dir, volume string, err error) *failing {
	t.Helper()
	f := &failing{}
	f.failWith(err)
	r.opener = func(ctx context.Context, name, volumeURL string) (gofiler.Filer, error) {
		b, err := r.openURL(ctx, name, volumeURL, time.Time{})
		if err != nil || name != volume {
			return b, err
		}
		if f.opens.Add(1) > 1 {
			if err := f.failure(); err != nil {
				return nil, errors.Join(err, closeFiler(b))
			}
		}
		return &failingFiler{Filer: b, f: f}, nil
	}
	_, e := add(t, r, volume, fileURL(t, dir, volume))
	require.NoError(t, e)
	return f
}

// offline returns why the volume is offline, or empty, which is empty too
// while it's being opened again, when it isn't found
func offline(t *testing.T, r *Registry, volume string) string {
	t.Helper()
	r.RLock()
	defer r.RUnlock()
	if e, exists := r.backend[volume]; exists && e.offline != nil {
		return e.offline.Error()
	}
	return ""
}

// online returns true when the volume is ready, and isn't offline
func online(r *Registry, volume string) bool {
	r.RLock()
	defer r.RUnlock()
	e, exists := r.backend[volume]
	return exists && e.state == stateReady && e.offline == nil
}

func Test_Registry_OfflineSignIn(t *testing.T) {
	retry := watchRetry
	watchRetry = 10 * time.Millisecond
	t.Cleanup(func() { watchRetry = retry })

	// A volume whose watch fails, as its user needs to sign in again, is
	// offline, with why, and isn't opened again
	r, dir := newRegistry(t)
	signIn := authclient.NewSignInError(authclient.SignIn{ClientID: "client-1", Credential: "GOOGLE_TOKEN"}, gofiler.ErrNotAuthorized.With("sign-in is revoked"))
	f := addFailing(t, r, dir, "tasks", signIn)
	require.Eventually(t, func() bool { return offline(t, r, "tasks") != "" }, 5*time.Second, time.Millisecond)
	assert.Contains(t, offline(t, r, "tasks"), "sign-in is revoked")
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), f.opens.Load(), "it isn't opened again")

	// Nor is it watched again, when another volume is added
	watches := f.watches.Load()
	_, err := add(t, r, "other", fileURL(t, dir, "other"))
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, watches, f.watches.Load(), "it isn't watched again")
	assert.NotEmpty(t, offline(t, r, "tasks"))

	// It's still listed, and used, offline
	v, err := r.GetVolume(context.Background(), "tasks")
	require.NoError(t, err)
	assert.False(t, v.Enabled)
	assert.NotEmpty(t, v.Error)

	// Updating it, once its user has signed in, opens it again, which isn't
	// offline
	f.failWith(nil)
	volume, err := update(t, r, "tasks", nil)
	require.NoError(t, err)
	assert.True(t, volume.Enabled)
	assert.Empty(t, volume.Error)
	assert.Empty(t, offline(t, r, "tasks"))
}

func Test_Registry_OfflineRetry(t *testing.T) {
	retry := watchRetry
	watchRetry = 10 * time.Millisecond
	t.Cleanup(func() { watchRetry = retry })

	// A volume whose watch fails otherwise, such as when its server is down,
	// is offline, with why, and is opened again, until it can be, when it
	// isn't offline
	r, dir := newRegistry(t)
	f := addFailing(t, r, dir, "down", gofiler.ErrServiceUnavailable.With("server is down"))
	require.Eventually(t, func() bool { return offline(t, r, "down") != "" }, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return f.opens.Load() > 2 }, 5*time.Second, time.Millisecond, "it's opened again, until it can be")
	assert.Contains(t, offline(t, r, "down"), "server is down")
	f.failWith(nil)
	require.Eventually(t, func() bool { return online(r, "down") }, 5*time.Second, time.Millisecond)

	// Once it's opened again, it needs a sign-in, when it stays offline
	r2, dir2 := newRegistry(t)
	g := addFailing(t, r2, dir2, "tasks", gofiler.ErrServiceUnavailable.With("server is down"))
	require.Eventually(t, func() bool { return g.opens.Load() > 1 }, 5*time.Second, time.Millisecond)
	g.failWith(authclient.NewSignInError(authclient.SignIn{ClientID: "client-1"}, gofiler.ErrNotAuthorized.With("sign in again")))
	require.Eventually(t, func() bool { return strings.Contains(offline(t, r2, "tasks"), "sign in again") }, 5*time.Second, time.Millisecond)
	opens := g.opens.Load()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, opens, g.opens.Load(), "it stops opening it again")
}

// logBuffer is a buffer of logs, which is written from more than one
// goroutine
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// count returns how many lines have all the strings
func (b *logBuffer) count(strs ...string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for _, line := range strings.Split(b.buf.String(), "\n") {
		matches := line != ""
		for _, s := range strs {
			matches = matches && strings.Contains(line, s)
		}
		if matches {
			n++
		}
	}
	return n
}

func Test_Registry_OfflineLogs(t *testing.T) {
	retry := watchRetry
	watchRetry = 10 * time.Millisecond
	t.Cleanup(func() { watchRetry = retry })

	// A volume which is offline, and can't be opened again, of the same
	// reason, is logged as a warning once, rather than of each attempt,
	// which is logged for debugging, until it's mounted again
	// The directory is made first, so it's removed after the registry is
	// closed, as cleanups run in reverse
	dir := t.TempDir()
	r, err := New(context.Background(), WithDebounce(0), WithBroadcaster(newBroadcaster(t)), WithStore(newStore(t)))
	require.NoError(t, err)
	t.Cleanup(func() { r.close() })
	var logs logBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- r.Run(ctx, slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	require.Eventually(t, r.Running, 5*time.Second, time.Millisecond)

	f := addFailing(t, r, dir, "down", gofiler.ErrServiceUnavailable.With("server is down"))
	require.Eventually(t, func() bool { return f.opens.Load() > 4 }, 5*time.Second, time.Millisecond)
	assert.Equal(t, 1, logs.count("level=WARN", "watching a volume failed"))
	assert.Zero(t, logs.count("level=WARN", "can't be mounted"), "it's of the reason it's offline")
	assert.GreaterOrEqual(t, logs.count("level=DEBUG", "can't be mounted, again"), 3)

	// A reason which is new is a warning, once
	f.failWith(gofiler.ErrServiceUnavailable.With("server is still down"))
	require.Eventually(t, func() bool { return logs.count("level=WARN", "can't be mounted", "still down") == 1 }, 5*time.Second, time.Millisecond)
	opens := f.opens.Load()
	require.Eventually(t, func() bool { return f.opens.Load() > opens+2 }, 5*time.Second, time.Millisecond)
	assert.Equal(t, 1, logs.count("level=WARN", "can't be mounted", "still down"))

	// And it's mounted again, which is logged
	f.failWith(nil)
	require.Eventually(t, func() bool { return online(r, "down") }, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return logs.count("level=INFO", "mounted volume", "volume=down") >= 2 }, 5*time.Second, time.Millisecond)
}

func Test_Registry_StateChanges(t *testing.T) {
	retry := watchRetry
	watchRetry = 10 * time.Millisecond
	t.Cleanup(func() { watchRetry = retry })

	// What the registry knows of a volume is published as it changes: when
	// it's mounted, when it's offline, and when it's mounted again, but not
	// of each attempt which fails for the same reason
	dir := t.TempDir()
	b := newBroadcaster(t)
	r, err := New(context.Background(), WithDebounce(0), WithBroadcaster(b), WithStore(newStore(t)))
	require.NoError(t, err)
	t.Cleanup(func() { r.close() })
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan changes.Change, 100)
	_, err = b.Watch(ctx, changes.ChangeFilter{Schema: schema.RegistryChangeSchema, Table: schema.VolumeChangeTable}, ch)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx, nil) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	require.Eventually(t, r.Running, 5*time.Second, time.Millisecond)
	var mu sync.Mutex
	var rows []string
	go func() {
		for change := range ch {
			if change.Row != "" {
				mu.Lock()
				rows = append(rows, change.Row)
				mu.Unlock()
			}
		}
	}()
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(rows)
	}

	// It's mounted, and then goes offline, when its watch fails, and isn't
	// published again of each attempt to open it again, which fails
	f := addFailing(t, r, dir, "down", gofiler.ErrServiceUnavailable.With("server is down"))
	require.Eventually(t, func() bool { return f.opens.Load() > 4 }, 5*time.Second, time.Millisecond)
	offline := count()
	assert.GreaterOrEqual(t, offline, 2, "it's mounted, and offline")
	opens := f.opens.Load()
	require.Eventually(t, func() bool { return f.opens.Load() > opens+3 }, 5*time.Second, time.Millisecond)
	assert.Equal(t, offline, count(), "of the same reason")

	// And it's mounted again
	f.failWith(nil)
	require.Eventually(t, func() bool { return online(r, "down") }, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return count() > offline }, 5*time.Second, time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	for _, row := range rows {
		assert.Equal(t, "down", row)
	}
}
