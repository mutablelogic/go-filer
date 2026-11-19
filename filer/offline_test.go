package filer

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
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
		b, err := r.openURL(ctx, name, volumeURL)
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
