package filer

import (
	"context"
	"net/url"
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

// failingFiler is a backend whose watch fails with its error, and whose
// clones fail with it too, until it's nil, when they're the backend's
type failingFiler struct {
	gofiler.Filer
	err    atomic.Pointer[error]
	clones atomic.Int32
}

func (f *failingFiler) failWith(err error) { f.err.Store(&err) }

func (f *failingFiler) failure() error {
	if err := f.err.Load(); err != nil {
		return *err
	}
	return nil
}

func (f *failingFiler) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	if err := f.failure(); err != nil {
		return err
	}
	return f.Filer.WatchObjects(ctx, ch)
}

func (f *failingFiler) Clone(ctx context.Context, patch url.Values) (gofiler.Filer, error) {
	f.clones.Add(1)
	if err := f.failure(); err != nil {
		return nil, err
	}
	return f.Filer.Clone(ctx, patch)
}

// addFailing registers a volume, of a directory, whose backend fails with
// the error
func addFailing(t *testing.T, r *Registry, dir, volume string, err error) *failingFiler {
	t.Helper()
	b, e := r.new(context.Background(), fileURL(t, dir, volume))
	require.NoError(t, e)
	f := &failingFiler{Filer: b}
	f.failWith(err)
	pending := &entry{closed: make(chan struct{})}
	r.Lock()
	r.backend[volume] = pending
	r.Unlock()
	_, e = r.register(pending, volume, f)
	require.NoError(t, e)
	return f
}

// offline returns why the volume is offline, or empty
func offline(t *testing.T, r *Registry, volume string) string {
	t.Helper()
	v, err := r.Volume(volume)
	require.NoError(t, err)
	return v.Offline
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
	assert.Zero(t, f.clones.Load(), "it isn't opened again")

	// It's still listed, and used, offline
	list := r.Volumes()
	require.Len(t, list, 1)
	assert.NotEmpty(t, list[0].Offline)

	// Updating it, once its user has signed in, opens it again, which isn't
	// offline
	f.failWith(nil)
	volume, err := r.Update(context.Background(), "tasks", nil)
	require.NoError(t, err)
	assert.Empty(t, volume.Offline)
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
	require.Eventually(t, func() bool { return f.clones.Load() > 1 }, 5*time.Second, time.Millisecond, "it's opened again, until it can be")
	assert.Contains(t, offline(t, r, "down"), "server is down")
	f.failWith(nil)
	require.Eventually(t, func() bool { return offline(t, r, "down") == "" }, 5*time.Second, time.Millisecond)

	// Once it's opened again, it needs a sign-in, when it stays offline
	r2, dir2 := newRegistry(t)
	g := addFailing(t, r2, dir2, "tasks", gofiler.ErrServiceUnavailable.With("server is down"))
	require.Eventually(t, func() bool { return g.clones.Load() > 0 }, 5*time.Second, time.Millisecond)
	g.failWith(authclient.NewSignInError(authclient.SignIn{ClientID: "client-1"}, gofiler.ErrNotAuthorized.With("sign in again")))
	require.Eventually(t, func() bool { return strings.Contains(offline(t, r2, "tasks"), "sign in again") }, 5*time.Second, time.Millisecond)
	clones := g.clones.Load()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, clones, g.clones.Load(), "it stops opening it again")
}
