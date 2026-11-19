package registry

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"testing"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	gosqlite "github.com/mutablelogic/go-sqlite"
	require "github.com/stretchr/testify/require"
)

// newStore returns a store of a SQLite database in memory, which is closed
// when the test ends
func newStore(t *testing.T) *sqlite.Store {
	t.Helper()
	s, err := sqlite.New(context.Background(), gosqlite.Memory)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

// mountTimeout is how long a test waits for Run to mount, or unmount, a
// volume
const mountTimeout = 10 * time.Second

// add creates a volume of the URL, enabled, and waits for Run to mount it,
// returning its backend, or why it can't be created, or mounted
func add(t *testing.T, r *Registry, name string, u *url.URL) (gofiler.Filer, error) {
	t.Helper()
	rawURL := ""
	if u != nil {
		rawURL = u.String()
	}
	if _, err := r.CreateVolume(context.Background(), schema.VolumeMeta{Name: name, URL: rawURL, Enabled: true}); err != nil {
		return nil, err
	}
	return mounted(t, r, name, nil)
}

// mounted waits for Run to mount the volume as it's stored, in place of the
// entry, when it isn't nil, and returns its backend, or why it can't be
// mounted
func mounted(t *testing.T, r *Registry, name string, old *entry) (gofiler.Filer, error) {
	t.Helper()
	deadline := time.Now().Add(mountTimeout)
	for time.Now().Before(deadline) {
		stored, err := r.store.GetVolume(context.Background(), name)
		if err != nil {
			return nil, err
		}
		r.RLock()
		e := r.backend[name]
		failed := r.failed[name]
		var b gofiler.Filer
		if e != nil && e.state == stateReady && e != old && r.metaOf(name, e.Filer).URL == stored.URL {
			b = e.Filer
		}
		r.RUnlock()
		if b != nil {
			return b, nil
		} else if failed != nil {
			return nil, failed
		}
		time.Sleep(time.Millisecond)
	}
	return nil, gofiler.ErrServiceUnavailable.Withf("volume %q isn't mounted", name)
}

// del deletes a volume, and waits for Run to unmount it, and close its
// backend, returning why it couldn't be closed, or nil when it isn't stored
func del(t *testing.T, r *Registry, name string) error {
	t.Helper()
	r.RLock()
	e := r.backend[name]
	r.RUnlock()
	if _, err := r.DeleteVolume(context.Background(), name); errors.Is(err, gofiler.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	if e == nil {
		return nil
	}
	select {
	case <-e.closed:
		return e.err
	case <-time.After(mountTimeout):
		return gofiler.ErrServiceUnavailable.Withf("volume %q isn't unmounted", name)
	}
}

// update patches the options of a volume's URL, and waits for Run to mount
// it again, when it changes, returning the volume, or why it can't be
// updated, or mounted
func update(t *testing.T, r *Registry, name string, patch url.Values) (*schema.Volume, error) {
	t.Helper()
	ctx := context.Background()
	volume, err := r.GetVolume(ctx, name)
	if err != nil {
		return nil, err
	}
	u, err := PatchURL(volume.URL, patch)
	if err != nil {
		return nil, err
	}
	r.RLock()
	old := r.backend[name]
	offline := old != nil && old.offline != nil
	r.RUnlock()
	updated, err := r.UpdateVolume(ctx, schema.VolumeMeta{Name: name, URL: u, Enabled: true})
	if err != nil {
		return nil, err
	}
	if updated.URL == volume.URL && !offline {
		old = nil
	}
	if _, err := mounted(t, r, name, old); err != nil {
		return nil, err
	}
	return r.GetVolume(ctx, name)
}

// volumes returns the names of the volumes which are stored
func volumes(t *testing.T, r *Registry) []string {
	t.Helper()
	list, err := r.ListVolumes(context.Background())
	require.NoError(t, err)
	result := []string{}
	for _, volume := range list {
		result = append(result, volume.Name)
	}
	slices.Sort(result)
	return result
}

// validate returns the URL of a volume, of its name, once its backend is
// created from it, which checks it, and closed
func validate(r *Registry, u *url.URL) (*url.URL, error) {
	if u == nil {
		return nil, gofiler.ErrBadParameter.With("url is nil")
	}
	meta, err := r.validate(context.Background(), schema.VolumeMeta{Name: VolumeName(u), URL: u.String()})
	if err != nil {
		return nil, err
	}
	return url.Parse(meta.URL)
}

// withIndex returns a URL, with its index option
func withIndex(t *testing.T, u *url.URL, mode string) string {
	t.Helper()
	query := u.Query()
	query.Set("index", mode)
	result := *u
	result.RawQuery = query.Encode()
	return result.String()
}
