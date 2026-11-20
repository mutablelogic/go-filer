package registry

import (
	"context"
	"errors"
	"net/url"
	"sync"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/indexer"
	schema "github.com/mutablelogic/go-filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// CreateVolume stores a volume, which is mounted by Run, when it's enabled,
// and returns it, before it's mounted. Its backend is created from its URL
// first, which checks it, and then closed, and its URL is the backend's, with
// each of its options, defaults too, and its index option. Returns
// ErrConflict when the volume is stored already, and ErrBadParameter when its
// name isn't an identifier, or its URL's, or the backend can't be created.
func (self *Registry) CreateVolume(ctx context.Context, volume schema.VolumeMeta) (*schema.Volume, error) {
	// The store creates it once, of the registries which create it at once,
	// and checking it exists first saves validating a volume which does
	if _, err := self.store.GetVolume(ctx, volume.Name); err == nil {
		return nil, gofiler.ErrConflict.Withf("volume already exists: %q", volume.Name)
	} else if !errors.Is(err, gofiler.ErrNotFound) {
		return nil, err
	}
	meta, err := self.validate(ctx, volume)
	if err != nil {
		return nil, err
	}
	stored, err := self.store.CreateVolume(ctx, meta)
	if err != nil {
		return nil, err
	}
	self.sync(meta.Name, false)
	return self.view(ctx, stored), nil
}

// UpdateVolume stores a volume's URL, and whether it's enabled, which Run
// mounts again, or unmounts, and returns it, before it's mounted. Its
// backend is created from its URL first, which checks it, as CreateVolume
// does. Its scheme can't be changed, which is ErrBadParameter. A volume
// whose backend is another, of its host or path, doesn't keep its index. A volume which is offline is opened again, even
// when it isn't changed, such as once its user has signed in again. Returns
// ErrNotFound when the volume isn't stored, or is deleted meanwhile.
func (self *Registry) UpdateVolume(ctx context.Context, volume schema.VolumeMeta) (*schema.Volume, error) {
	current, err := self.store.GetVolume(ctx, volume.Name)
	if err != nil {
		return nil, err
	}

	// Its scheme is what the volume is, which isn't changed
	if from, to := schemeOf(current.URL), schemeOf(volume.URL); from != to {
		return nil, gofiler.ErrBadParameter.Withf("volume %q is of %s, so it can't be updated to %s", volume.Name, from, to)
	}
	meta, err := self.validate(ctx, volume)
	if err != nil {
		return nil, err
	}
	stored, err := self.store.UpdateVolume(ctx, meta)
	if err != nil {
		return nil, err
	}

	// The index of another backend is removed now, in case the volume isn't
	// mounted, and once the backend which is mounted is closed, in case its
	// indexer writes to it meanwhile
	if !sameBackend(current.URL, stored.URL) {
		self.forget(ctx, stored.Name)
	}
	self.sync(meta.Name, true)
	return self.view(ctx, stored), nil
}

// GetVolume returns a volume of the store, with its index, when it's
// mounted and indexed, and why it isn't enabled, when it's enabled but
// offline, or can't be mounted. Returns ErrNotFound when it isn't stored.
func (self *Registry) GetVolume(ctx context.Context, name string) (*schema.Volume, error) {
	stored, err := self.store.GetVolume(ctx, name)
	if err != nil {
		return nil, err
	}
	return self.view(ctx, stored), nil
}

// ListVolumes returns the volumes of the store, in order of their names, as
// GetVolume returns them, which includes those which aren't enabled. Their
// indexes' states are read at once, a few at a time, as each is a query of
// the store.
func (self *Registry) ListVolumes(ctx context.Context) ([]schema.Volume, error) {
	stored, err := self.store.ListVolumes(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]schema.Volume, len(stored))
	var wg sync.WaitGroup
	limit := make(chan struct{}, viewConcurrency)
	for i, volume := range stored {
		wg.Go(func() {
			limit <- struct{}{}
			defer func() { <-limit }()
			result[i] = *self.view(ctx, volume)
		})
	}
	wg.Wait()
	return result, nil
}

// viewConcurrency is how many volumes' indexes' states are read at once
const viewConcurrency = 8

// DeleteVolume removes a volume from the store, with its index, which Run
// unmounts, and returns it, as it was, before it's unmounted. Returns
// ErrNotFound when it isn't stored.
func (self *Registry) DeleteVolume(ctx context.Context, name string) (*schema.Volume, error) {
	volume, err := self.GetVolume(ctx, name)
	if err != nil {
		return nil, err
	}
	if err := self.store.DeleteVolume(ctx, name); err != nil {
		return nil, err
	}
	self.sync(name, false)
	return volume, nil
}

// PatchURL returns a volume's URL, with the patch of its options: each key
// replaces the option's values with its own, or, when it has none, removes
// the option, so it's the default (see gofiler.PatchValues)
func PatchURL(volumeURL string, patch url.Values) (string, error) {
	u, err := url.Parse(volumeURL)
	if err != nil {
		return "", gofiler.ErrBadParameter.Withf("invalid url: %v", err)
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", gofiler.ErrBadParameter.Withf("invalid options: %v", err)
	}
	u.RawQuery = gofiler.PatchValues(query, patch).Encode()
	return u.String(), nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// sameBackend returns true when the URLs are of the same backend, of their
// scheme, host and path, whatever their options, so its index is kept
func sameBackend(a, b string) bool {
	ua, errA := url.Parse(a)
	ub, errB := url.Parse(b)
	if errA != nil || errB != nil {
		return false
	}
	return ua.Scheme == ub.Scheme && ua.Host == ub.Host && ua.Path == ub.Path
}

// schemeOf returns the scheme of a URL, or empty, when it can't be parsed
func schemeOf(volumeURL string) string {
	if u, err := url.Parse(volumeURL); err == nil {
		return u.Scheme
	}
	return ""
}

// indexOf returns what's indexed of a volume, of its stored URL
func indexOf(volumeURL string) indexer.Mode {
	u, err := url.Parse(volumeURL)
	if err != nil {
		return indexer.ModeNone
	}
	if mode, err := indexer.ParseMode(u.Query().Get(indexer.IndexParam)); err == nil {
		return mode
	}
	return indexer.ModeNone
}

// validate returns what's stored of a volume, once its backend is created
// from its URL, which checks it, and closed: its URL is the backend's, with
// each of its options, and its index option
func (self *Registry) validate(ctx context.Context, volume schema.VolumeMeta) (schema.VolumeMeta, error) {
	if !types.IsIdentifier(volume.Name) {
		return volume, gofiler.ErrBadParameter.Withf("invalid volume name: %q", volume.Name)
	} else if volume.URL == "" {
		return volume, gofiler.ErrBadParameter.Withf("volume %q has no url", volume.Name)
	}
	u, err := url.Parse(volume.URL)
	if err != nil {
		return volume, gofiler.ErrBadParameter.Withf("invalid url of volume %q", volume.Name)
	}
	b, err := self.new(ctx, u)
	if err != nil {
		return volume, err
	}
	meta := self.metaOf(volume.Name, b)
	meta.Enabled = volume.Enabled
	if name := VolumeName(b.URL()); name != volume.Name {
		err = gofiler.ErrBadParameter.Withf("url %q is not for volume %q", b.URL(), volume.Name)
	}
	return meta, errors.Join(err, closeFiler(b))
}

// view returns a stored volume, with what the registry knows of it: whether
// it's enabled, which it isn't while it's offline, or can't be mounted, with
// why, and its index's state, when it's mounted and indexed, which is read
// of the context, so it's traced as part of the request
func (self *Registry) view(ctx context.Context, stored *schema.Volume) *schema.Volume {
	volume := *stored
	if !volume.Enabled {
		return &volume
	}

	// What the registry knows of it, which is read with it locked
	var backend gofiler.Filer
	var offline error
	self.RLock()
	if e, mounted := self.backend[volume.Name]; mounted && e.state == stateReady {
		backend, offline = e.Filer, e.offline
	} else {
		offline = self.failed[volume.Name]
	}
	self.RUnlock()
	if offline != nil {
		volume.Enabled, volume.Error = false, offline.Error()
	}
	volume.Mounted = backend != nil
	if backend != nil {
		if i, ok := backend.(*indexer.Indexer); ok {
			volume.Index = &schema.VolumeIndex{Mode: string(i.Mode())}
			if state, err := i.State(ctx); err != nil {
				volume.Index.Error = err.Error()
			} else {
				volume.Index.IndexState = state
			}
			if task, err := i.Metadata(ctx); err != nil && volume.Index.Error == "" {
				volume.Index.Error = err.Error()
			} else {
				volume.Index.Metadata = task
			}
		}
	}
	return &volume
}
