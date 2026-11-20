package registry

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"time"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	gofiler "github.com/mutablelogic/go-filer"
	indexer "github.com/mutablelogic/go-filer/indexer"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// syncs are the volumes which Run syncs with the store, each of which is
// synced by one worker at a time, as it can be slow, such as a backend
// which signs in, so the others aren't held up
type syncs struct {
	ctx     context.Context // Run's, while it's running, or nil
	logger  *slog.Logger
	volumes map[string]*syncState
}

// syncState is a volume which is synced, or to be
type syncState struct {
	running bool          // A worker is syncing it
	pending bool          // It's synced again, once the worker has synced it
	force   bool          // It's opened again, even when it's mounted as it's stored
	retry   time.Duration // How long until it's opened again, when it can't be
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// sync syncs a volume with the store, while Run is running, or else when it
// runs, as it syncs every volume then. With force, a volume which is mounted
// as it's stored is opened again, such as one which is offline.
func (self *Registry) sync(name string, force bool) {
	self.syncMu.Lock()
	defer self.syncMu.Unlock()
	state, exists := self.syncs.volumes[name]
	if !exists {
		state = &syncState{}
		self.syncs.volumes[name] = state
	}
	state.pending, state.force = true, state.force || force
	if !state.running && self.syncs.ctx != nil {
		state.running = true
		self.workers.Add(1)
		go self.syncWorker(self.syncs.ctx, self.syncs.logger, name, state)
	}
}

// syncAll syncs the volumes of the store, and those which are mounted, as
// they can be deleted from it, or returns the error when the store can't be
// read
func (self *Registry) syncAll(ctx context.Context, logger *slog.Logger) error {
	names := map[string]bool{}
	if volumes, err := self.store.ListVolumes(ctx); err != nil {
		logger.ErrorContext(ctx, "the volumes can't be read from the store", "error", err.Error())
		return err
	} else {
		for _, volume := range volumes {
			names[volume.Name] = true
		}
	}
	for _, name := range self.Keys() {
		names[name] = true
	}
	for name := range names {
		self.sync(name, false)
	}
	return nil
}

// startSyncs lets the volumes be synced, by workers, which are cancelled
// with the context, and syncs those which were asked to be meanwhile
func (self *Registry) startSyncs(ctx context.Context, logger *slog.Logger) {
	self.syncMu.Lock()
	self.syncs.ctx, self.syncs.logger = ctx, logger
	var pending []string
	for name, state := range self.syncs.volumes {
		if state.pending {
			pending = append(pending, name)
		}
	}
	self.syncMu.Unlock()
	for _, name := range pending {
		self.sync(name, false)
	}
}

// stopSyncs stops the volumes being synced, and waits for the workers, whose
// context must be cancelled
func (self *Registry) stopSyncs() {
	self.syncMu.Lock()
	self.syncs.ctx = nil
	self.syncMu.Unlock()
	self.workers.Wait()
}

// syncWorker syncs the volume, until it doesn't need to be again
func (self *Registry) syncWorker(ctx context.Context, logger *slog.Logger, name string, state *syncState) {
	defer self.workers.Done()
	for {
		self.syncMu.Lock()
		if !state.pending || ctx.Err() != nil {
			state.running = false
			self.syncMu.Unlock()
			return
		}
		force := state.force
		state.pending, state.force = false, false
		self.syncMu.Unlock()
		self.syncVolume(ctx, logger, name, force)
	}
}

// syncVolume mounts the volume, when it's stored, and enabled, and isn't
// mounted as it's stored, or it's forced, or unmounts it, when it isn't
func (self *Registry) syncVolume(ctx context.Context, logger *slog.Logger, name string, force bool) {
	stored, err := self.store.GetVolume(ctx, name)
	if errors.Is(err, gofiler.ErrNotFound) {
		stored = nil
	} else if err != nil {
		logger.ErrorContext(ctx, "a volume can't be read from the store", "volume", name, "error", err.Error())
		self.retry(ctx, logger, name, err)
		return
	}

	// The volume's entry, which is mounted, or nil
	self.RLock()
	e := self.backend[name]
	if e != nil && e.state != stateReady {
		e = nil
	}
	current, offline, created := "", error(nil), time.Time{}
	if e != nil {
		current, offline, created = self.metaOf(name, e.Filer).URL, e.offline, e.created
	}
	self.RUnlock()

	// A volume which isn't stored, or enabled, is unmounted, and one which
	// is stored, but isn't indexed, doesn't keep an index, once its indexer
	// has stopped
	if stored == nil || !stored.Enabled {
		self.setFailed(name, nil)
		if e != nil {
			self.unmount(name, e)
			logger.InfoContext(ctx, "unmounted volume", "volume", name)
		}
		if stored != nil && indexOf(stored.URL) == indexer.ModeNone {
			self.forget(ctx, name)
		}
		return
	}

	// One which is mounted as it's stored, of the same stored volume, rather
	// than one which was deleted, and created again, is left as it is, unless
	// it's offline, and forced
	if e != nil && current == stored.URL && created.Equal(stored.Created) && (offline == nil || !force) {
		return
	}

	// Otherwise it's opened, from its URL, and mounted, in place of the one
	// which is mounted
	b, err := self.open(ctx, name, stored.URL)
	if err != nil {
		// It's logged as a warning when it's why it can't be mounted, for the
		// first time, rather than of each attempt, as it's retried
		if previous := self.whyFailed(name, e); previous != nil && previous.Error() == err.Error() {
			logger.DebugContext(ctx, "a volume can't be mounted, again", "volume", name, "error", err.Error())
		} else {
			logger.WarnContext(ctx, "a volume can't be mounted", "volume", name, "error", err.Error())
		}
		if e != nil {
			self.setOffline(e, err)
		} else {
			self.setFailed(name, err)
		}
		self.retry(ctx, logger, name, err)
		return
	}
	if err := self.mount(ctx, name, e, b, stored); err != nil {
		return
	}
	self.setFailed(name, nil)
	self.syncMu.Lock()
	if state, exists := self.syncs.volumes[name]; exists {
		state.retry = 0
	}
	self.syncMu.Unlock()
	logger.InfoContext(ctx, "mounted volume", "volume", name, "url", stored.URL)
}

// open creates the backend of a volume, from its stored URL, which must be
// the volume's, or of the opener, in tests
func (self *Registry) open(ctx context.Context, name, volumeURL string) (gofiler.Filer, error) {
	if self.opener != nil {
		return self.opener(ctx, name, volumeURL)
	}
	return self.openURL(ctx, name, volumeURL)
}

// openURL creates the backend of a volume, from its URL, which must be the
// volume's
func (self *Registry) openURL(ctx context.Context, name, volumeURL string) (gofiler.Filer, error) {
	u, err := url.Parse(volumeURL)
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("invalid url of volume %q", name)
	}
	b, err := self.new(ctx, u)
	if err != nil {
		return nil, err
	} else if VolumeName(b.URL()) != name {
		return nil, errors.Join(gofiler.ErrBadParameter.Withf("url %q is not for volume %q", b.URL(), name), closeFiler(b))
	}
	return b, nil
}

// mount registers the backend of the stored volume, in place of the entry,
// which is mounted, or nil, which is closed first, keeping the volume's
// name. A volume which isn't indexed, or whose backend is another, doesn't
// keep its index. Returns
// ErrServiceUnavailable, closing the backend, when the registry is closed.
func (self *Registry) mount(ctx context.Context, name string, old *entry, b gofiler.Filer, stored *schema.Volume) error {
	pending := &entry{closed: make(chan struct{}), created: stored.Created}
	self.Lock()
	for {
		// A volume which is being closed keeps its name until it's closed,
		// so what it used, such as its directory, is released first
		if self.backend == nil {
			self.Unlock()
			return errors.Join(gofiler.ErrServiceUnavailable.With("registry is closed"), closeFiler(b))
		} else if closing, exists := self.backend[name]; exists && closing != old && closing.state == stateClosing {
			self.Unlock()
			<-closing.closed
			self.Lock()
			continue
		}
		break
	}
	// The index of the backend which is mounted is removed, once it's
	// closed, when it isn't indexed any more, or the backend is another
	_, indexed := b.(*indexer.Indexer)
	if old != nil && self.backend[name] == old && old.state == stateReady {
		_, wasIndexed := old.Filer.(*indexer.Indexer)
		old.forget = wasIndexed && (!indexed || !sameBackend(self.metaOf(name, old.Filer).URL, stored.URL))
		self.startClose(old, name)
	} else {
		old = nil
	}
	self.backend[name] = pending
	self.Unlock()
	if old != nil {
		<-old.closed
	} else if !indexed {
		// A volume which isn't indexed doesn't keep an index, of when it was
		// indexed, before it was unmounted
		self.forget(ctx, name)
	}
	if _, err := self.register(pending, name, b); err != nil {
		self.unreserve(pending, name)
		return err
	}
	return nil
}

// unmount closes the volume's entry, which is mounted, and waits for it
func (self *Registry) unmount(name string, e *entry) {
	self.Lock()
	if self.backend != nil && self.backend[name] == e && e.state == stateReady {
		self.startClose(e, name)
	}
	self.Unlock()
	<-e.closed
}

// retry syncs the volume again, after a while, which doubles each time,
// forcing it to be opened again, unless its user needs to sign in again,
// which only they can do, by updating it
func (self *Registry) retry(ctx context.Context, logger *slog.Logger, name string, err error) {
	if authclient.SignInOf(err) != nil {
		logger.WarnContext(ctx, "a volume is offline, until its user signs in again, with volume-update", "volume", name)
		return
	}
	self.syncMu.Lock()
	state, exists := self.syncs.volumes[name]
	if !exists {
		self.syncMu.Unlock()
		return
	}
	state.retry = min(max(watchRetry, state.retry*2), watchRetryMax)
	delay := state.retry
	self.syncMu.Unlock()
	time.AfterFunc(delay, func() {
		if ctx.Err() == nil {
			self.sync(name, true)
		}
	})
}

// whyFailed returns why the volume of the entry is offline, or, without
// one, why it can't be mounted, or nil
func (self *Registry) whyFailed(name string, e *entry) error {
	self.RLock()
	defer self.RUnlock()
	if e != nil {
		return e.offline
	}
	return self.failed[name]
}

// setFailed records why the volume can't be mounted, or that it can, with
// nil
func (self *Registry) setFailed(name string, err error) {
	self.Lock()
	defer self.Unlock()
	if err == nil {
		delete(self.failed, name)
	} else {
		self.failed[name] = err
	}
}
