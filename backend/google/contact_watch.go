package google

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"slices"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	vcard "github.com/mutablelogic/go-metadata/pkg/vcard"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// contactWatch is what a watch knows: the token of the changes after what
// it knows, which only the watch uses, and, which the filer is locked for,
// as its writes change them too, the contacts, by their ids, whether they
// changed since they were saved, and when they were, and the events of the
// filer's writes, which the watch hasn't sent, and the signal that there
// are some
type contactWatch struct {
	token   string
	people  map[string]httpclient.Person
	dirty   bool
	saved   time.Time
	pending []schema.Event
	signal  chan struct{}
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// contactsKey is the key of the contacts a watch knows, in the volume's
	// cache, whose version is the token of the changes after them, and
	// contactsFormat the format of its content, one contact of JSON a line
	contactsKey    = "contacts"
	contactsFormat = "1"

	// photoKey is the prefix of the keys of the photos, in the volume's
	// cache, of their links, which are kept for photoTTL, and photoType the
	// value of their media type
	photoKey  = "photo/"
	photoTTL  = 24 * time.Hour
	photoType = "type"
)

// contactSaveInterval is the least time between the saves of the contacts
// a watch knows, while they change, which is a variable, so a test can set it
var contactSaveInterval = 5 * time.Minute

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WatchObjects syncs the contacts, at the refresh interval, of the token of
// the changes since it last did, and sends what changed, until the context
// is cancelled: a contact which is created, or whose entity tag changes, or
// which is deleted. While it's watched, the filer reads the contacts it
// knows, rather than getting them again.
//
// The contacts, and their token, are saved in the volume's cache, so a
// watch which starts again, such as after a restart, syncs what changed
// since, rather than getting every contact. The first change is a rescan
// of the root, once they're known. When Google doesn't accept the token,
// such as one which is more than seven days old, every contact is got
// again, and the root is rescanned. When Google is unavailable, it's synced
// again at the next interval. One watch of a filer is at a time, and
// another is ErrConflict. Returns ErrNotImplemented when the refresh
// interval is zero, and the error of reading them, such as a SignInError,
// when a sign-in is needed.
// Reference: https://developers.google.com/people/v1/contacts#sync_the_users_contacts
func (self *ContactFiler) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	if self.refreshInterval == 0 {
		return gofiler.ErrNotImplemented.Withf("volume %q isn't watched, as its %s is zero", self.name, paramRefreshInterval)
	}
	send := func(event schema.Event) error {
		select {
		case ch <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	w := &contactWatch{signal: make(chan struct{}, 1)}
	if err := self.watch(w); err != nil {
		return err
	}
	defer self.unwatch(w)

	// The contacts which were saved, and what changed since, or every
	// contact, when there are none, or the token isn't accepted
	restored := self.restoreContacts(ctx, w)
	if restored {
		if _, err := self.syncContacts(ctx, w); errors.Is(err, httpclient.ErrSyncTokenGone) {
			restored = false
		} else if err != nil && !errors.Is(err, gofiler.ErrServiceUnavailable) {
			return err
		}
	}
	if !restored {
		if err := self.resyncContacts(ctx, w); err != nil {
			return err
		}
	}
	defer self.saveContacts(context.WithoutCancel(ctx), w, true)
	self.saveContacts(ctx, w, false)
	if err := send(schema.Rescan(self.name)); err != nil {
		return err
	}

	ticker := time.NewTicker(self.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.signal:
			// What the filer wrote, which the contacts it knows have already
			for _, event := range self.takeContacts(w) {
				if err := send(event); err != nil {
					return err
				}
			}
			self.saveContacts(ctx, w, false)
			continue
		case <-ticker.C:
		}
		events, err := self.syncContacts(ctx, w)
		if errors.Is(err, httpclient.ErrSyncTokenGone) {
			// What changed since isn't known, so every contact is got again
			if err = self.resyncContacts(ctx, w); err == nil {
				events = []schema.Event{schema.Rescan(self.name)}
			}
		}
		switch {
		case errors.Is(err, gofiler.ErrServiceUnavailable):
			// It's synced again at the next interval
			continue
		case err != nil:
			return err
		}
		for _, event := range events {
			if err := send(event); err != nil {
				return err
			}
		}
		self.saveContacts(ctx, w, false)
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// watch starts a watch, or returns ErrConflict, when the filer is watched
// already, as one watch keeps the contacts the filer reads
func (self *ContactFiler) watch(w *contactWatch) error {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.live != nil {
		return gofiler.ErrConflict.Withf("volume %q is already watched", self.name)
	}
	self.live = w
	return nil
}

// unwatch stops the filer reading the contacts of a watch, which has
// stopped, and clears those which were got before it, so they're got again
func (self *ContactFiler) unwatch(w *contactWatch) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.live == w {
		self.live = nil
	}
	self.people.clear()
}

// livePeople returns the contacts of the watch, or false, when it isn't
// watched, or doesn't know them yet
func (self *ContactFiler) livePeople() ([]httpclient.Person, bool) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.live == nil || self.live.people == nil {
		return nil, false
	}
	return slices.Collect(maps.Values(self.live.people)), true
}

// livePerson returns a contact, of its id, of the watch, or false, when it
// isn't watched, or doesn't know them yet, and nil, when it doesn't know it
func (self *ContactFiler) livePerson(id string) (*httpclient.Person, bool) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.live == nil || self.live.people == nil {
		return nil, false
	} else if person, exists := self.live.people[id]; exists {
		return &person, true
	}
	return nil, true
}

// resyncContacts gets every contact, and the token of the changes after
// them, which the watch then knows. The watch isn't changed when they can't
// be got.
func (self *ContactFiler) resyncContacts(ctx context.Context, w *contactWatch) error {
	unlock, err := self.lockSync(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	people, token, err := self.client.SyncConnections(ctx, httpclient.PersonFields, "")
	if err != nil {
		return self.apiError("watch", "/", err)
	}
	byId := make(map[string]httpclient.Person, len(people))
	for _, person := range people {
		if id := contactId(&person); !person.Metadata.Deleted && googleId(id) {
			byId[id] = person
		}
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	w.token, w.people, w.dirty = token, byId, true
	return nil
}

// syncContacts gets what changed since the token, and applies it to the
// contacts the watch knows, after the events of the filer's writes, which
// are sent first, and returns the events of what changed. Returns
// ErrSyncTokenGone when Google doesn't accept the token.
func (self *ContactFiler) syncContacts(ctx context.Context, w *contactWatch) ([]schema.Event, error) {
	unlock, err := self.lockSync(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	changed, token, err := self.client.SyncConnections(ctx, httpclient.PersonFields, w.token)
	if errors.Is(err, httpclient.ErrSyncTokenGone) {
		return nil, err
	} else if err != nil {
		return nil, self.apiError("watch", "/", err)
	}
	self.mu.Lock()
	defer self.mu.Unlock()

	// A new token is saved, though nothing changed, so a restart syncs from
	// it, rather than from an older one
	if token != w.token {
		w.token, w.dirty = token, true
	}
	events := append(w.pending, self.applyContacts(w, changed)...)
	w.pending = nil
	return events, nil
}

// lockSync waits for a sync, or a write, to finish, and returns the
// function which lets the next one start, or the context's error, when
// it's cancelled first
func (self *ContactFiler) lockSync(ctx context.Context) (func(), error) {
	select {
	case self.syncing <- struct{}{}:
		return func() { <-self.syncing }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// applyContacts applies contacts which changed to those the watch knows,
// which the filer must be locked for, and returns the events of what
// changed: a contact which is created, or whose entity tag changes, or
// which is deleted, in the order of their paths. A contact which is the
// same, such as one the filer wrote, isn't sent again.
func (self *ContactFiler) applyContacts(w *contactWatch, changed []httpclient.Person) []schema.Event {
	var events []schema.Event
	event := func(t schema.EventType, id string) {
		events = append(events, schema.Event{EventType: t, ObjectKey: schema.ObjectKey{Name: self.name, Path: "/" + id + contactExt}})
	}
	for _, person := range changed {
		id := contactId(&person)
		if !googleId(id) {
			continue
		}
		old, exists := w.people[id]
		switch {
		case person.Metadata.Deleted:
			if exists {
				delete(w.people, id)
				event(schema.EventTypeDeleted, id)
			}
		case !exists:
			w.people[id] = person
			event(schema.EventTypeCreated, id)
		case old.ETag != person.ETag:
			w.people[id] = person
			event(schema.EventTypeUpdated, id)
		}
	}
	if len(events) > 0 {
		w.dirty = true
	}
	slices.SortFunc(events, func(a, b schema.Event) int { return strings.Compare(a.Path, b.Path) })
	return events
}

// wroteContact applies a contact the filer wrote, as it is now, or which it
// deleted, of its metadata, to those the watch knows, when it's watched,
// and sends it the events, as they're written, so it doesn't send them
// again, when they're synced, or, when it isn't watched, clears the
// contacts which are kept, so they're got again
func (self *ContactFiler) wroteContact(person httpclient.Person) {
	self.mu.Lock()
	defer self.mu.Unlock()
	w := self.live
	if w == nil || w.people == nil {
		self.people.clear()
		return
	}
	if events := self.applyContacts(w, []httpclient.Person{person}); len(events) > 0 {
		w.pending = append(w.pending, events...)
		select {
		case w.signal <- struct{}{}:
		default:
		}
	}
}

// takeContacts returns the events of what the filer wrote, which the watch
// hasn't sent, and empties them
func (self *ContactFiler) takeContacts(w *contactWatch) []schema.Event {
	self.mu.Lock()
	defer self.mu.Unlock()
	events := w.pending
	w.pending = nil
	return events
}

// restoreContacts sets the contacts a watch knows, and the token of the
// changes after them, to those which were saved in the volume's cache, and
// returns true, or false, when there are none, or they can't be read
func (self *ContactFiler) restoreContacts(ctx context.Context, w *contactWatch) bool {
	entry, err := self.cache.Get(ctx, contactsKey)
	if err != nil {
		return false
	}
	defer entry.Close()
	meta := entry.Meta()
	if meta.Values[driveTreeFormatValue] != contactsFormat || meta.Version == "" {
		return false
	}
	people := make(map[string]httpclient.Person)
	dec := json.NewDecoder(entry)
	for {
		var person httpclient.Person
		if err := dec.Decode(&person); errors.Is(err, io.EOF) {
			break
		} else if err != nil || !googleId(contactId(&person)) {
			return false
		}
		people[contactId(&person)] = person
	}
	self.mu.Lock()
	defer self.mu.Unlock()
	w.token, w.people = meta.Version, people
	return true
}

// saveContacts stores the contacts a watch knows, and the token of the
// changes after them, in the volume's cache, when they've changed since
// they were saved, and they weren't saved for contactSaveInterval, or now,
// when it's true, as they're encoded, through a pipe. A save which fails is
// tried again, after the interval.
func (self *ContactFiler) saveContacts(ctx context.Context, w *contactWatch, now bool) {
	self.mu.Lock()
	if !w.dirty || w.people == nil || (!now && time.Since(w.saved) < contactSaveInterval) {
		self.mu.Unlock()
		return
	}
	token := w.token
	people := slices.Collect(maps.Values(w.people))
	w.dirty = false
	self.mu.Unlock()

	pr, pw := io.Pipe()
	go func() {
		enc := json.NewEncoder(pw)
		for i := range people {
			if err := enc.Encode(&people[i]); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		pw.Close()
	}()
	meta := gofiler.CacheMeta{Version: token, Values: map[string]string{driveTreeFormatValue: contactsFormat}}
	entry, err := self.cache.Put(ctx, contactsKey, 0, meta, pr)
	pr.CloseWithError(err)
	self.mu.Lock()
	defer self.mu.Unlock()
	if err != nil {
		w.dirty = true
	} else {
		entry.Close()
	}
	w.saved = time.Now()
}

// photo returns a contact's photo, of its link, at photoSize, which is
// kept in the volume's cache for photoTTL, so it isn't got each time the
// contact is read, or nil when it can't be got, which isn't an error, so
// the contact is read with its link. It's got without the sign-in's token,
// which isn't needed, nor of the quota, as it isn't the API's.
func (self *ContactFiler) photo(ctx context.Context, link string) *vcard.Photo {
	hash := sha256.Sum256([]byte(link))
	entry, err := self.cache.GetOrCreate(ctx, photoKey+hex.EncodeToString(hash[:16]), "", photoTTL, func(w io.Writer) (gofiler.CacheMeta, error) {
		mediaType, data, err := fetchPhoto(ctx, link)
		if err != nil {
			return gofiler.CacheMeta{}, err
		} else if _, err := w.Write(data); err != nil {
			return gofiler.CacheMeta{}, err
		}
		return gofiler.CacheMeta{Values: map[string]string{photoType: mediaType}}, nil
	})
	if err != nil {
		return nil
	}
	defer entry.Close()
	data, err := io.ReadAll(io.LimitReader(entry, maxPhotoSize+1))
	if err != nil || len(data) == 0 || len(data) > maxPhotoSize {
		return nil
	}
	return &vcard.Photo{MediaType: entry.Meta().Values[photoType], Data: data}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// fetchPhoto gets a photo, of its link, at photoSize, and returns its media
// type, and its data, or an error, when it isn't an image, or is too large
func fetchPhoto(ctx context.Context, link string) (string, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, photoTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rePhotoSize.ReplaceAllString(link, fmt.Sprintf("=s%d", photoSize)), nil)
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("Accept", "image/*")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if resp.StatusCode != http.StatusOK || err != nil || !strings.HasPrefix(mediaType, "image/") {
		return "", nil, fmt.Errorf("photo %q: %s", link, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxPhotoSize+1))
	if err != nil {
		return "", nil, err
	} else if len(data) == 0 || len(data) > maxPhotoSize {
		return "", nil, fmt.Errorf("photo %q: %d bytes", link, len(data))
	}
	return mediaType, data, nil
}
