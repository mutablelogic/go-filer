package google

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"sync"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// calendarSync is what's known while the calendars are watched: the sync
// token of each calendar, by its id, which are stored, and when a
// calendar's token was last rejected, so one whose tokens are rejected again
// and again, such as a holiday calendar's, isn't synced every poll
type calendarSync struct {
	tokens    map[string]string
	stored    map[string]string
	rejected  map[string]time.Time
	calendars map[string]string // The entity tags of the calendars, by their ids, which change when one's renamed
}

// calendarWatch is a watch's events, of what the filer wrote, which it
// hasn't sent, and the signal that there are some
type calendarWatch struct {
	mu      sync.Mutex
	pending []schema.Event
	signal  chan struct{}
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// calendarSyncSuffix is the suffix of the keys of calendars' sync tokens,
	// of their sign-ins' keys, which a sign-in's key can't have, so they're
	// never the same
	calendarSyncSuffix = "_CALENDAR_SYNC"

	// calendarResyncInterval is how often a calendar whose sync token was
	// rejected is synced again, at most
	calendarResyncInterval = time.Hour
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WatchObjects polls each calendar for its changes since its sync token, at
// the refresh interval, and sends them, until the context is cancelled: an
// event which is created, updated, moved, which is deleted where it was, or
// cancelled, of where the events which were listed are, as a change doesn't
// say where an event was. The root is rescanned instead when that can't be
// said: when an event which repeats changed, as a sync is of it, rather
// than the times it occurs, nothing has been listed, a calendar is added,
// removed, or changed, such as renamed, which its events' content is of, a
// calendar's token was rejected, so its changes may have been missed, or
// the days which are listed move on. The first change is a rescan of the
// root, once the calendars are watched. What the filer writes, of the event
// action, is sent as it's written, rather than when it's synced, and isn't
// sent again when it is (see wrote).
//
// The tokens are stored, with the provider of credentials, of the sign-in
// (see calendarSyncKey), so after a restart, the changes since they were
// stored are found. Volumes of the same sign-in share them, as they're of
// its calendars, rather than of a window, so one which is synced of
// another's earlier token rescans what changed since, again. A calendar
// without one is synced for one. A token which Google rejects, as it's
// expired, is synced again, which a calendar is, at most, each
// calendarResyncInterval. Returns ErrNotImplemented when the refresh
// interval is zero, and the error of reading them, such as a SignInError,
// when a sign-in is needed.
// Reference: https://developers.google.com/calendar/api/guides/sync
func (self *CalendarFiler) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	if self.refreshInterval == 0 {
		return gofiler.ErrNotImplemented.Withf("volume %q isn't watched, as its %s is zero", self.name, paramRefreshInterval)
	}
	state := &calendarSync{tokens: self.loadTokens(ctx), rejected: make(map[string]time.Time), calendars: make(map[string]string)}
	state.stored = maps.Clone(state.tokens)
	from, _ := self.days()

	// What the filer writes, from when it's watching, before the poll, so
	// it's in the rescan
	watch := self.watch()
	defer self.unwatch(watch)
	if _, _, err := self.poll(ctx, state); err != nil {
		return err
	}

	// What was got before it's watched may be out of date, of what the poll
	// found, which isn't sent, so it's got again, of the rescan, which what
	// the filer wrote, until then, is of
	self.clearCache()
	watch.take()
	send := func(event schema.Event) error {
		select {
		case ch <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := send(schema.Rescan(self.name)); err != nil {
		return err
	}
	ticker := time.NewTicker(self.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-watch.signal:
			// What the filer wrote, whose cache it cleared
			for _, event := range watch.take() {
				if err := send(event); err != nil {
					return err
				}
			}
			continue
		case <-ticker.C:
		}
		rescan, events, err := self.poll(ctx, state)
		if err != nil {
			return err
		}

		// The days which are listed move on, so events enter, and leave,
		// them, without changing
		if next, _ := self.days(); !next.Equal(from) {
			from, rescan = next, true
		}
		if rescan {
			events = []schema.Event{schema.Rescan(self.name)}
		}
		if len(events) > 0 {
			// What was got from Google is out of date, so it's got again,
			// before what changed is sent, and read
			self.clearCache()
		}
		for _, event := range events {
			if err := send(event); err != nil {
				return err
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// poll syncs each calendar, of its token, and returns the events of what
// changed, or true when the root is rescanned instead (see WatchObjects).
// The tokens are stored when they change.
func (self *CalendarFiler) poll(ctx context.Context, state *calendarSync) (bool, []schema.Event, error) {
	// The calendars are got from Google, rather than the cache, as what
	// changed of them is found of them
	calendars, err := self.client.Calendars(ctx)
	if err != nil {
		return false, nil, self.apiError("watch", "/", err)
	}
	rescan := false
	var events []schema.Event
	seen := make(map[string]bool, len(calendars))
	etags := make(map[string]string, len(calendars))
	for _, calendar := range calendars {
		etags[calendar.Id] = calendar.ETag
	}
	if !maps.Equal(etags, state.calendars) {
		// A calendar which is added, removed, or changed, such as renamed,
		// which its events' content is of
		rescan, state.calendars = true, etags
	}
	for i := range calendars {
		calendar := &calendars[i]
		seen[calendar.Id] = true
		token, exists := state.tokens[calendar.Id]
		if exists && token != "" {
			changed, next, err := self.client.Sync(ctx, calendar.Id, token)
			switch {
			case errors.Is(err, httpclient.ErrSyncTokenGone):
				// It's synced again, for another, unless it was rejected a
				// while ago, when it waits
				delete(state.tokens, calendar.Id)
				rescan = true
				if at, rejected := state.rejected[calendar.Id]; rejected && self.now().Sub(at) < calendarResyncInterval {
					continue
				}
				state.rejected[calendar.Id] = self.now()
			case err != nil:
				return false, nil, self.apiError("watch", "/", err)
			default:
				if next != "" {
					state.tokens[calendar.Id] = next
				}
				if len(changed) > 0 {
					if hinted, ok := self.hinted(calendar, changed); ok {
						events = append(events, hinted...)
					} else {
						rescan = true
					}
				}
				continue
			}
		} else if at, rejected := state.rejected[calendar.Id]; rejected && self.now().Sub(at) < calendarResyncInterval {
			continue
		}

		// A calendar without a token, which is new, or was rejected, is
		// synced, for one, of all its events
		_, next, err := self.client.Sync(ctx, calendar.Id, "")
		if err != nil {
			return false, nil, self.apiError("watch", "/", err)
		}
		state.tokens[calendar.Id] = next
		rescan = true
	}

	// A calendar which is removed has no token
	for id := range state.tokens {
		if !seen[id] {
			delete(state.tokens, id)
			delete(state.rejected, id)
			rescan = true
		}
	}
	if !maps.Equal(state.tokens, state.stored) {
		self.storeTokens(ctx, state.tokens)
		state.stored = maps.Clone(state.tokens)
	}
	return rescan, events, nil
}

// watch returns a watch, which is sent what the filer writes, until it's
// unwatched
func (self *CalendarFiler) watch() *calendarWatch {
	watch := &calendarWatch{signal: make(chan struct{}, 1)}
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.watches == nil {
		self.watches = make(map[*calendarWatch]struct{})
	}
	self.watches[watch] = struct{}{}
	return watch
}

// unwatch removes a watch
func (self *CalendarFiler) unwatch(watch *calendarWatch) {
	self.mu.Lock()
	defer self.mu.Unlock()
	delete(self.watches, watch)
}

// wrote sends the watches what the filer wrote, as it wrote it, of an event
// of a calendar, as it was, or nil, when it's created, and as it is, or nil,
// when it's deleted: deleted where it was, and created where it is, or
// updated, when it's where it was, of the days which are listed. Where it
// is, is known, and its entity tag, so a sync doesn't send it again. The
// cache is cleared first, so it's read as it is.
func (self *CalendarFiler) wrote(calendar *httpclient.Calendar, before, after *httpclient.Event) {
	self.clearCache()
	self.mu.Lock()
	defer self.mu.Unlock()
	from, to := self.days()
	pathOf := func(e *httpclient.Event) string {
		if e == nil || !reEventId.MatchString(e.Id) || !onDays(e, from, to) {
			return ""
		}
		return self.eventPath(calendar, e)
	}
	old, now := pathOf(before), pathOf(after)
	event := func(t schema.EventType, p string) schema.Event {
		return schema.Event{EventType: t, ObjectKey: schema.ObjectKey{Name: self.name, Path: p}}
	}
	var events []schema.Event
	if old != "" && old != now {
		events = append(events, event(schema.EventTypeDeleted, old))
	}
	if now != "" && now == old {
		events = append(events, event(schema.EventTypeUpdated, now))
	} else if now != "" {
		events = append(events, event(schema.EventTypeCreated, now))
	}

	// Where it is, and its entity tag, which a sync is of
	e := after
	if e == nil {
		e = before
	}
	key := eventKey(calendar.Id, e.Id)
	if self.paths != nil {
		if now != "" {
			self.paths[key] = now
		} else {
			delete(self.paths, key)
		}
	}
	if after != nil {
		if self.written == nil {
			self.written = make(map[string]string)
		}
		self.written[key] = after.ETag
	} else {
		delete(self.written, key)
	}
	if len(events) == 0 {
		return
	}
	for watch := range self.watches {
		watch.mu.Lock()
		watch.pending = append(watch.pending, events...)
		watch.mu.Unlock()
		select {
		case watch.signal <- struct{}{}:
		default:
		}
	}
}

// take returns the events of what the filer wrote, which the watch hasn't
// sent, and empties them
func (self *calendarWatch) take() []schema.Event {
	self.mu.Lock()
	defer self.mu.Unlock()
	events := self.pending
	self.pending = nil
	return events
}

// loadTokens returns the sync tokens which are stored, of the sign-in, or
// none, when there aren't any, or they can't be read
func (self *CalendarFiler) loadTokens(ctx context.Context) map[string]string {
	result := make(map[string]string)
	if self.opts.Credentials == nil {
		return result
	}
	data, err := self.opts.Credentials.Get(ctx, calendarSyncKey(self.credential))
	if err != nil {
		return result
	} else if err := json.Unmarshal(data, &result); err != nil {
		return make(map[string]string)
	}
	return result
}

// storeTokens stores the sync tokens, of the sign-in, with the provider of
// credentials, when it can store them, which they're synced again without,
// when it can't
func (self *CalendarFiler) storeTokens(ctx context.Context, tokens map[string]string) {
	if self.opts.Credentials == nil {
		return
	}
	if data, err := json.Marshal(tokens); err == nil {
		_ = self.opts.Credentials.Set(ctx, calendarSyncKey(self.credential), data)
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// calendarSyncKey returns the key of the sync tokens of the calendars of a
// sign-in, of its key, with the provider of credentials, such as
// GOOGLE_TOKEN_CALENDAR_SYNC, of GOOGLE_TOKEN, which isn't a sign-in, as
// it's only signed in to by its own key
func calendarSyncKey(credential string) string {
	return credential + calendarSyncSuffix
}
