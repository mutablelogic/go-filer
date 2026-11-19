package google

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
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
// root, once the calendars are watched.
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
	if _, _, err := self.poll(ctx, state); err != nil {
		return err
	}
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
