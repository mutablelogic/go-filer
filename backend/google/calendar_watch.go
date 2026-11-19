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
	tokens   map[string]string
	stored   map[string]string
	rejected map[string]time.Time
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// calendarResyncInterval is how often a calendar whose sync token was
	// rejected is synced again, at most
	calendarResyncInterval = time.Hour
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WatchObjects polls each calendar for its changes since its sync token, at
// the refresh interval, and sends a rescan of the root, when any of them
// changed, until the context is cancelled. The rescan lists the days which
// are listed again, whose events which changed, are created, or are
// deleted, are found of their entity tags, as a sync token can't be of
// those days, nor of each time an event which repeats occurs, so it says
// that an event changed, but not where. The root is rescanned as well when
// a calendar is added, or removed, or the days which are listed move on,
// and the first change is a rescan of it, once the calendars are watched.
//
// The tokens are stored, with the provider of credentials, of the sign-in
// (see calendarSyncKey), so after a restart, the changes since they were
// stored are found. Volumes of the same sign-in share them, as they're of
// its calendars, rather than of a window, so one which is synced of
// another's earlier token rescans what changed since, again. A calendar without one is synced for one. A token which
// Google rejects, as it's expired, is synced again, which a calendar is, at
// most, each calendarResyncInterval. Returns ErrNotImplemented when the
// refresh interval is zero, and the error of reading them, such as a
// SignInError, when a sign-in is needed.
// Reference: https://developers.google.com/calendar/api/guides/sync
func (self *CalendarFiler) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	if self.refreshInterval == 0 {
		return gofiler.ErrNotImplemented.Withf("volume %q isn't watched, as its %s is zero", self.name, paramRefreshInterval)
	}
	state := &calendarSync{tokens: self.loadTokens(ctx), rejected: make(map[string]time.Time)}
	state.stored = maps.Clone(state.tokens)
	from, _ := self.days()
	if _, err := self.poll(ctx, state); err != nil {
		return err
	}
	select {
	case ch <- schema.Rescan(self.name):
	case <-ctx.Done():
		return ctx.Err()
	}
	ticker := time.NewTicker(self.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		changed, err := self.poll(ctx, state)
		if err != nil {
			return err
		}
		// The days which are listed move on, so events enter, and leave,
		// them, without changing
		if next, _ := self.days(); !next.Equal(from) {
			from, changed = next, true
		}
		if !changed {
			continue
		}
		select {
		case ch <- schema.Rescan(self.name):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// poll syncs each calendar, of its token, and returns true when any of
// them changed: an event of it changed, it's new, or it's removed, or its
// token was rejected, so its changes may have been missed. The tokens are
// stored when they change.
func (self *CalendarFiler) poll(ctx context.Context, state *calendarSync) (bool, error) {
	calendars, err := self.client.Calendars(ctx)
	if err != nil {
		return false, self.apiError("watch", "/", err)
	}
	changed := false
	seen := make(map[string]bool, len(calendars))
	for _, calendar := range calendars {
		seen[calendar.Id] = true
		token, exists := state.tokens[calendar.Id]
		if exists && token != "" {
			n, next, err := self.client.Sync(ctx, calendar.Id, token)
			switch {
			case errors.Is(err, httpclient.ErrSyncTokenGone):
				// It's synced again, for another, unless it was rejected a
				// while ago, when it waits
				delete(state.tokens, calendar.Id)
				changed = true
				if at, rejected := state.rejected[calendar.Id]; rejected && self.now().Sub(at) < calendarResyncInterval {
					continue
				}
				state.rejected[calendar.Id] = self.now()
			case err != nil:
				return false, self.apiError("watch", "/", err)
			default:
				if n > 0 {
					changed = true
				}
				if next != "" {
					state.tokens[calendar.Id] = next
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
			return false, self.apiError("watch", "/", err)
		}
		state.tokens[calendar.Id] = next
		changed = true
	}

	// A calendar which is removed has no token
	for id := range state.tokens {
		if !seen[id] {
			delete(state.tokens, id)
			delete(state.rejected, id)
			changed = true
		}
	}
	if !maps.Equal(state.tokens, state.stored) {
		self.storeTokens(ctx, state.tokens)
		state.stored = maps.Clone(state.tokens)
	}
	return changed, nil
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
	return credential + "_CALENDAR_SYNC"
}
