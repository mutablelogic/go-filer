package google

import (
	"context"
	"maps"
	"path"
	"slices"
	"strings"
	"time"

	// Packages
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	store "github.com/mutablelogic/go-filer/store"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// savedEvent is an event of a listing, which is saved in the volume's cache,
// of its calendar's id
type savedEvent struct {
	Calendar string           `json:"calendar"`
	Event    httpclient.Event `json:"event"`
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// listingKeys is the prefix of the keys of the listings in the volume's
	// cache, of their directories, which end with "#", so the root's,
	// list:/#, isn't a prefix of the others'
	listingKeys = "list:/"

	// calendarTagsKey is the key of the entity tags of the calendars, by
	// their ids, which the listings which are saved are of, so a calendar
	// which is added, removed, or changed, such as renamed, while it isn't
	// watched, is known, when it's watched again
	calendarTagsKey = "calendars"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// persisting returns true while the filer is watched, and its watch has
// started, as only then are the listings which are saved in the volume's
// cache kept up to date, when what they're of changes
func (self *CalendarFiler) persisting() bool {
	self.mu.Lock()
	defer self.mu.Unlock()
	return self.persist
}

// setPersisting sets whether the listings which are saved are used
func (self *CalendarFiler) setPersisting(persist bool) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.persist = persist
}

// loadCalendarTags returns the entity tags of the calendars, by their ids,
// which were saved in the volume's cache, or none, when there are none
func (self *CalendarFiler) loadCalendarTags(ctx context.Context) map[string]string {
	tags := make(map[string]string)
	if _, err := store.GetCacheJSON(ctx, self.cache, calendarTagsKey, &tags); err != nil {
		return make(map[string]string)
	}
	return tags
}

// saveCalendarTags saves the entity tags of the calendars of a sync, in the
// volume's cache, when they aren't those before it
func (self *CalendarFiler) saveCalendarTags(ctx context.Context, state *calendarSync, before map[string]string) {
	if !maps.Equal(state.calendars, before) {
		_ = store.PutCacheJSON(ctx, self.cache, calendarTagsKey, 0, store.CacheMeta{}, state.calendars)
	}
}

// loadListing returns the events of a directory, of the days, which were
// saved in the volume's cache, with the calendars they're of, whose paths
// are added for the watch, as a listing's are, or false, when there's none,
// or one of its calendars isn't one any more
func (self *CalendarFiler) loadListing(ctx context.Context, dir string, from, to time.Time) ([]calendarEvent, bool) {
	var saved []savedEvent
	if _, err := store.GetCacheJSON(ctx, self.cache, listingKey(dir), &saved); err != nil {
		return nil, false
	}
	calendars, err := self.calendars(ctx)
	if err != nil {
		return nil, false
	}
	byId := make(map[string]*httpclient.Calendar, len(calendars))
	for i := range calendars {
		byId[calendars[i].Id] = &calendars[i]
	}
	result := make([]calendarEvent, 0, len(saved))
	for i := range saved {
		calendar, exists := byId[saved[i].Calendar]
		if !exists {
			return nil, false
		}
		event := saved[i].Event
		result = append(result, calendarEvent{calendar: calendar, event: &event, path: self.eventPath(calendar, &event)})
	}
	slices.SortFunc(result, func(a, b calendarEvent) int { return strings.Compare(a.path, b.path) })
	self.listedPaths(from, to, result)
	return result, true
}

// saveListing saves the events of a directory in the volume's cache, until
// what they're of changes, which isn't an error when it can't, as they're
// got again
func (self *CalendarFiler) saveListing(ctx context.Context, dir string, events []calendarEvent) {
	saved := make([]savedEvent, 0, len(events))
	for _, e := range events {
		saved = append(saved, savedEvent{Calendar: e.calendar.Id, Event: *e.event})
	}
	_ = store.PutCacheJSON(ctx, self.cache, listingKey(dir), 0, store.CacheMeta{}, saved)
}

// dropListings deletes the listings which are saved, of the events of the
// paths: of their months, their years, and the root's, so they're got
// again, or, without any, every listing
func (self *CalendarFiler) dropListings(ctx context.Context, paths ...string) {
	if len(paths) == 0 {
		_ = self.cache.Delete(ctx, listingKeys)
		return
	}
	dirs := map[string]bool{"/": true}
	for _, p := range paths {
		if p == "" {
			continue
		}
		month := path.Dir(p)
		dirs[month], dirs[path.Dir(month)] = true, true
	}
	for dir := range dirs {
		_ = self.cache.Delete(ctx, listingKey(dir))
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// listingKey returns the key of a directory's listing, in the volume's cache
func listingKey(dir string) string {
	return "list:" + dir + "#"
}
