package google

import (
	"context"
	"errors"
	"path"
	"slices"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
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

	// checkpointKey is the key of the sync tokens, and the entity tags, of
	// the calendars, by their ids, which the listings which are saved are
	// up to date of, which a watch which starts again uses them of, when
	// they're the tokens it starts of, and nothing changed since. It's
	// deleted before the listings are, and saved after, so they aren't
	// used, when they couldn't be deleted.
	checkpointKey = "checkpoint"

	// The values of a listing: the days it's of
	listingFrom = "from"
	listingTo   = "to"
)

// checkpoint is the sync tokens, and the entity tags, of the calendars, by
// their ids, which the listings which are saved are up to date of
type checkpoint struct {
	Tokens map[string]string `json:"tokens"`
	Tags   map[string]string `json:"tags"`
}

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

// loadCheckpoint returns the checkpoint of the listings which are saved,
// or false, when there's none
func (self *CalendarFiler) loadCheckpoint(ctx context.Context) (checkpoint, bool) {
	var result checkpoint
	if _, err := store.GetCacheJSON(ctx, self.cache, checkpointKey, &result); err != nil {
		return checkpoint{}, false
	}
	return result, true
}

// saveCheckpoint saves the checkpoint of the listings which are saved, of
// the sync tokens, and the calendars' entity tags, of a sync, once the
// listings it changed are deleted
func (self *CalendarFiler) saveCheckpoint(ctx context.Context, state *calendarSync) error {
	return store.PutCacheJSON(ctx, self.cache, checkpointKey, 0, gofiler.CacheMeta{}, checkpoint{Tokens: state.tokens, Tags: state.calendars})
}

// loadListing returns the events of a directory, of the days, which were
// saved in the volume's cache, with the calendars they're of, whose paths
// are added for the watch, as a listing's are, or false, when there's none,
// or one of its calendars isn't one any more
func (self *CalendarFiler) loadListing(ctx context.Context, dir string, from, to time.Time) ([]calendarEvent, bool) {
	var saved []savedEvent
	if meta, err := store.GetCacheJSON(ctx, self.cache, listingKey(dir), &saved); err != nil {
		return nil, false
	} else if meta.Values[listingFrom] != from.UTC().Format(time.RFC3339) || meta.Values[listingTo] != to.UTC().Format(time.RFC3339) {
		// It's of other days, such as of the day before a restart
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

// listingGeneration returns the generation of the listings, which changes
// each time they're deleted, so a listing which was got before isn't saved
func (self *CalendarFiler) listingGeneration() uint64 {
	self.listMu.Lock()
	defer self.listMu.Unlock()
	return self.listGen
}

// saveListing saves the events of a directory, of the days, in the volume's
// cache, until what they're of changes, unless listings were deleted since
// the generation, when it was got, so it may be out of date, which isn't
// an error when it can't, as they're got again
func (self *CalendarFiler) saveListing(ctx context.Context, dir string, from, to time.Time, events []calendarEvent, generation uint64) {
	saved := make([]savedEvent, 0, len(events))
	for _, e := range events {
		saved = append(saved, savedEvent{Calendar: e.calendar.Id, Event: *e.event})
	}
	self.listMu.Lock()
	defer self.listMu.Unlock()
	if self.listGen != generation {
		return
	}
	meta := gofiler.CacheMeta{Values: map[string]string{listingFrom: from.UTC().Format(time.RFC3339), listingTo: to.UTC().Format(time.RFC3339)}}
	_ = store.PutCacheJSON(ctx, self.cache, listingKey(dir), 0, meta, saved)
}

// dropListings deletes the checkpoint, and then the listings which are
// saved, of the events of the paths: of their months, their years, and the
// root's, so they're got again, or, without any, every listing, and changes
// their generation, so a listing which is being got isn't saved. Returns
// the error of deleting them, when the checkpoint is deleted, so they
// aren't used.
func (self *CalendarFiler) dropListings(ctx context.Context, paths ...string) error {
	self.listMu.Lock()
	defer self.listMu.Unlock()
	self.listGen++
	if err := self.cache.Delete(ctx, checkpointKey); err != nil {
		return err
	} else if len(paths) == 0 {
		return self.cache.Delete(ctx, listingKeys)
	}
	dirs := map[string]bool{"/": true}
	for _, p := range paths {
		if p == "" {
			continue
		}
		month := path.Dir(p)
		dirs[month], dirs[path.Dir(month)] = true, true
	}
	var result error
	for dir := range dirs {
		result = errors.Join(result, self.cache.Delete(ctx, listingKey(dir)))
	}
	return result
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// listingKey returns the key of a directory's listing, in the volume's cache
func listingKey(dir string) string {
	return "list:" + dir + "#"
}
