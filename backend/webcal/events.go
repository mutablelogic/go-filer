package webcal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	ical "github.com/mutablelogic/go-metadata/pkg/ical"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// events are a feed's events, as files, of the folders of their days, by
// their paths, with where their lines are in the feed, which is in the
// volume's cache, of its key, and its generation, and the calendar's name
type events struct {
	title      string
	key        string                    // the key of the feed in the volume's cache
	generation string                    // which feed it is, of those of the key
	src        *source                   // the calendar's properties, and time zones
	objects    map[string]*schema.Object // of their paths, folders with "/" after them
	files      map[string]*file          // the events' lines, of their paths
	paths      []string                  // every path, in order
	keys       map[string]string         // the paths of the events, of their UIDs
}

// file is where an event's lines are, and those of its changes, in the
// feed, and the time zones they're in
type file struct {
	zones []string
	parts []part
}

// event is an event of a feed, of its UID, with the changes to the times it
// repeats, of their RECURRENCE-IDs
type event struct {
	key        string      // its UID, or a hash of it, when it has none
	components []component // the event, then its changes, in the order of when they were to start
	start      ical.Time   // when it starts, or first does, or zero
	title      string
}

// objectList is a list of objects, as a listing's iterator
type objectList struct {
	mu      sync.Mutex
	objects []*schema.Object
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// undated is the folder of the events which have no start, such as a
	// task which isn't due
	undated = "undated"

	// slugLength is the most runes of an event's title in its path
	slugLength = 64

	// ext is the extension of an event's file
	ext = ".ics"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newEvents returns the events of a feed, as files, at their days' paths,
// of the folders of their years and months, of when they start, or first
// do, or a task is due, in their own zones, and those which have none, of
// the folder undated. An event's file is its iCalendar, of its lines, and
// those of its changes to the times it repeats, which have its UID. Events
// of the same title and day are told apart by a number, in the order of
// their UIDs.
func newEvents(src *source, key, generation string) *events {
	self := &events{title: src.name, key: key, generation: generation, src: src, objects: make(map[string]*schema.Object), files: make(map[string]*file), keys: make(map[string]string)}
	self.dir("/")

	// The events, of their UIDs, with their changes
	byKey := make(map[string]*event)
	for _, c := range src.components {
		e := c.event
		key := strings.TrimSpace(e.UID)
		if key == "" {
			key = "event-" + hash(e.Summary, e.Start.Time.String(), e.Due.Time.String())
		}
		ev, exists := byKey[key]
		if !exists {
			ev = &event{key: key}
			byKey[key] = ev
		}
		ev.components = append(ev.components, c)
	}
	list := slices.SortedFunc(maps.Values(byKey), func(a, b *event) int { return strings.Compare(a.key, b.key) })
	for _, ev := range list {
		// The event, which isn't a change, is first, then its changes, of
		// when they were to start
		slices.SortStableFunc(ev.components, func(a, b component) int {
			switch {
			case a.event.RecurrenceID.IsZero() != b.event.RecurrenceID.IsZero():
				if a.event.RecurrenceID.IsZero() {
					return -1
				}
				return 1
			default:
				return a.event.RecurrenceID.Time.Compare(b.event.RecurrenceID.Time)
			}
		})
		first := ev.components[0].event
		ev.title = strings.Join(strings.Fields(first.Summary), " ")
		ev.start = first.Start
		if ev.start.IsZero() {
			ev.start = first.Due
		}
	}

	// The paths of the events, of their folders, and those of the same name
	// are numbered, of the first number which no other event's path is
	taken := make(map[string]bool)
	for _, ev := range list {
		folder, base := "/"+undated+"/", slug(ev.title, ev.key)
		if t := ev.start.Time; !t.IsZero() {
			folder = fmt.Sprintf("/%04d/%04d-%02d/", t.Year(), t.Year(), t.Month())
			base = fmt.Sprintf("%02d-%s", t.Day(), base)
		}
		name := folder + base + ext
		for n := 2; taken[name]; n++ {
			name = fmt.Sprintf("%s%s-%d%s", folder, base, n, ext)
		}
		taken[name] = true
		self.file(name, ev, src)
		self.keys[ev.key] = name
	}
	self.paths = slices.Sorted(maps.Keys(self.objects))

	// What's decoded of the events isn't kept, once they're files
	src.components = nil
	return self
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// dir adds a folder, and those above it
func (self *events) dir(p string) {
	for p != "" {
		if _, exists := self.objects[p]; exists {
			return
		}
		self.objects[p] = &schema.Object{ObjectKey: schema.ObjectKey{Path: p}, ObjectAttr: schema.ObjectAttr{IsDir: true}, ObjectMeta: schema.ObjectMeta{ContentType: schema.DirectoryType}}
		if p == "/" {
			return
		}
		p = path.Dir(strings.TrimSuffix(p, "/"))
		if p != "/" {
			p += "/"
		}
	}
}

// file adds an event, as its iCalendar, at the path, with its folders: the
// calendar's properties, the time zones its times are in, and its lines,
// and those of its changes, as they are, apart from DTSTAMP, which is when
// it was last modified, or created, or else the feed's. Many feeds set it to
// when they're fetched, so the event's entity tag is of its iCalendar
// without it, and an event which didn't change isn't updated. Its lines
// aren't kept, but where they are, and its size, and entity tag, are of
// them, as they're rendered.
func (self *events) file(p string, ev *event, src *source) {
	self.dir(path.Dir(p) + "/")
	f := &file{}
	var modified time.Time
	cal := &ical.Calendar{Name: src.name, TZ: src.tz}
	for _, c := range ev.components {
		for _, tzid := range c.tzids {
			if _, exists := src.zones[tzid]; exists && !slices.Contains(f.zones, tzid) {
				f.zones = append(f.zones, tzid)
			}
		}
	}
	prefix := src.prefix(f.zones)
	hash := sha256.New()
	hash.Write(prefix)
	size := len(prefix) + len(endCalendar)
	for _, c := range ev.components {
		if c.stamp.After(modified) {
			modified = c.stamp
		}
		hash.Write(c.sum[:])
		size += c.length
		f.parts = append(f.parts, c.part)
		cal.Events = append(cal.Events, c.event)
	}
	object := &schema.Object{
		ObjectKey:  schema.ObjectKey{Path: p},
		ObjectAttr: schema.ObjectAttr{Size: types.Ptr(int64(size)), ETag: types.Ptr(hex.EncodeToString(hash.Sum(nil)[:16])), ModTime: modified},
		ObjectMeta: schema.ObjectMeta{ContentType: cal.ContentType(), Meta: meta(ev, src.name)},
	}
	self.objects[p], self.files[p] = object, f
}

// render returns the iCalendar of an event's file, of its lines, which are
// read from the feed, which is the generation of the events
func (self *events) render(feed io.ReaderAt, f *file) ([]byte, error) {
	b := self.src.prefix(f.zones)
	for _, p := range f.parts {
		raw := make([]byte, p.n)
		if _, err := feed.ReadAt(raw, p.off); err != nil {
			return nil, err
		}
		out, _ := renderComponent(raw, p.stamp)
		b = append(b, out...)
	}
	return append(b, endCalendar...), nil
}

// get returns the object of a path, and the iCalendar of an event, or
// fs.ErrNotExist when there's none, of a folder's path, with or without "/"
// after it
func (self *events) get(op, p string) (*schema.Object, *file, error) {
	if p == "" {
		p = "/"
	}
	if object, exists := self.objects[p]; exists {
		return object, self.files[p], nil
	} else if object, exists := self.objects[p+"/"]; exists && !strings.HasSuffix(p, "/") {
		return object, nil, nil
	}
	return nil, nil, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
}

// listing returns the objects beneath a folder, of its children, or every
// one, when it's recursive, or only an event, of its path
func (self *events) listing(op, p string, recursive bool) ([]*schema.Object, error) {
	object, _, err := self.get(op, p)
	if err != nil {
		return nil, err
	} else if !object.IsDir {
		if strings.HasSuffix(p, "/") {
			return nil, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
		}
		return []*schema.Object{object}, nil
	}
	folder := object.Path
	var result []*schema.Object
	for _, child := range self.paths {
		if child == folder || !strings.HasPrefix(child, folder) {
			continue
		} else if rest := strings.TrimSuffix(strings.TrimPrefix(child, folder), "/"); !recursive && strings.Contains(rest, "/") {
			continue
		}
		result = append(result, self.objects[child])
	}
	return result, nil
}

////////////////////////////////////////////////////////////////////////////////
// ITERATOR

func (self *objectList) Next(ctx context.Context) (*schema.Object, error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	} else if len(self.objects) == 0 {
		return nil, io.EOF
	}
	object := self.objects[0]
	self.objects = self.objects[1:]
	return object, nil
}

func (self *objectList) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// meta returns the metadata of an event which its iCalendar doesn't have,
// as the backend's: its title, as dc:title, so it has one when it isn't
// enriched, its UID, and the calendar's name
func meta(ev *event, calendar string) []schema.Meta {
	var result []schema.Meta
	add := func(ns, key, value string) {
		if value == "" {
			return
		}
		if data, err := json.Marshal(value); err == nil {
			result = append(result, schema.Meta{Namespace: ns, Key: key, Value: data})
		}
	}
	add("dc", "title", ev.title)
	if uid := strings.TrimSpace(ev.components[0].event.UID); uid != "" {
		add("dc", "identifier", uid)
	}
	add(Scheme, "calendar", calendar)
	return result
}

// slug returns the name of an event, of its title: its letters and digits,
// in lower case, of words with "-" between them, of up to slugLength runes,
// or of a hash of its key, when it has none
func slug(title, key string) string {
	var b strings.Builder
	dash, n := false, 0
	for _, r := range strings.ToLower(title) {
		if n >= slugLength {
			break
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if dash && b.Len() > 0 {
				b.WriteByte('-')
				n++
			}
			b.WriteRune(r)
			n++
			dash = false
		} else {
			dash = true
		}
	}
	if b.Len() == 0 {
		return "event-" + hash(key)
	}
	return b.String()
}

// hash returns a short hash of values
func hash(values ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(values, "\x00")))
	return hex.EncodeToString(sum[:4])
}

// notModified returns ErrNotModified when the request's conditions say an
// object hasn't changed, of its entity tag, "*", or when it was modified
// (see schema.Object.NotModified)
func notModified(object *schema.Object, req schema.GetObjectRequest) error {
	if object.NotModified(req) {
		return gofiler.ErrNotModified.Withf("%s isn't modified", req.Path)
	}
	return nil
}

// isDir returns the error of reading a folder
func isDir(op, p string) error {
	return &fs.PathError{Op: op, Path: p, Err: fs.ErrInvalid}
}

// isContext returns true when the error is the context's
func isContext(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// isBadParameter returns true when the error is ErrBadParameter
func isBadParameter(err error) bool {
	return errors.Is(err, gofiler.ErrBadParameter)
}

// isStatus returns true when the error is a response's, of the status
func isStatus(err error, status int) bool {
	var e httpresponse.Err
	return errors.As(err, &e) && int(e) == status
}

// changes returns the changes of the events, of their UIDs, and of their
// folders, of their paths, of one fetch of a feed to the next, in the order
// of their paths: an event which is new is created, one which is gone is
// deleted, one whose iCalendar changed, of its entity tag, is updated, and
// one which moved, as its title, or day, changed, is deleted where it was,
// and created where it is. A path which is deleted, and created, of other
// events, is updated.
func changes(before, after *events) []schema.Event {
	created, updated, deleted := make(map[string]bool), make(map[string]bool), make(map[string]bool)
	for key, path := range after.keys {
		was, exists := before.keys[key]
		switch {
		case !exists:
			created[path] = true
		case was != path:
			deleted[was], created[path] = true, true
		case types.Value(before.objects[was].ETag) != types.Value(after.objects[path].ETag):
			updated[path] = true
		}
	}
	for key, was := range before.keys {
		if _, exists := after.keys[key]; !exists {
			deleted[was] = true
		}
	}
	for path := range created {
		if deleted[path] {
			delete(created, path)
			delete(deleted, path)
			updated[path] = true
		}
	}

	// The folders, of their paths
	for _, p := range after.paths {
		if after.objects[p].IsDir {
			if _, exists := before.objects[p]; !exists {
				created[p] = true
			}
		}
	}
	for _, p := range before.paths {
		if before.objects[p].IsDir {
			if _, exists := after.objects[p]; !exists {
				deleted[p] = true
			}
		}
	}

	var result []schema.Event
	add := func(paths map[string]bool, t schema.EventType) {
		for _, p := range slices.Sorted(maps.Keys(paths)) {
			result = append(result, schema.Event{EventType: t, ObjectKey: schema.ObjectKey{Path: p}})
		}
	}
	add(created, schema.EventTypeCreated)
	add(updated, schema.EventTypeUpdated)
	add(deleted, schema.EventTypeDeleted)
	return result
}
