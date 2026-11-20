package rss

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	rss "github.com/mutablelogic/go-rss"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// items are a feed's items, as files, of the folders of their days, by
// their paths, with the pages of the items, and the feed's title
type items struct {
	title   string
	objects map[string]*schema.Object // of their paths, folders with "/" after them
	content map[string]string         // the pages of the items, of their paths
	paths   []string                  // every path, in order
	keys    map[string]string         // the paths of the items, of their guids, or links, when they have none
}

// item is an item of a feed, with its path, which is of its day
type item struct {
	*rss.Item
	date  time.Time // when it was published, or zero
	title string
	key   string // what tells items of the same title, and day, apart
}

// objectList is a list of objects, as a listing's iterator
type objectList struct {
	mu      sync.Mutex
	objects []*schema.Object
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// undated is the folder of the items which aren't dated
	undated = "undated"

	// slugLength is the most runes of an item's title in its path
	slugLength = 64

	// contentType is the type of an item's page
	contentType = "text/html; charset=utf-8"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// newItems returns the items of a feed's channel, as files, at their days'
// paths, of the folders of their years and months, and those which aren't
// dated, of the folder undated. Items of the same title and day are told
// apart by a number, in the order of their guids, or links.
func newItems(channel *rss.Channel) *items {
	self := &items{title: strings.TrimSpace(channel.Title), objects: make(map[string]*schema.Object), content: make(map[string]string), keys: make(map[string]string)}
	self.dir("/")

	// The items, of their folders, in an order which doesn't change of the
	// feed's
	var list []item
	for _, i := range channel.Items {
		if i == nil {
			continue
		}
		it := item{Item: i, title: strings.TrimSpace(html.UnescapeString(i.Title))}
		if i.PubDate != nil {
			if t, err := i.PubDate.Parse(); err == nil {
				it.date = t.UTC()
			}
		}
		it.key = firstOf(guid(i), link(i), it.title, i.Description)
		list = append(list, it)
	}
	slices.SortStableFunc(list, func(a, b item) int { return strings.Compare(a.key, b.key) })

	// The paths of the items, of their folders, and those of the same name
	// are numbered, of the first number which no other item's path is, such
	// as of another's title which ends with a number
	taken := make(map[string]bool)
	for _, it := range list {
		folder, base := "/"+undated+"/", slug(it.title, it.key)
		if !it.date.IsZero() {
			folder = fmt.Sprintf("/%04d/%04d-%02d/", it.date.Year(), it.date.Year(), it.date.Month())
			base = fmt.Sprintf("%02d-%s", it.date.Day(), base)
		}
		name := folder + base + ".html"
		for n := 2; taken[name]; n++ {
			name = fmt.Sprintf("%s%s-%d.html", folder, base, n)
		}
		taken[name] = true
		self.file(name, it, channel)
		self.keys[it.key] = name
	}
	self.paths = slices.Sorted(func(yield func(string) bool) {
		for p := range self.objects {
			if !yield(p) {
				return
			}
		}
	})
	return self
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// dir adds a folder, and those above it
func (self *items) dir(p string) {
	for p != "" {
		if _, exists := self.objects[p]; exists {
			return
		}
		self.objects[p] = &schema.Object{ObjectKey: schema.ObjectKey{Path: p}, ObjectAttr: schema.ObjectAttr{IsDir: true}, ObjectMeta: schema.ObjectMeta{ContentType: "inode/directory"}}
		if p == "/" {
			return
		}
		p = path.Dir(strings.TrimSuffix(p, "/"))
		if p != "/" {
			p += "/"
		}
	}
}

// file adds an item, as the page of it, at the path, with its folders
func (self *items) file(p string, it item, channel *rss.Channel) {
	self.dir(path.Dir(p) + "/")
	content, metadata := page(it, channel), meta(it, channel)

	// Its entity tag is of its page, and its metadata, which the page
	// doesn't have, such as a podcast's episode, so a change of either is a
	// change of the file
	hash := sha256.New()
	hash.Write([]byte(content))
	if data, err := json.Marshal(metadata); err == nil {
		hash.Write(data)
	}
	object := &schema.Object{
		ObjectKey:  schema.ObjectKey{Path: p},
		ObjectAttr: schema.ObjectAttr{Size: types.Ptr(int64(len(content))), ETag: types.Ptr(hex.EncodeToString(hash.Sum(nil)[:16])), ModTime: it.date},
		ObjectMeta: schema.ObjectMeta{ContentType: contentType, Meta: metadata},
	}
	self.objects[p], self.content[p] = object, content
}

// get returns the object of a path, and the page of an item, or
// fs.ErrNotExist when there's none, of a folder's path, with or without "/"
// after it
func (self *items) get(op, p string) (*schema.Object, string, error) {
	if p == "" {
		p = "/"
	}
	if object, exists := self.objects[p]; exists {
		return object, self.content[p], nil
	} else if object, exists := self.objects[p+"/"]; exists && !strings.HasSuffix(p, "/") {
		return object, "", nil
	}
	return nil, "", &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
}

// listing returns the objects beneath a folder, of its children, or every
// one, when it's recursive, or only an item, of its path
func (self *items) listing(op, p string, recursive bool) ([]*schema.Object, error) {
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

// page returns the HTML page of an item: its title, author, date, and link,
// and its description, which is the feed's HTML, as it is
func page(it item, channel *rss.Channel) string {
	var b strings.Builder
	esc := html.EscapeString
	b.WriteString("<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n")
	fmt.Fprintf(&b, "<title>%s</title>\n", esc(it.title))
	if author := author(it); author != "" {
		fmt.Fprintf(&b, "<meta name=\"author\" content=\"%s\">\n", esc(author))
	}
	if !it.date.IsZero() {
		fmt.Fprintf(&b, "<meta name=\"date\" content=\"%s\">\n", it.date.Format(time.RFC3339))
	}
	if l := link(it.Item); l != "" {
		fmt.Fprintf(&b, "<link rel=\"canonical\" href=\"%s\">\n", esc(l))
	}
	b.WriteString("</head>\n<body>\n")
	fmt.Fprintf(&b, "<h1>%s</h1>\n", esc(it.title))
	if title := strings.TrimSpace(channel.Title); title != "" {
		fmt.Fprintf(&b, "<p>%s</p>\n", esc(title))
	}
	if it.Description != "" {
		b.WriteString(it.Description)
		b.WriteString("\n")
	}
	if e := it.Enclosure; e != nil && e.URL != "" {
		fmt.Fprintf(&b, "<p><a href=\"%s\">%s</a></p>\n", esc(e.URL), esc(firstOf(e.Type, e.URL)))
	}
	b.WriteString("</body>\n</html>\n")
	return b.String()
}

// meta returns the metadata of an item, which its page doesn't have of its
// own, as the backend's: its title, author, date, its identifier, which is
// its guid, or its link, when it has none, its link, its feed, and its
// enclosure, with a podcast's episode, season and duration
func meta(it item, channel *rss.Channel) []schema.Meta {
	var result []schema.Meta
	add := func(ns, key string, value any) {
		if s, ok := value.(string); ok && s == "" {
			return
		}
		if data, err := json.Marshal(value); err == nil {
			result = append(result, schema.Meta{Namespace: ns, Key: key, Value: data})
		}
	}
	add("dc", "title", it.title)
	add("dc", "creator", author(it))
	if !it.date.IsZero() {
		add("dc", "date", it.date.Format(time.RFC3339))
	}
	add("dc", "identifier", firstOf(guid(it.Item), link(it.Item)))
	if subjects := categories(it.Categories); len(subjects) > 0 {
		add("dc", "subject", subjects)
	}
	add("dc", "language", strings.TrimSpace(channel.Language))
	add("dc", "rights", strings.TrimSpace(channel.Copyright))
	add("rss", "url", link(it.Item))
	add("rss", "comments", strings.TrimSpace(it.Comments))
	add("rss", "feed", strings.TrimSpace(channel.Title))
	add("rss", "image", image(it.Item, channel))
	if e := it.Enclosure; e != nil && e.URL != "" {
		enclosure := map[string]any{"url": e.URL}
		if e.Type != "" {
			enclosure["type"] = e.Type
		}
		if n, err := strconv.ParseInt(e.Length, 10, 64); err == nil && n > 0 {
			enclosure["length"] = n
		}
		add("rss", "enclosure", enclosure)
	}
	add("rss", "episode", strings.TrimSpace(it.Episode))
	add("rss", "season", strings.TrimSpace(it.Season))
	add("rss", "episode_type", strings.TrimSpace(it.EpisodeType))
	if it.Duration != nil {
		if d, err := it.Duration.Seconds(); err == nil && d > 0 {
			add("rss", "duration", d.Seconds())
		}
	}
	return result
}

// categories returns the names of categories, in order, without those which
// are repeated, or empty
func categories(list []*rss.Category) []string {
	var result []string
	for _, c := range list {
		if c == nil {
			continue
		}
		if name := strings.TrimSpace(html.UnescapeString(firstOf(c.Value, c.Text))); name != "" && !slices.Contains(result, name) {
			result = append(result, name)
		}
	}
	return result
}

// image returns the URL of an item's image, of a podcast's episode, or of
// its feed, or empty
func image(i *rss.Item, channel *rss.Channel) string {
	for _, img := range []*rss.Image{i.Image, channel.Image} {
		if img != nil {
			if u := strings.TrimSpace(firstOf(img.HRef, img.URL)); u != "" {
				return u
			}
		}
	}
	return ""
}

// author returns the author of an item
func author(it item) string {
	return strings.TrimSpace(it.Author)
}

// guid returns the guid of an item
func guid(i *rss.Item) string {
	if i.GUID == nil {
		return ""
	}
	return strings.TrimSpace(i.GUID.Value)
}

// link returns the link of an item, of its first link which has one
func link(i *rss.Item) string {
	for _, l := range i.Link {
		if l != nil {
			if v := strings.TrimSpace(firstOf(l.Value, l.URL)); v != "" {
				return v
			}
		}
	}
	return ""
}

// slug returns the name of an item, of its title: its letters and digits,
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
		sum := sha256.Sum256([]byte(key))
		return "item-" + hex.EncodeToString(sum[:4])
	}
	return b.String()
}

// firstOf returns the first value which isn't empty
func firstOf(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
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

// changes returns the changes of the items, of their guids, or links, when
// they have none, and of their folders, of their paths, of one fetch of a
// feed to the next, in the order of their paths: an item which is new is
// created, one which is gone is deleted, one whose page changed, of its
// entity tag, is updated, and one which moved, as its title, or date,
// changed, is deleted where it was, and created where it is. A path which
// is deleted, and created, of other items, is updated.
func changes(before, after *items) []schema.Event {
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
