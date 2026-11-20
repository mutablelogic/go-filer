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
	self := &items{title: strings.TrimSpace(channel.Title), objects: make(map[string]*schema.Object), content: make(map[string]string)}
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
	// are numbered
	taken := make(map[string]int)
	for _, it := range list {
		folder, base := "/"+undated+"/", slug(it.title, it.key)
		if !it.date.IsZero() {
			folder = fmt.Sprintf("/%04d/%04d-%02d/", it.date.Year(), it.date.Year(), it.date.Month())
			base = fmt.Sprintf("%02d-%s", it.date.Day(), base)
		}
		name := folder + base
		if n := taken[name]; n > 0 {
			taken[name]++
			name = fmt.Sprintf("%s-%d", name, n+1)
		} else {
			taken[name] = 1
		}
		self.file(name+".html", it, channel)
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
	content := page(it, channel)
	sum := sha256.Sum256([]byte(content))
	object := &schema.Object{
		ObjectKey:  schema.ObjectKey{Path: p},
		ObjectAttr: schema.ObjectAttr{Size: types.Ptr(int64(len(content))), ETag: types.Ptr(hex.EncodeToString(sum[:16])), ModTime: it.date},
		ObjectMeta: schema.ObjectMeta{ContentType: contentType, Meta: meta(it, channel)},
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
// own, as the backend's: its title, author, date, link and guid, its feed,
// and its enclosure, with a podcast's episode, season and duration
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
	add("dc", "identifier", link(it.Item))
	add("rss", "guid", guid(it.Item))
	add("rss", "feed", strings.TrimSpace(channel.Title))
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
	if it.Duration != nil {
		if d, err := it.Duration.Seconds(); err == nil && d > 0 {
			add("rss", "duration", d.Seconds())
		}
	}
	return result
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

// notModified returns ErrNotModified when an object's entity tag is one of
// those of the request
func notModified(object *schema.Object, req schema.GetObjectRequest) error {
	if object.ETag != nil && slices.Contains(req.IfNoneMatch, *object.ETag) {
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
