// Package rss is a backend of an RSS feed, of rss://<name>/<host>/<path>,
// whose items are files, of their days, /YYYY/YYYY-MM/DD-<title>.html, each
// an HTML page of the item, with its title, author, date and link as its
// metadata, and its enclosure, such as a podcast's episode, too. It's read
// only, and is polled, at the feed's ttl, or its refresh interval, for the
// changes to its items, of their guids. It's a backend which isn't built
// into the registry, which a program adds with
// registry.WithBackend(rss.Backend()).
package rss

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	rss "github.com/mutablelogic/go-rss"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Filer is the backend of an RSS feed
type Filer struct {
	name        string
	feed        *url.URL // the URL of the feed
	description string
	refresh     time.Duration // the refresh interval of the option, or zero, when it isn't set
	opts        gofiler.Opts
	cache       gofiler.CacheStore // the volume's cache, which the registry owns
	client      *client.Client

	mu           sync.Mutex
	items        *items        // the feed's items, as files, when it's fetched
	fetched      time.Time     // when it was fetched
	ttl          time.Duration // the feed's ttl, of how long it can be kept, or zero
	etag         string        // the entity tag of the feed, which it's fetched again of, if it changed
	lastModified string        // when the feed was last modified, which it's fetched again of, too
	restored     bool          // the feed was got from the volume's cache, or there was none
	baseline     *items        // the items which were got from the volume's cache, which a watch compares with, first
	now          func() time.Time
}

var _ gofiler.Filer = (*Filer)(nil)

// feed decodes a feed of a response, of go-rss, with its entity tag, and
// when it was last modified, of its headers, and saves it, as it's read, in
// the volume's cache, of them
type feed struct {
	*rss.Feed
	etag, lastModified string
	ctx                context.Context
	cache              gofiler.CacheStore
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// feedKey is the key of the feed in the volume's cache, whose version is
	// its entity tag, and lastModifiedValue the value of when it was last
	// modified
	feedKey           = "feed"
	lastModifiedValue = "last-modified"

	// checkedKey is the key of when the feed was last fetched, or found not
	// to be modified, in the volume's cache
	checkedKey = "feed-checked"

	// Scheme is the scheme of a feed's URL, rss://<name>/<host>/<path>
	Scheme = "rss"

	// Description is what the backend is
	Description = "RSS feed"

	paramName     = "name"
	paramRefresh  = "refresh_interval"
	paramReadonly = "readonly"
	paramInsecure = "insecure"
	paramIndex    = "index"

	// DefaultRefresh is how long a feed which is fetched is kept, and how
	// often it's polled, when it has no ttl, and the refresh_interval
	// option isn't set
	DefaultRefresh = 5 * time.Minute

	// accept is what's accepted of a feed's server
	accept = "application/rss+xml, application/xml;q=0.9, text/xml;q=0.9, */*;q=0.1"
)

// options are the volume's options, of a URL's query, and the rest are the
// feed's own
var options = map[string]bool{paramName: true, paramRefresh: true, paramReadonly: true, paramInsecure: true, paramIndex: true, schema.OptDescription: true}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Open creates the backend of the feed of the URL, rss://<name>/<host>/<path>,
// whose name is its name option, or its host, which its Backend opens.
// The feed is https://<host>/<path>, or http, with insecure=true, with the
// parameters of the URL's query which aren't the volume's options: name,
// refresh_interval, readonly, insecure and description. The feed is fetched,
// so one which can't be is an error, and is described of its title, unless
// it has a description option.
func Open(ctx context.Context, u *url.URL, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
	if u == nil || u.Scheme != Scheme {
		return nil, gofiler.ErrBadParameter.With("not a feed's url")
	}
	name := u.Query().Get(paramName)
	if name == "" {
		name = u.Host
	}

	// The feed's host, and path, of the URL's path, and its query, of the
	// parameters which aren't the volume's
	host, path, _ := strings.Cut(strings.TrimPrefix(u.EscapedPath(), "/"), "/")
	if host == "" {
		return nil, gofiler.ErrBadParameter.Withf("a feed's url is rss://<name>/<host>/<path>: %q", u.String())
	}
	params, query := url.Values{}, url.Values{}
	for key, values := range u.Query() {
		if options[key] {
			params[key] = values
		} else {
			query[key] = values
		}
	}
	scheme := "https"
	if insecure := params.Get(paramInsecure); insecure == "true" {
		scheme = "http"
	} else if insecure != "" && insecure != "false" {
		return nil, gofiler.ErrBadParameter.Withf("%s: %q", paramInsecure, insecure)
	}
	feed, err := url.Parse(scheme + "://" + host + "/" + path)
	if err != nil || feed.Host == "" {
		return nil, gofiler.ErrBadParameter.Withf("a feed's url is rss://<name>/<host>/<path>: %q", u.String())
	}
	feed.RawQuery = query.Encode()
	return New(ctx, name, feed, params, cache, opts)
}

// New creates the backend of a feed, of its URL, the volume's options, its
// refresh_interval, and its description, and its cache
func New(ctx context.Context, name string, feed *url.URL, params url.Values, cache gofiler.CacheStore, opts gofiler.Opts) (*Filer, error) {
	if cache == nil {
		return nil, gofiler.ErrBadParameter.Withf("volume %q has no cache", name)
	} else if !types.IsIdentifier(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	} else if feed == nil || (feed.Scheme != "http" && feed.Scheme != "https") || feed.Host == "" {
		return nil, gofiler.ErrBadParameter.With("the url of a feed is http or https")
	}
	f := *feed
	self := &Filer{name: name, feed: &f, opts: opts, cache: cache, now: time.Now, description: params.Get(schema.OptDescription)}
	if value := params.Get(paramRefresh); value != "" {
		if d, err := time.ParseDuration(value); err != nil || d < 0 {
			return nil, gofiler.ErrBadParameter.Withf("%s: %q", paramRefresh, value)
		} else {
			self.refresh = d
		}
	}
	if value := params.Get(paramReadonly); value != "" && value != "true" {
		return nil, gofiler.ErrBadParameter.With("a feed is read-only")
	}
	c, err := client.New(append(append([]client.ClientOpt(nil), opts.ClientOpts...), client.OptEndpoint(self.feed.String()))...)
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("feed: %v", err)
	}
	self.client = c

	// The feed is fetched, so one which can't be isn't a volume, and is
	// described of its title
	items, err := self.list(ctx, "open", "/")
	if err != nil {
		return nil, err
	}
	if self.description == "" {
		self.description = "RSS feed " + items.title
		if items.title == "" {
			self.description = "RSS feed " + self.feed.String()
		}
	}
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Actions returns nil, as a feed is read-only
func (self *Filer) Actions() []gofiler.Action {
	return nil
}

// Description returns what the feed is, such as "RSS feed The Example Podcast"
func (self *Filer) Description() string {
	return self.description
}

// URL returns the URL of the feed's volume, rss://<name>/<host>/<path>, with
// its options, and the feed's own parameters
func (self *Filer) URL() *url.URL {
	query := self.feed.Query()
	for key, values := range self.params() {
		query[key] = values
	}
	return &url.URL{Scheme: Scheme, Host: self.name, Path: "/" + self.feed.Host + self.feed.Path, RawPath: "/" + self.feed.Host + self.feed.EscapedPath(), RawQuery: query.Encode()}
}

// Clone returns a new backend of the feed, with its options changed by the
// patch, which fetches it, as a new one does
func (self *Filer) Clone(ctx context.Context, patch url.Values) (gofiler.Filer, error) {
	u := self.URL()
	u.RawQuery = gofiler.PatchValues(u.Query(), patch).Encode()
	return Open(ctx, u, self.cache, self.opts)
}

// GetObject returns an item, or a folder of its days
func (self *Filer) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	items, err := self.list(ctx, "get", req.Path)
	if err != nil {
		return nil, err
	}
	object, _, err := items.get("get", req.Path)
	if err != nil {
		return nil, err
	}
	return self.named(object), notModified(object, req)
}

// ReadObject returns the HTML page of an item
func (self *Filer) ReadObject(ctx context.Context, req schema.GetObjectRequest) (gofiler.ReadObjectFunc, *schema.Object, error) {
	items, err := self.list(ctx, "read", req.Path)
	if err != nil {
		return nil, nil, err
	}
	object, content, err := items.get("read", req.Path)
	if err != nil {
		return nil, nil, err
	} else if object.IsDir {
		return nil, nil, isDir("read", req.Path)
	} else if err := notModified(object, req); err != nil {
		return nil, nil, err
	}
	return func(ctx context.Context, fn func(io.Reader) error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(strings.NewReader(content))
	}, self.named(object), nil
}

// ListObjects lists the folders of the days of the items, and the items, of
// a folder, or every one beneath it, when it's recursive, or only an item,
// in the order of their paths, after the path it starts after
func (self *Filer) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	items, err := self.list(ctx, "list", req.Path)
	if err != nil {
		return nil, err
	}
	objects, err := items.listing("list", req.Path, req.Recursive)
	if err != nil {
		return nil, err
	}
	result := make([]*schema.Object, 0, len(objects))
	for _, object := range objects {
		if object.Path > req.StartAfter {
			result = append(result, self.named(object))
		}
	}
	return &objectList{objects: result}, nil
}

// CreateObject isn't allowed, as a feed is read-only
func (self *Filer) CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error) {
	return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
}

// DeleteObjects isn't allowed, as a feed is read-only
func (self *Filer) DeleteObjects(context.Context, schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
}

// WatchObjects polls the feed, at its interval, and sends the changes to
// its items, and their folders: a rescan of the root first, once it's
// watched, and then the paths which are created, updated, of their entity
// tags, or deleted, of each time it changed. A feed which isn't modified,
// of its entity tag, or when it was last modified, isn't fetched again.
func (self *Filer) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	before, err := self.list(ctx, "watch", "/")
	if err != nil {
		return err
	}

	// The items which were got from the volume's cache, such as before a
	// restart, which the feed may have been fetched again since, are
	// compared with, first, so what changed meanwhile is sent, after the
	// rescan
	self.mu.Lock()
	baseline := self.baseline
	self.baseline = nil
	self.mu.Unlock()
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
	} else if baseline != nil && baseline != before {
		for _, event := range changes(baseline, before) {
			if err := send(event); err != nil {
				return err
			}
		}
	}
	for {
		timer := time.NewTimer(self.interval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		after, changed, err := self.fetch(ctx, "watch", "/")
		if err != nil {
			return err
		} else if !changed {
			continue
		}
		for _, event := range changes(before, after) {
			event.Name = self.name
			if err := send(event); err != nil {
				return err
			}
		}
		before = after
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// params returns each of the backend's options, so its URL says how it's set
// up, of its refresh interval, only when it's set, so the feed's ttl can
// change
func (self *Filer) params() url.Values {
	params := url.Values{
		paramReadonly:         {"true"},
		schema.OptDescription: {self.description},
	}
	if self.feed.Scheme == "http" {
		params.Set(paramInsecure, "true")
	}
	if self.refresh > 0 {
		params.Set(paramRefresh, self.refresh.String())
	}
	return params
}

// interval returns how long the feed which is fetched is kept, and how
// often it's polled: the refresh interval of the option, but not less than
// the feed's ttl, or the ttl, or DefaultRefresh, without either
func (self *Filer) interval() time.Duration {
	self.mu.Lock()
	defer self.mu.Unlock()
	return self.intervalLocked()
}

func (self *Filer) intervalLocked() time.Duration {
	switch {
	case self.refresh > 0:
		return max(self.refresh, self.ttl)
	case self.ttl > 0:
		return self.ttl
	default:
		return DefaultRefresh
	}
}

// list returns the feed's items, which are fetched, unless they were within
// the interval
func (self *Filer) list(ctx context.Context, op, path string) (*items, error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.restoreLocked(ctx)
	if self.items != nil && self.now().Sub(self.fetched) < self.intervalLocked() {
		return self.items, nil
	}
	items, _, err := self.fetchLocked(ctx, op, path)
	return items, err
}

// fetch fetches the feed, and returns its items, and whether they changed,
// which they didn't when it isn't modified
func (self *Filer) fetch(ctx context.Context, op, path string) (*items, bool, error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.restoreLocked(ctx)
	return self.fetchLocked(ctx, op, path)
}

// restoreLocked gets the feed which was saved in the volume's cache, such as
// before a restart, once, when it hasn't been fetched, of when it was
// fetched, and its entity tag, and when it was last modified, so it's
// fetched again when it would have been, of them, which it isn't, when it
// isn't modified. A feed which can't be read of it is fetched. It must be
// locked.
func (self *Filer) restoreLocked(ctx context.Context) {
	if self.restored || self.items != nil {
		return
	}
	self.restored = true
	entry, err := self.cache.Get(ctx, feedKey)
	if err != nil {
		return
	}
	defer entry.Close()
	parsed, err := rss.Read(entry)
	if err != nil || parsed == nil || parsed.Channel == nil {
		return
	}
	meta := entry.Meta()
	self.channelLocked(parsed.Channel)
	self.etag, self.lastModified = meta.Version, meta.Values[lastModifiedValue]
	self.items, self.fetched = newItems(parsed.Channel), entry.Stored()
	self.baseline = self.items

	// It was last checked when it was saved, or, of a 304, after
	var checked time.Time
	if _, err := store.GetCacheJSON(ctx, self.cache, checkedKey, &checked); err == nil && checked.After(self.fetched) {
		self.fetched = checked
	}
}

// checkedLocked saves when the feed was last fetched, or found not to be
// modified, in the volume's cache, so a restart fetches it again when it
// would have been, which isn't an error when it can't
func (self *Filer) checkedLocked(ctx context.Context) {
	_ = store.PutCacheJSON(ctx, self.cache, checkedKey, 0, gofiler.CacheMeta{}, self.fetched)
}

// channelLocked sets the feed's ttl, of its channel, which must be locked
func (self *Filer) channelLocked(channel *rss.Channel) {
	self.ttl = 0
	if channel.TTL != nil {
		if ttl, err := channel.TTL.Minutes(); err == nil && ttl > 0 {
			self.ttl = ttl
		}
	}
}

// fetchLocked fetches the feed, unless it isn't modified, of its entity tag,
// or when it was last modified, when its items were fetched before, which
// must be locked
func (self *Filer) fetchLocked(ctx context.Context, op, path string) (*items, bool, error) {
	var opts []client.RequestOpt
	if self.items != nil && self.etag != "" {
		opts = append(opts, client.OptReqHeader("If-None-Match", self.etag))
	}
	if self.items != nil && self.lastModified != "" {
		opts = append(opts, client.OptReqHeader("If-Modified-Since", self.lastModified))
	}
	response := feed{ctx: ctx, cache: self.cache}
	if err := self.client.DoWithContext(ctx, client.NewRequestEx(http.MethodGet, accept), &response, opts...); self.items != nil && isStatus(err, http.StatusNotModified) {
		self.fetched = self.now()
		self.checkedLocked(ctx)
		return self.items, false, nil
	} else if err != nil {
		return nil, false, fetchError(op, path, self.feed.String(), err)
	} else if response.Feed == nil || response.Channel == nil {
		return nil, false, gofiler.ErrBadParameter.Withf("%s %s: %s isn't an RSS feed", op, path, self.feed.String())
	}
	self.channelLocked(response.Channel)
	self.etag, self.lastModified = response.etag, response.lastModified
	self.items, self.fetched = newItems(response.Channel), self.now()
	self.checkedLocked(ctx)
	return self.items, true, nil
}

// named returns a copy of an object, with the volume's name
func (self *Filer) named(object *schema.Object) *schema.Object {
	result := *object
	result.Name = self.name
	return &result
}

// Unmarshal decodes a response, as a feed, of go-rss, and saves it, as it's
// read, in the volume's cache, of its entity tag, and when it was last
// modified, once it's all read, unless it can't be decoded, so the one which
// was saved is kept. The cache is never the only copy, so a feed which can't
// be saved is decoded, as it would be without it.
func (f *feed) Unmarshal(header http.Header, r io.Reader) error {
	f.etag, f.lastModified = header.Get("ETag"), header.Get("Last-Modified")
	pr, pw := io.Pipe()
	saved := make(chan struct{})
	go func() {
		defer close(saved)
		meta := gofiler.CacheMeta{Version: f.etag, Values: map[string]string{lastModifiedValue: f.lastModified}}
		entry, err := f.cache.Put(f.ctx, feedKey, 0, meta, pr)
		if err == nil {
			entry.Close()
		}
		pr.CloseWithError(err)
	}()
	w := &bestEffort{w: pw}
	parsed, err := rss.Read(io.TeeReader(r, w))
	if err == nil {
		// The rest of the response, such as whitespace, after the feed
		_, err = io.Copy(w, r)
	}
	if err != nil {
		// Wrapped, so an error which is io.EOF, such as of an empty response,
		// isn't the end of what's saved, which would replace the feed which
		// was saved with it
		pw.CloseWithError(fmt.Errorf("the feed can't be read: %w", err))
		<-saved
		return gofiler.ErrBadParameter.Withf("the feed can't be read: %v", err)
	}
	pw.Close()
	<-saved
	f.Feed = parsed
	return nil
}

// bestEffort writes to a writer, until it can't, after which it discards
// what it's written, so what reads, and writes to it, isn't stopped
type bestEffort struct {
	w      io.Writer
	failed bool
}

func (b *bestEffort) Write(p []byte) (int, error) {
	if !b.failed {
		if _, err := b.w.Write(p); err != nil {
			b.failed = true
		}
	}
	return len(p), nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// fetchError returns why a feed can't be fetched: a context which is
// cancelled, or done, a feed which can't be read, or isn't found, which
// won't change, or otherwise a server which is unavailable, which is tried
// again
func fetchError(op, path, feed string, err error) error {
	switch {
	case isContext(err):
		return err
	case isBadParameter(err):
		return err
	case isStatus(err, http.StatusNotFound), isStatus(err, http.StatusGone):
		return gofiler.ErrBadParameter.Withf("%s %s: the feed %s isn't found", op, path, feed)
	default:
		return gofiler.ErrServiceUnavailable.Withf("%s %s: %v", op, path, err)
	}
}
