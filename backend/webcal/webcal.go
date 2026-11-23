// Package webcal is a backend of an iCalendar feed, which a calendar is
// subscribed to, of webcal://<name>/<host>/<path>, whose events are files, of
// their days, /YYYY/YYYY-MM/DD-<title>.ics, each an iCalendar of the event,
// with its changes to the times it repeats. It's read-only, and is polled,
// at its refresh interval, for the changes to its events, of their UIDs. A
// feed which isn't modified, of its entity tag, or when it was last
// modified, isn't fetched again.
package webcal

import (
	"bytes"
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
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Filer is the backend of an iCalendar feed
type Filer struct {
	name        string
	feed        *url.URL // the URL of the feed
	description string
	refresh     time.Duration // how often it's polled
	opts        gofiler.Opts
	cache       gofiler.CacheStore // the volume's cache, which the registry owns
	client      *client.Client

	mu           sync.Mutex
	events       *events   // the feed's events, as files, when it's fetched
	fetched      time.Time // when it was fetched, or found not to be modified
	etag         string    // the entity tag of the feed, which it's fetched again of, if it changed
	lastModified string    // when the feed was last modified, which it's fetched again of, too
	restored     bool      // the feed was got from the volume's cache, or there was none
	baseline     *events   // the events which were got from the volume's cache, which a watch compares with, first
	now          func() time.Time
}

var _ gofiler.Filer = (*Filer)(nil)

// feed decodes a feed of a response, as a calendar, with its entity tag,
// and when it was last modified, of its headers, and saves it, as it's
// read, in the volume's cache, of them
type feed struct {
	source             *source
	etag, lastModified string
	ctx                context.Context
	cache              gofiler.CacheStore
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// Scheme is the scheme of a feed's volume's URL,
	// webcal://<name>/<host>/<path>, which is a feed's own scheme too, as a
	// calendar's "Subscribe" links have, webcal://<host>/<path>
	Scheme = "webcal"

	// Description is what the backend is
	Description = "Calendar subscription"

	// DefaultRefresh is how often a feed is polled, and how long it's kept,
	// when the refresh_interval option isn't set
	DefaultRefresh = 5 * time.Minute

	// feedKey is the key of the feed in the volume's cache, whose version is
	// its entity tag, and lastModifiedValue the value of when it was last
	// modified, and checkedKey the key of when it was last fetched, or found
	// not to be modified
	feedKey           = "feed"
	lastModifiedValue = "last-modified"
	checkedKey        = "feed-checked"

	paramName     = "name"
	paramRefresh  = "refresh_interval"
	paramReadonly = "readonly"
	paramInsecure = "insecure"
	paramIndex    = "index"

	// accept is what's accepted of a feed's server
	accept = "text/calendar, */*;q=0.1"
)

// options are the volume's options, of a URL's query, and the rest are the
// feed's own
var options = map[string]bool{paramName: true, paramRefresh: true, paramReadonly: true, paramInsecure: true, paramIndex: true, schema.OptDescription: true}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Open creates the backend of the feed of the URL,
// webcal://<name>/<host>/<path>, whose name is its name option, or its host.
// The feed is https://<host>/<path>, or http, with insecure=true, with the
// parameters of the URL's query which aren't the volume's options: name,
// refresh_interval, readonly, insecure, index and description. The feed is
// fetched, so one which can't be is an error, and is described of its name,
// unless it has a description option.
func Open(ctx context.Context, u *url.URL, cache gofiler.CacheStore, opts gofiler.Opts) (gofiler.Filer, error) {
	if u == nil || u.Scheme != Scheme {
		return nil, gofiler.ErrBadParameter.With("not a calendar feed's url")
	}
	name := u.Query().Get(paramName)
	if name == "" {
		name = u.Host
	}

	// The feed's host, and path, of the URL's path, and its query, of the
	// parameters which aren't the volume's
	host, path, _ := strings.Cut(strings.TrimPrefix(u.EscapedPath(), "/"), "/")
	if host == "" {
		return nil, gofiler.ErrBadParameter.Withf("a calendar feed's url is webcal://<name>/<host>/<path>: %q", u.String())
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
		return nil, gofiler.ErrBadParameter.Withf("a calendar feed's url is webcal://<name>/<host>/<path>: %q", u.String())
	}
	feed.RawQuery = query.Encode()
	return New(ctx, name, feed, params, cache, opts)
}

// New creates the backend of a feed, of its URL, which is http or https, the
// volume's options, its refresh_interval, and its description, and its
// cache
func New(ctx context.Context, name string, feed *url.URL, params url.Values, cache gofiler.CacheStore, opts gofiler.Opts) (*Filer, error) {
	if cache == nil {
		return nil, gofiler.ErrBadParameter.Withf("volume %q has no cache", name)
	} else if !types.IsIdentifier(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	} else if feed == nil || (feed.Scheme != "http" && feed.Scheme != "https") || feed.Host == "" {
		return nil, gofiler.ErrBadParameter.With("the url of a calendar feed is http or https")
	}
	f := *feed
	self := &Filer{name: name, feed: &f, opts: opts, cache: cache, now: time.Now, refresh: DefaultRefresh, description: params.Get(schema.OptDescription)}
	if value := params.Get(paramRefresh); value != "" {
		if d, err := time.ParseDuration(value); err != nil || d <= 0 {
			return nil, gofiler.ErrBadParameter.Withf("%s: %q", paramRefresh, value)
		} else {
			self.refresh = d
		}
	}
	if value := params.Get(paramReadonly); value != "" && value != "true" {
		return nil, gofiler.ErrBadParameter.With("a calendar feed is read-only")
	}
	c, err := client.New(append(append([]client.ClientOpt(nil), opts.ClientOpts...), client.OptEndpoint(self.feed.String()))...)
	if err != nil {
		return nil, gofiler.ErrBadParameter.Withf("calendar feed: %v", err)
	}
	self.client = c

	// The feed is fetched, so one which can't be isn't a volume, and is
	// described of its name
	events, err := self.list(ctx, "open", "/")
	if err != nil {
		return nil, err
	}
	if self.description == "" {
		self.description = "Calendar " + events.title
		if events.title == "" {
			self.description = "Calendar " + self.feed.String()
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

// Description returns what the feed is, such as "Calendar UK Holidays"
func (self *Filer) Description() string {
	return self.description
}

// URL returns the URL of the feed's volume, webcal://<name>/<host>/<path>,
// with its options, and the feed's own parameters
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

// GetObject returns an event, or a folder of its days
func (self *Filer) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	events, err := self.list(ctx, "get", req.Path)
	if err != nil {
		return nil, err
	}
	object, _, err := events.get("get", req.Path)
	if err != nil {
		return nil, err
	}
	return self.named(object), notModified(object, req)
}

// ReadObject returns the iCalendar of an event
func (self *Filer) ReadObject(ctx context.Context, req schema.GetObjectRequest) (gofiler.ReadObjectFunc, *schema.Object, error) {
	events, err := self.list(ctx, "read", req.Path)
	if err != nil {
		return nil, nil, err
	}
	object, content, err := events.get("read", req.Path)
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

// ListObjects lists the folders of the days of the events, and the events,
// of a folder, or every one beneath it, when it's recursive, or only an
// event, in the order of their paths, after the path it starts after
func (self *Filer) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	events, err := self.list(ctx, "list", req.Path)
	if err != nil {
		return nil, err
	}
	objects, err := events.listing("list", req.Path, req.Recursive)
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

// WatchObjects polls the feed, at its refresh interval, and sends the
// changes to its events, and their folders: a rescan of the root first, once
// it's watched, and then the paths which are created, updated, of their
// entity tags, or deleted, of each time it changed. A feed which isn't
// modified, of its entity tag, or when it was last modified, isn't fetched
// again.
func (self *Filer) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	before, err := self.list(ctx, "watch", "/")
	if err != nil {
		return err
	}

	// The events which were got from the volume's cache, such as before a
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
			event.Name = self.name
			if err := send(event); err != nil {
				return err
			}
		}
	}
	for {
		timer := time.NewTimer(self.refresh)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		// The events are compared with those this watch last sent, rather
		// than of whether the feed was modified, as a read, or a listing,
		// may have fetched it since, so a 304 is of events which changed
		after, err := self.fetch(ctx, "watch", "/")
		if err != nil {
			return err
		} else if after == before {
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
// up
func (self *Filer) params() url.Values {
	params := url.Values{
		paramReadonly:         {"true"},
		paramRefresh:          {self.refresh.String()},
		schema.OptDescription: {self.description},
	}
	if self.feed.Scheme == "http" {
		params.Set(paramInsecure, "true")
	}
	return params
}

// list returns the feed's events, which are fetched, unless they were
// within the refresh interval
func (self *Filer) list(ctx context.Context, op, path string) (*events, error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.restoreLocked(ctx)
	if self.events != nil && self.now().Sub(self.fetched) < self.refresh {
		return self.events, nil
	}
	events, _, err := self.fetchLocked(ctx, op, path)
	return events, err
}

// fetch fetches the feed, and returns its events, which are those it had,
// when it isn't modified
func (self *Filer) fetch(ctx context.Context, op, path string) (*events, error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.restoreLocked(ctx)
	events, _, err := self.fetchLocked(ctx, op, path)
	return events, err
}

// restoreLocked gets the feed which was saved in the volume's cache, such as
// before a restart, once, when it hasn't been fetched, of when it was
// fetched, and its entity tag, and when it was last modified, so it's
// fetched again when it would have been, of them, which it isn't, when it
// isn't modified. A feed which can't be read of it is fetched. It must be
// locked.
func (self *Filer) restoreLocked(ctx context.Context) {
	if self.restored || self.events != nil {
		return
	}
	self.restored = true
	entry, err := self.cache.Get(ctx, feedKey)
	if err != nil {
		return
	}
	defer entry.Close()
	data, err := io.ReadAll(io.LimitReader(entry, maxFeed+1))
	if err != nil || len(data) > maxFeed {
		return
	}
	src, err := parseFeed(data)
	if err != nil {
		return
	}
	meta := entry.Meta()
	self.etag, self.lastModified = meta.Version, meta.Values[lastModifiedValue]
	self.events, self.fetched = newEvents(src), entry.Stored()
	self.baseline = self.events

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

// fetchLocked fetches the feed, unless it isn't modified, of its entity tag,
// or when it was last modified, when its events were fetched before, which
// must be locked
func (self *Filer) fetchLocked(ctx context.Context, op, path string) (*events, bool, error) {
	var opts []client.RequestOpt
	if self.events != nil && self.etag != "" {
		opts = append(opts, client.OptReqHeader("If-None-Match", self.etag))
	}
	if self.events != nil && self.lastModified != "" {
		opts = append(opts, client.OptReqHeader("If-Modified-Since", self.lastModified))
	}
	response := feed{ctx: ctx, cache: self.cache}
	if err := self.client.DoWithContext(ctx, client.NewRequestEx(http.MethodGet, accept), &response, opts...); self.events != nil && isStatus(err, http.StatusNotModified) {
		self.fetched = self.now()
		self.checkedLocked(ctx)
		return self.events, false, nil
	} else if err != nil {
		return nil, false, fetchError(op, path, self.feed.String(), err)
	} else if response.source == nil {
		return nil, false, gofiler.ErrBadParameter.Withf("%s %s: %s isn't a calendar", op, path, self.feed.String())
	}
	self.etag, self.lastModified = response.etag, response.lastModified
	self.events, self.fetched = newEvents(response.source), self.now()
	self.checkedLocked(ctx)
	return self.events, true, nil
}

// named returns a copy of an object, with the volume's name
func (self *Filer) named(object *schema.Object) *schema.Object {
	result := *object
	result.Name = self.name
	return &result
}

// Unmarshal reads a response, of up to maxFeed octets, as a calendar, and
// saves it in the volume's cache, of its entity tag, and when it was last
// modified, once it's read, and is a calendar which isn't cut off, so the
// one which was saved is kept otherwise. The cache is never the only copy,
// so a feed which can't be saved is read, as it would be without it.
func (f *feed) Unmarshal(header http.Header, r io.Reader) error {
	f.etag, f.lastModified = header.Get("ETag"), header.Get("Last-Modified")
	data, err := io.ReadAll(io.LimitReader(r, maxFeed+1))
	if err == nil && len(data) > maxFeed {
		err = fmt.Errorf("it's more than %d MiB", maxFeed>>20)
	}
	var src *source
	if err == nil {
		src, err = parseFeed(data)
	}
	if err != nil {
		return gofiler.ErrBadParameter.Withf("the calendar can't be read: %v", err)
	}
	meta := gofiler.CacheMeta{Version: f.etag, Values: map[string]string{lastModifiedValue: f.lastModified}}
	if entry, err := f.cache.Put(f.ctx, feedKey, 0, meta, bytes.NewReader(data)); err == nil {
		entry.Close()
	}
	f.source = src
	return nil
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
		return gofiler.ErrBadParameter.Withf("%s %s: the calendar %s isn't found", op, path, feed)
	default:
		return gofiler.ErrServiceUnavailable.Withf("%s %s: %v", op, path, err)
	}
}
