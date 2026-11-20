// Package rss is a backend of an RSS feed, of rss://<name>?url=<feed>,
// whose items are files, of their days, /YYYY/YYYY-MM/DD-<title>.html, each
// an HTML page of the item, with its title, author, date and link as its
// metadata, and its enclosure, such as a podcast's episode, too. It's read
// only, and isn't watched, so an indexed volume is scanned again at the
// indexer's interval. It's a backend which isn't built into the registry,
// which a program adds with
// registry.WithBackend(rss.Scheme, rss.Description, rss.Open).
package rss

import (
	"context"
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
	rss "github.com/mutablelogic/go-rss"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Filer is the backend of an RSS feed
type Filer struct {
	name        string
	feed        string // the URL of the feed
	description string
	refresh     time.Duration // how long a feed which is fetched is kept
	opts        gofiler.Opts
	client      *client.Client

	mu      sync.Mutex
	items   *items    // the feed's items, as files, when it's fetched
	fetched time.Time // when it was fetched
	now     func() time.Time
}

var _ gofiler.Filer = (*Filer)(nil)

// feed decodes a feed of a response, of go-rss
type feed struct {
	*rss.Feed
}

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// Scheme is the scheme of a feed's URL, rss://<name>?url=<feed>
	Scheme = "rss"

	// Description is what the backend is
	Description = "RSS feeds, of their items"

	paramURL      = "url"
	paramRefresh  = "refresh-interval"
	paramReadonly = "readonly"

	// DefaultRefresh is how long a feed which is fetched is kept, so the
	// requests of a listing, and those after it, are of one fetch
	DefaultRefresh = 5 * time.Minute

	// accept is what's accepted of a feed's server
	accept = "application/rss+xml, application/xml;q=0.9, text/xml;q=0.9, */*;q=0.1"
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Open creates the backend of the feed of the URL, rss://<name>?url=<feed>,
// whose name is its name option, or its host, which is a registry.Backend.
// The feed is fetched, so one which can't be is an error, and is described
// of its title, unless it has a description option.
func Open(ctx context.Context, u *url.URL, opts gofiler.Opts) (gofiler.Filer, error) {
	if u == nil || u.Scheme != Scheme {
		return nil, gofiler.ErrBadParameter.With("not a feed's url")
	} else if u.Path != "" && u.Path != "/" {
		return nil, gofiler.ErrBadParameter.Withf("a volume of a feed has no path: %q", u.Path)
	}
	name := u.Query().Get("name")
	if name == "" {
		name = u.Host
	}
	return New(ctx, name, u.Query(), opts)
}

// New creates the backend of a feed, of the options: its url, its
// refresh-interval, and its description
func New(ctx context.Context, name string, params url.Values, opts gofiler.Opts) (*Filer, error) {
	if !types.IsIdentifier(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	}
	self := &Filer{name: name, refresh: DefaultRefresh, opts: opts, now: time.Now, description: params.Get(schema.OptDescription)}
	for key, values := range params {
		value := ""
		if len(values) > 0 {
			value = values[0]
		}
		switch key {
		case paramURL:
			if u, err := url.Parse(value); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return nil, gofiler.ErrBadParameter.Withf("the url of a feed is http or https: %q", value)
			}
			self.feed = value
		case paramRefresh:
			if d, err := time.ParseDuration(value); err != nil || d < 0 {
				return nil, gofiler.ErrBadParameter.Withf("%s: %q", paramRefresh, value)
			} else {
				self.refresh = d
			}
		case paramReadonly:
			if value != "" && value != "true" {
				return nil, gofiler.ErrBadParameter.With("a feed is read-only")
			}
		case "name", schema.OptDescription:
		default:
			return nil, gofiler.ErrBadParameter.Withf("unknown option of a feed: %q", key)
		}
	}
	if self.feed == "" {
		return nil, gofiler.ErrBadParameter.Withf("a feed needs its url, of the %q option", paramURL)
	}
	c, err := client.New(append(append([]client.ClientOpt(nil), opts.ClientOpts...), client.OptEndpoint(self.feed))...)
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
			self.description = "RSS feed " + self.feed
		}
	}
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// URL returns the URL of the feed's volume, with its options
// Description returns what the feed is, such as "RSS feed The Example Podcast"
func (self *Filer) Description() string {
	return self.description
}

func (self *Filer) URL() *url.URL {
	return &url.URL{Scheme: Scheme, Host: self.name, RawQuery: self.params().Encode()}
}

// Clone returns a new backend of the feed, with its options changed by the
// patch, which fetches it, as a new one does
func (self *Filer) Clone(ctx context.Context, patch url.Values) (gofiler.Filer, error) {
	params := gofiler.PatchValues(self.params(), patch)
	if params.Get(paramURL) != self.feed && !patch.Has(schema.OptDescription) {
		params.Del(schema.OptDescription)
	}
	return New(ctx, self.name, params, self.opts)
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

// WatchObjects isn't implemented, as a feed is fetched, so its index is
// scanned again at an interval
func (self *Filer) WatchObjects(context.Context, chan<- schema.Event) error {
	return gofiler.ErrNotImplemented.With("a feed isn't watched")
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// params returns each of the backend's options, so its URL says how it's set
// up
func (self *Filer) params() url.Values {
	return url.Values{
		paramURL:              {self.feed},
		paramRefresh:          {self.refresh.String()},
		paramReadonly:         {"true"},
		schema.OptDescription: {self.description},
	}
}

// list returns the feed's items, which are fetched, unless they were within
// the refresh interval
func (self *Filer) list(ctx context.Context, op, path string) (*items, error) {
	self.mu.Lock()
	defer self.mu.Unlock()
	if self.items != nil && self.now().Sub(self.fetched) < self.refresh {
		return self.items, nil
	}
	var response feed
	if err := self.client.DoWithContext(ctx, client.NewRequestEx(http.MethodGet, accept), &response); err != nil {
		return nil, fetchError(op, path, self.feed, err)
	} else if response.Feed == nil || response.Channel == nil {
		return nil, gofiler.ErrBadParameter.Withf("%s %s: %s isn't an RSS feed", op, path, self.feed)
	}
	self.items, self.fetched = newItems(response.Channel), self.now()
	return self.items, nil
}

// named returns a copy of an object, with the volume's name
func (self *Filer) named(object *schema.Object) *schema.Object {
	result := *object
	result.Name = self.name
	return &result
}

// Unmarshal decodes a response, as a feed, of go-rss
func (f *feed) Unmarshal(_ http.Header, r io.Reader) error {
	parsed, err := rss.Read(r)
	if err != nil {
		return gofiler.ErrBadParameter.Withf("the feed can't be read: %v", err)
	}
	f.Feed = parsed
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
		return gofiler.ErrBadParameter.Withf("%s %s: the feed %s isn't found", op, path, feed)
	default:
		return gofiler.ErrServiceUnavailable.Withf("%s %s: %v", op, path, err)
	}
}
