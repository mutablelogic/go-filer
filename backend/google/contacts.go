package google

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	store "github.com/mutablelogic/go-filer/store"
	vcard "github.com/mutablelogic/go-metadata/pkg/vcard"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ContactFiler is a filer of a person's Google Contacts: each contact is a
// file at the root, of its id, as a vCard, with its metadata, such as its
// name. It's read, and watched, and written, when it's writable, of the
// contact action.
type ContactFiler struct {
	contactOpt
	name   string
	opts   gofiler.Opts     // Its credentials, and client options, which a clone is created with
	cache  store.CacheStore // The volume's cache, which the registry owns
	tokens *tokens
	client *httpclient.PeopleClient

	// The contacts, with their fields, got from Google, for the refresh
	// interval, which a contact is read from, rather than got again, or,
	// while it's watched, those the watch knows, which it keeps up to date
	people *cache[[]httpclient.Person]
	mu     sync.Mutex
	live   *contactWatch

	// syncing is held by a sync, and by a write, from its request until
	// what it got is applied, so a sync whose response is older doesn't
	// undo a write, nor a write a sync. It's a channel of one, so waiting
	// for it stops when the context is cancelled.
	syncing chan struct{}
}

// contactOpt are the options of a contact filer, of its parameters
type contactOpt struct {
	readonly        bool          // Contacts can't be created, updated or deleted
	quota           uint          // The most requests each minute, or zero
	refreshInterval time.Duration // How often changes are polled for, or zero not to
	endpoint        string        // The endpoint of the API, which is Google's, but a test's
	credential      string        // The key of the user's sign-in
	description     string        // What the volume is, which is of the user who signed in, by default
}

var _ gofiler.Filer = (*ContactFiler)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// ContactsScheme is the scheme of the URL of a volume of a person's
	// Google Contacts, gcontacts://<name>
	ContactsScheme = "gcontacts"

	// ContactNamespace is the namespace of the metadata of contacts
	ContactNamespace = "contact"

	// contactExt is the extension of a contact's file
	contactExt = ".vcf"

	// contactURL is the link of a contact, of its id, in Google Contacts
	contactURL = "https://contacts.google.com/person/"

	// photoSize is the most pixels of the longest side of a contact's photo
	// which is got, maxPhotoSize the most of it which is read, which at
	// photoSize is tens of kilobytes, and photoTimeout how long it's waited
	// for
	photoSize    = 512
	maxPhotoSize = 1 << 20
	photoTimeout = 30 * time.Second
)

// rePhotoSize matches the size at the end of a photo's link, such as =s100
var rePhotoSize = regexp.MustCompile(`=s\d+(-[a-z]+)?$`)

// peopleEndpoint is the endpoint of the People API, which is Google's,
// unless a test sets another. It isn't a parameter, as a volume's URL could
// send the sign-in's token to another server.
var peopleEndpoint = httpclient.PeopleEndpoint

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewContactFiler returns a filer of the Google Contacts of the volume's
// name, with the parameters, the user's sign-in, of the provider of
// credentials, and go-client's options of its requests, such as a tracer.
// The sign-in, and the contacts, are checked, as for NewTaskFiler, and its
// parameters are readonly, which is true by default, and, when it's false,
// needs a sign-in which can write contacts, of the contact action, quota,
// refresh_interval and credential, as a task filer's.
func NewContactFiler(ctx context.Context, name string, params url.Values, cache store.CacheStore, opts gofiler.Opts) (*ContactFiler, error) {
	if cache == nil {
		return nil, gofiler.ErrBadParameter.Withf("volume %q has no cache", name)
	}
	if !types.IsIdentifier(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	}
	opt, err := parseContactOpts(params)
	if err != nil {
		return nil, err
	}
	opts.ClientOpts = slices.Clone(opts.ClientOpts)
	self := &ContactFiler{contactOpt: opt, name: name, opts: opts, cache: cache, syncing: make(chan struct{}, 1)}
	self.people = newCache[[]httpclient.Person](time.Now, func() time.Duration { return self.refreshInterval })

	// The user's sign-in, which has the access the volume needs: to read
	// contacts, or to write them, too
	required, scopes := []string{httpclient.ContactsScope}, []string{httpclient.ContactsScope, httpclient.EmailScope}
	if opt.readonly {
		required, scopes = []string{httpclient.ContactsReadonlyScope, httpclient.ContactsScope}, []string{httpclient.ContactsReadonlyScope, httpclient.EmailScope}
	}
	if self.tokens, err = newTokens(ctx, opts.Credentials, opt.credential, required, scopes); err != nil {
		return nil, err
	}

	// The client, whose requests have the sign-in's token, and are limited
	// to the quota, spaced evenly
	api, err := url.Parse(opt.endpoint)
	if err != nil {
		return nil, err
	}
	clientopts := append(slices.Clone(opts.ClientOpts), client.OptTransport(func(parent http.RoundTripper) http.RoundTripper {
		return &tokenTransport{parent: parent, tokens: self.tokens, scheme: api.Scheme, host: api.Host}
	}))
	if opt.quota > 0 {
		clientopts = append(clientopts, client.OptRateLimit(float32(opt.quota)/60))
	}
	if self.client, err = httpclient.NewPeopleClient(opt.endpoint, clientopts...); err != nil {
		return nil, err
	}

	// Check the contacts can be read
	if err := self.client.Check(ctx); err != nil {
		return nil, self.apiError("check", "/", err)
	}

	// What the volume is, when it isn't set, is of the user who signed in
	if self.description == "" {
		self.description = describe(ctx, "Google Contacts", self.tokens, opts.ClientOpts)
	}
	return self, nil
}

// parseContactOpts returns the options of the parameters, which are
// rejected when they can't be parsed, or are for writing
func parseContactOpts(params url.Values) (contactOpt, error) {
	opt := contactOpt{readonly: true, quota: DefaultTasksQuota, refreshInterval: DefaultRefreshInterval, credential: DefaultCredential, endpoint: peopleEndpoint}
	for key := range params {
		switch key {
		case paramReadonly, paramQuota, paramRefreshInterval, paramCredential, schema.OptDescription:
		default:
			return opt, gofiler.ErrBadParameter.Withf("unknown parameter: %q", key)
		}
	}
	opt.description = params.Get(schema.OptDescription)
	if value := params.Get(paramReadonly); value != "" {
		if readonly, err := strconv.ParseBool(value); err != nil {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramReadonly, value)
		} else {
			opt.readonly = readonly
		}
	}
	if value := params.Get(paramQuota); value != "" {
		quota, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramQuota, value)
		}
		opt.quota = uint(quota)
	}
	if value := params.Get(paramRefreshInterval); value != "" {
		interval, err := time.ParseDuration(value)
		if err != nil || interval < 0 {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramRefreshInterval, value)
		}
		opt.refreshInterval = interval
	}
	if value := params.Get(paramCredential); value != "" {
		if err := credentialKey(value); err != nil {
			return opt, err
		}
		opt.credential = value
	}
	return opt, nil
}

// Close the filer
func (self *ContactFiler) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - PROPERTIES

// Description returns what the volume is, such as "Google Contacts for ada@example.com"
func (self *ContactFiler) Description() string {
	return self.description
}

// URL returns the URL of the filer, such as gcontacts://contacts, with each
// of its parameters
func (self *ContactFiler) URL() *url.URL {
	return &url.URL{Scheme: ContactsScheme, Host: self.name, RawQuery: self.params().Encode()}
}

// Clone returns a new filer of the contacts, with the parameters changed by
// the patch, and the same client options, which is checked as a new filer
// is
func (self *ContactFiler) Clone(ctx context.Context, patch url.Values) (gofiler.Filer, error) {
	// A clone of another sign-in, without a description, is described of it
	params := gofiler.PatchValues(self.params(), patch)
	credential := params.Get(paramCredential)
	if credential == "" {
		credential = DefaultCredential
	}
	if credential != self.credential && !patch.Has(schema.OptDescription) {
		params.Del(schema.OptDescription)
	}
	clone, err := NewContactFiler(ctx, self.name, params, self.cache, self.opts)
	if err != nil {
		return nil, err
	}
	return clone, nil
}

// Readonly returns true unless the volume is writable, with readonly=false
func (self *ContactFiler) Readonly() bool {
	return self.readonly
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - OBJECTS

// GetObject returns the root, which is a directory, or a contact
func (self *ContactFiler) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	object, _, err := self.stat(ctx, "get", req.Path)
	if err != nil {
		return nil, err
	} else if object.NotModified(req) {
		return nil, gofiler.ErrNotModified.Withf("%q", object.Path)
	}
	return object, nil
}

// ReadObject returns the function which reads a contact's vCard, and the
// contact. The root can't be read, which is fs.ErrInvalid.
func (self *ContactFiler) ReadObject(ctx context.Context, req schema.GetObjectRequest) (gofiler.ReadObjectFunc, *schema.Object, error) {
	object, content, err := self.stat(ctx, "read", req.Path)
	if err != nil {
		return nil, nil, err
	} else if object.IsDir {
		return nil, nil, &fs.PathError{Op: "read", Path: req.Path, Err: fs.ErrInvalid}
	} else if object.NotModified(req) {
		return nil, nil, gofiler.ErrNotModified.Withf("%q", object.Path)
	}
	return func(ctx context.Context, fn func(io.Reader) error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(&contentReader{ctx: ctx, r: bytes.NewReader(content)})
	}, object, nil
}

// ListObjects lists the root, which is its contacts, in the order of their
// paths, whether or not it's recursive, or a contact, after the path the
// request starts after
func (self *ContactFiler) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	id, isRoot, err := contactPath("list", req.Path)
	if err != nil {
		return nil, err
	}
	var objects []*schema.Object
	if isRoot {
		people, err := self.connections(ctx)
		if err != nil {
			return nil, self.apiError("list", "/", err)
		}
		for i := range people {
			if object, _ := self.contactObject(ctx, &people[i], false); object != nil && object.Path > req.StartAfter {
				objects = append(objects, object)
			}
		}
		slices.SortFunc(objects, func(a, b *schema.Object) int { return strings.Compare(a.Path, b.Path) })
	} else if object, _, err := self.stat(ctx, "list", "/"+id+contactExt); err != nil {
		return nil, err
	} else if object.Path > req.StartAfter {
		objects = append(objects, object)
	}
	return &objectList{objects: objects}, nil
}

// CreateObject isn't implemented, as contacts aren't written, so far
func (self *ContactFiler) CreateObject(context.Context, schema.CreateObjectRequest) (*schema.Object, error) {
	return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
}

// DeleteObjects isn't implemented, as contacts aren't written, so far
func (self *ContactFiler) DeleteObjects(context.Context, schema.DeleteObjectsRequest) (*schema.DeleteObjectsResponse, error) {
	return nil, gofiler.ErrForbidden.Withf("Volume %q is read-only", self.name)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// params returns each of the filer's parameters, so its URL says how it's
// set up
func (self *ContactFiler) params() url.Values {
	return url.Values{
		paramReadonly:         {strconv.FormatBool(self.readonly)},
		paramQuota:            {strconv.FormatUint(uint64(self.quota), 10)},
		paramRefreshInterval:  {self.refreshInterval.String()},
		paramCredential:       {self.credential},
		schema.OptDescription: {self.description},
	}
}

// apiError returns the error of a request, as a task filer's does
func (self *ContactFiler) apiError(op, path string, err error) error {
	return apiError(self.tokens, self.credential, op, path, err)
}

// stat returns the root, or a contact, and its vCard
func (self *ContactFiler) stat(ctx context.Context, op, p string) (*schema.Object, []byte, error) {
	id, isRoot, err := contactPath(op, p)
	if err != nil {
		return nil, nil, err
	} else if isRoot {
		people, err := self.connections(ctx)
		if err != nil {
			return nil, nil, self.apiError(op, p, err)
		}
		count := 0
		for i := range people {
			if !people[i].Metadata.Deleted && googleId(contactId(&people[i])) {
				count++
			}
		}
		root := &schema.Object{ObjectKey: schema.ObjectKey{Name: self.name, Path: "/"}, ObjectMeta: schema.ObjectMeta{ContentType: schema.DirectoryType}, ObjectAttr: schema.ObjectAttr{IsDir: true}}
		root.Meta = append(root.Meta, schema.FolderMeta(count, 0)...)
		return root, nil, nil
	}
	person, err := self.person(ctx, id)
	if err != nil {
		return nil, nil, self.apiError(op, p, err)
	} else if contactId(person) != id || !isContact(person) {
		// A profile, such as people/me, or of another id, isn't a contact,
		// as it isn't listed, or watched
		return nil, nil, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
	}
	object, content := self.contactObject(ctx, person, op == "read")
	if object == nil {
		return nil, nil, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
	}
	return object, content, nil
}

// connections returns the contacts, with their fields, of the watch, while
// it's watched, or those which were got, for the refresh interval
func (self *ContactFiler) connections(ctx context.Context) ([]httpclient.Person, error) {
	if people, ok := self.livePeople(); ok {
		return people, nil
	}
	return self.people.get(ctx, "", func() ([]httpclient.Person, error) {
		return self.client.Connections(ctx, httpclient.PersonFields)
	})
}

// person returns a contact, of its id, of the contacts which were got,
// when they were, or else of Google
func (self *ContactFiler) person(ctx context.Context, id string) (*httpclient.Person, error) {
	// The watch knows whether it's there, which is so, rather than what was
	// got before, which may have it still
	if person, ok := self.livePerson(id); ok && person != nil {
		return person, nil
	} else if ok {
		return nil, httpresponse.Err(http.StatusNotFound).Withf("contact %q", id)
	}
	if people, ok := self.people.peek(""); ok {
		for i := range people {
			if contactId(&people[i]) == id {
				person := people[i]
				return &person, nil
			}
		}
	}
	return self.client.Person(ctx, "people/"+id, httpclient.PersonFields)
}

// contactObject returns a contact, which is a file, of its vCard, with its
// metadata, and, when it's read, its content, and its size, whose photo is
// got, and embedded, so it isn't a link, which it is when it can't be got.
// It has no size when it isn't read, as its photo isn't got for a listing.
// Returns nil when it's deleted, or its id isn't one.
func (self *ContactFiler) contactObject(ctx context.Context, person *httpclient.Person, read bool) (*schema.Object, []byte) {
	id := contactId(person)
	if person.Metadata.Deleted || !googleId(id) {
		return nil, nil
	}
	card := contactCard(id, person)
	object := &schema.Object{
		ObjectKey:  schema.ObjectKey{Name: self.name, Path: "/" + id + contactExt},
		ObjectMeta: schema.ObjectMeta{ContentType: vcard.ContentType},
		ObjectAttr: schema.ObjectAttr{ModTime: card.Rev},
	}
	var content []byte
	if read {
		if card.Photo != nil {
			if photo := self.photo(ctx, card.Photo.URI); photo != nil {
				card.Photo = photo
			}
		}
		var err error
		if content, err = card.MarshalText(); err != nil {
			return nil, nil
		}
		object.Size = types.Ptr(int64(len(content)))
	}
	if person.ETag != "" {
		object.ETag = types.Ptr(person.ETag)
	}
	// What the vCard can't have: its link in Google Contacts, as its URLs
	// are the person's, and its name, as dc:title, so it has one when it
	// isn't enriched
	object.Meta = append(object.Meta, schema.Meta{Namespace: ContactNamespace, Key: "url", Value: json.RawMessage(quote(contactURL + url.PathEscape(id)))})
	if name := card.DisplayName(); name != "" {
		object.Meta = append(object.Meta, schema.Meta{Namespace: DCNamespace, Key: "title", Value: json.RawMessage(quote(name))})
	}
	return object, content
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// contactPath returns the id of a contact, of its path, /<id>.vcf, or
// whether it's the root, or fs.ErrNotExist of another path, such as one
// which ends with "/", or whose id can't be Google's
func contactPath(op, p string) (string, bool, error) {
	rel := strings.Trim(path.Clean("/"+p), "/")
	switch {
	case rel == "":
		return "", true, nil
	case !strings.HasSuffix(p, "/") && !strings.Contains(rel, "/") && strings.HasSuffix(rel, contactExt):
		if id := strings.TrimSuffix(rel, contactExt); googleId(id) {
			return id, false, nil
		}
	}
	return "", false, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
}

// isContact returns true when a person is a contact, of a source of the
// CONTACT type, rather than only a profile
func isContact(person *httpclient.Person) bool {
	for _, source := range person.Metadata.Sources {
		if source.Type == "CONTACT" {
			return true
		}
	}
	return false
}

// contactId returns the id of a contact, of its resource name, people/<id>
func contactId(person *httpclient.Person) string {
	id, _ := strings.CutPrefix(person.ResourceName, "people/")
	return id
}

// contactCard returns a contact as a vCard: its names, nicknames, emails,
// phones, URLs, addresses, organisations, and their titles, birthday,
// notes, photo, and when its contact was updated, with the values which are
// primary preferred
func contactCard(id string, person *httpclient.Person) *vcard.Card {
	card := &vcard.Card{UID: id}
	for _, name := range primaryFirst(person.Names, func(n httpclient.PersonName) bool { return n.Metadata.Primary }) {
		card.FN = name.DisplayName
		card.Name = vcard.Name{Family: name.FamilyName, Given: name.GivenName, Middle: name.MiddleName, Prefix: name.HonorificPrefix, Suffix: name.HonorificSuffix}
		break
	}
	for _, nickname := range person.Nicknames {
		if nickname.Value != "" {
			card.Nicknames = append(card.Nicknames, nickname.Value)
		}
	}
	card.Emails = values(person.EmailAddresses, typesOf)
	card.Phones = values(person.PhoneNumbers, phoneTypes)
	card.URLs = values(person.URLs, typesOf)
	for _, a := range person.Addresses {
		// An address of one line, alone, such as the contact action's, is
		// its street
		if a.StreetAddress == "" && a.POBox == "" && a.ExtendedAddress == "" && a.City == "" && a.Region == "" && a.PostalCode == "" && a.Country == "" {
			a.StreetAddress = a.FormattedValue
		}
		card.Addresses = append(card.Addresses, vcard.Address{Types: typesOf(a.Type), POBox: a.POBox, Extended: a.ExtendedAddress, Street: a.StreetAddress, Locality: a.City, Region: a.Region, PostalCode: a.PostalCode, Country: a.Country})
	}
	for _, org := range person.Organizations {
		if org.Name != "" || org.Department != "" {
			o := vcard.Organization{Name: org.Name}
			if org.Department != "" {
				o.Units = []string{org.Department}
			}
			card.Orgs = append(card.Orgs, o)
		}
		if org.Title != "" {
			card.Titles = append(card.Titles, org.Title)
		}
	}
	for _, birthday := range primaryFirst(person.Birthdays, func(b httpclient.PersonDate) bool { return b.Metadata.Primary }) {
		if birthday.Date.Month > 0 && birthday.Date.Day > 0 {
			card.Birthday = &vcard.Date{Year: birthday.Date.Year, Month: birthday.Date.Month, Day: birthday.Date.Day}
			break
		}
	}
	for _, bio := range person.Biographies {
		if bio.Value != "" {
			card.Notes = append(card.Notes, bio.Value)
		}
	}
	for _, photo := range primaryFirst(person.Photos, func(p httpclient.PersonPhoto) bool { return p.Metadata.Primary }) {
		if photo.URL != "" && !photo.Default {
			card.Photo = &vcard.Photo{URI: photo.URL}
			break
		}
	}
	for _, source := range person.Metadata.Sources {
		if source.Type == "CONTACT" && source.UpdateTime.After(card.Rev) {
			card.Rev = source.UpdateTime.UTC()
		}
	}
	return card
}

// values returns the values of a contact, such as its emails, with their
// types, of the function, and the primary one first, and preferred, so a
// contact without a name is of its primary email
func values(in []httpclient.PersonValue, types func(string) []string) []vcard.Value {
	var result []vcard.Value
	for _, v := range primaryFirst(in, func(v httpclient.PersonValue) bool { return v.Metadata.Primary }) {
		if v.Value != "" {
			result = append(result, vcard.Value{Value: v.Value, Types: types(v.Type), Pref: v.Metadata.Primary})
		}
	}
	return result
}

// phoneTypes returns the vCard types of a phone number's type of Google's,
// such as mobile, which is cell, or homeFax, which is home and fax, or the
// type, in lower case, when it isn't one of them
func phoneTypes(t string) []string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "mobile":
		return []string{"cell"}
	case "workmobile":
		return []string{"work", "cell"}
	case "homefax":
		return []string{"home", "fax"}
	case "workfax":
		return []string{"work", "fax"}
	case "otherfax":
		return []string{"fax"}
	case "pager":
		return []string{"pager"}
	case "workpager":
		return []string{"work", "pager"}
	case "main", "googlevoice":
		return []string{"voice"}
	default:
		return typesOf(t)
	}
}

// typesOf returns the type of a value, such as home or work, in lower case
func typesOf(t string) []string {
	if t = strings.ToLower(strings.TrimSpace(t)); t == "" {
		return nil
	}
	return []string{t}
}

// primaryFirst returns the values, with the primary ones first
func primaryFirst[T any](in []T, primary func(T) bool) []T {
	result := slices.Clone(in)
	slices.SortStableFunc(result, func(a, b T) int {
		switch {
		case primary(a) && !primary(b):
			return -1
		case primary(b) && !primary(a):
			return 1
		default:
			return 0
		}
	})
	return result
}

// objectList is an iterator of objects
type objectList struct {
	mu      sync.Mutex
	objects []*schema.Object
}

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
