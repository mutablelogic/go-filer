package google

import (
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/filer/google/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	vcard "github.com/mutablelogic/go-metadata/pkg/vcard"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ContactFiler is a filer of a person's Google Contacts: each contact is a
// file at the root, of its id, as a vCard, with its metadata, such as its
// name. It's read, and watched, but not written, so far.
type ContactFiler struct {
	contactOpt
	name        string
	credentials gofiler.CredentialProvider
	tokens      *tokens
	clientopts  []client.ClientOpt
	client      *httpclient.PeopleClient
}

// contactOpt are the options of a contact filer, of its parameters
type contactOpt struct {
	quota           uint          // The most requests each minute, or zero
	refreshInterval time.Duration // How often changes are polled for, or zero not to
	endpoint        string        // The endpoint of the API, which is Google's, but a test's
	credential      string        // The key of the user's sign-in
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
)

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
// parameters are readonly, which must be true, so far, quota,
// refresh_interval and credential, as a task filer's.
func NewContactFiler(ctx context.Context, name string, params url.Values, credentials gofiler.CredentialProvider, clientopts ...client.ClientOpt) (*ContactFiler, error) {
	if !types.IsIdentifier(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	}
	opt, err := parseContactOpts(params)
	if err != nil {
		return nil, err
	}
	self := &ContactFiler{contactOpt: opt, name: name, credentials: credentials, clientopts: slices.Clone(clientopts)}

	// The user's sign-in, which can read contacts
	required := []string{httpclient.ContactsReadonlyScope, httpclient.ContactsScope}
	if self.tokens, err = newTokens(ctx, credentials, opt.credential, required, []string{httpclient.ContactsReadonlyScope}); err != nil {
		return nil, err
	}

	// The client, whose requests have the sign-in's token, and are limited
	// to the quota, spaced evenly
	api, err := url.Parse(opt.endpoint)
	if err != nil {
		return nil, err
	}
	opts := append(slices.Clone(clientopts), client.OptTransport(func(parent http.RoundTripper) http.RoundTripper {
		return &tokenTransport{parent: parent, tokens: self.tokens, scheme: api.Scheme, host: api.Host}
	}))
	if opt.quota > 0 {
		opts = append(opts, client.OptRateLimit(float32(opt.quota)/60))
	}
	if self.client, err = httpclient.NewPeopleClient(opt.endpoint, opts...); err != nil {
		return nil, err
	}

	// Check the contacts can be read
	if err := self.client.Check(ctx); err != nil {
		return nil, self.apiError("check", "/", err)
	}
	return self, nil
}

// parseContactOpts returns the options of the parameters, which are
// rejected when they can't be parsed, or are for writing
func parseContactOpts(params url.Values) (contactOpt, error) {
	opt := contactOpt{quota: DefaultTasksQuota, refreshInterval: DefaultRefreshInterval, credential: DefaultCredential, endpoint: peopleEndpoint}
	for key := range params {
		switch key {
		case paramReadonly, paramQuota, paramRefreshInterval, paramCredential:
		default:
			return opt, gofiler.ErrBadParameter.Withf("unknown parameter: %q", key)
		}
	}
	if value := params.Get(paramReadonly); value != "" {
		if readonly, err := strconv.ParseBool(value); err != nil {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramReadonly, value)
		} else if !readonly {
			return opt, gofiler.ErrNotImplemented.With("writing contacts isn't implemented yet, so a volume of contacts is read-only")
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

// URL returns the URL of the filer, such as gcontacts://contacts, with each
// of its parameters
func (self *ContactFiler) URL() *url.URL {
	return &url.URL{Scheme: ContactsScheme, Host: self.name, RawQuery: self.params().Encode()}
}

// Clone returns a new filer of the contacts, with the parameters changed by
// the patch, and the same client options, which is checked as a new filer
// is
func (self *ContactFiler) Clone(ctx context.Context, patch url.Values) (gofiler.Filer, error) {
	clone, err := NewContactFiler(ctx, self.name, gofiler.PatchValues(self.params(), patch), self.credentials, self.clientopts...)
	if err != nil {
		return nil, err
	}
	return clone, nil
}

// Readonly returns true, as contacts aren't written, so far
func (self *ContactFiler) Readonly() bool {
	return true
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
		return fn(strings.NewReader(string(content)))
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
		people, err := self.client.Connections(ctx, httpclient.PersonFields)
		if err != nil {
			return nil, self.apiError("list", "/", err)
		}
		for i := range people {
			if object, _ := self.contactObject(&people[i]); object != nil && object.Path > req.StartAfter {
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

// WatchObjects polls the contacts, at the refresh interval, and sends the
// changes, until the context is cancelled: a contact which is created, or
// whose entity tag changes, or which is deleted. The first change is a
// rescan of the root, once they're read. Returns ErrNotImplemented when
// the refresh interval is zero, and the error of reading them, such as a
// SignInError, when a sign-in is needed.
func (self *ContactFiler) WatchObjects(ctx context.Context, ch chan<- schema.Event) error {
	if self.refreshInterval == 0 {
		return gofiler.ErrNotImplemented.Withf("volume %q isn't watched, as its %s is zero", self.name, paramRefreshInterval)
	}
	last, err := self.snapshot(ctx)
	if err != nil {
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
		next, err := self.snapshot(ctx)
		if err != nil {
			return err
		}
		var events []schema.Event
		for p, etag := range next {
			if old, exists := last[p]; !exists {
				events = append(events, schema.Event{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Name: self.name, Path: p}})
			} else if old != etag {
				events = append(events, schema.Event{EventType: schema.EventTypeUpdated, ObjectKey: schema.ObjectKey{Name: self.name, Path: p}})
			}
		}
		for p := range last {
			if _, exists := next[p]; !exists {
				events = append(events, schema.Event{EventType: schema.EventTypeDeleted, ObjectKey: schema.ObjectKey{Name: self.name, Path: p}})
			}
		}
		slices.SortFunc(events, func(a, b schema.Event) int { return strings.Compare(a.Path, b.Path) })
		for _, event := range events {
			select {
			case ch <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		last = next
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// params returns each of the filer's parameters, so its URL says how it's
// set up
func (self *ContactFiler) params() url.Values {
	return url.Values{
		paramReadonly:        {"true"},
		paramQuota:           {strconv.FormatUint(uint64(self.quota), 10)},
		paramRefreshInterval: {self.refreshInterval.String()},
		paramCredential:      {self.credential},
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
		people, err := self.client.Connections(ctx, "metadata")
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
		root.Meta = append(root.Meta, schema.FolderMeta(count)...)
		return root, nil, nil
	}
	person, err := self.client.Person(ctx, "people/"+id, httpclient.PersonFields)
	if err != nil {
		return nil, nil, self.apiError(op, p, err)
	}
	object, content := self.contactObject(person)
	if object == nil {
		return nil, nil, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
	}
	return object, content, nil
}

// snapshot returns the entity tags of the contacts, by their paths
func (self *ContactFiler) snapshot(ctx context.Context) (map[string]string, error) {
	people, err := self.client.Connections(ctx, "metadata")
	if err != nil {
		return nil, self.apiError("watch", "/", err)
	}
	result := make(map[string]string, len(people))
	for i := range people {
		if id := contactId(&people[i]); !people[i].Metadata.Deleted && googleId(id) {
			result["/"+id+contactExt] = people[i].ETag
		}
	}
	return result, nil
}

// contactObject returns a contact, which is a file, of its vCard, with its
// metadata, and its content, or nil when it's deleted, or its id isn't one
func (self *ContactFiler) contactObject(person *httpclient.Person) (*schema.Object, []byte) {
	id := contactId(person)
	if person.Metadata.Deleted || !googleId(id) {
		return nil, nil
	}
	card := contactCard(id, person)
	content, err := card.MarshalText()
	if err != nil {
		return nil, nil
	}
	object := &schema.Object{
		ObjectKey:  schema.ObjectKey{Name: self.name, Path: "/" + id + contactExt},
		ObjectMeta: schema.ObjectMeta{ContentType: vcard.ContentType},
		ObjectAttr: schema.ObjectAttr{Size: types.Ptr(int64(len(content))), ModTime: card.Rev},
	}
	if person.ETag != "" {
		object.ETag = types.Ptr(person.ETag)
	}
	if name := card.DisplayName(); name != "" {
		object.Meta = append(object.Meta,
			schema.Meta{Namespace: ContactNamespace, Key: "name", Value: json.RawMessage(quote(name))},
			schema.Meta{Namespace: DCNamespace, Key: "title", Value: json.RawMessage(quote(name))},
		)
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
	card.Emails = values(person.EmailAddresses)
	card.Phones = values(person.PhoneNumbers)
	card.URLs = values(person.URLs)
	for _, a := range person.Addresses {
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
// types, and the primary one preferred
func values(in []httpclient.PersonValue) []vcard.Value {
	var result []vcard.Value
	for _, v := range in {
		if v.Value != "" {
			result = append(result, vcard.Value{Value: v.Value, Types: typesOf(v.Type), Pref: v.Metadata.Primary})
		}
	}
	return result
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
