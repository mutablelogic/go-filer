package google_test

import (
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	// Packages
	authclient "github.com/mutablelogic/go-auth/auth/httpclient"
	gofiler "github.com/mutablelogic/go-filer"
	google "github.com/mutablelogic/go-filer/backend/google"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	vcard "github.com/mutablelogic/go-metadata/pkg/vcard"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// fakePeople is the People API, of a person's contacts, which it pages two
// at a time, and of the sign-in's token
type fakePeople struct {
	sync.Mutex
	people   []httpclient.Person
	profiles map[string]httpclient.Person // Which are got, by their ids, but aren't contacts, so aren't listed
	pages    int
	gets     int      // How many times a person is got
	updated  []string // The fields of the last update, of updatePersonFields

	// The contacts, by their ids, of their entity tags, of each sync token,
	// so a sync returns what changed since, how many times every contact is
	// synced, and whether a token isn't accepted, as it's expired
	snapshots map[string]map[string]string
	fullSyncs int
	expired   bool
	synced    []string // The tokens which are synced of
	photos    int // How many times a photo is got
}

func newFakePeople(t *testing.T) (*fakePeople, string) {
	t.Helper()
	fake := &fakePeople{}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	google.SetPeopleEndpoint(t, server.URL)
	return fake, server.URL
}

// photo is a contact's photo, which the fake serves, without a token, at
// photoSize
var photo = []byte("\x89PNG\r\n\x1a\nphoto")

func (f *fakePeople) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.Lock()
	defer f.Unlock()
	if strings.HasPrefix(r.URL.Path, "/photos/") {
		f.photos++
		if r.URL.Path != "/photos/ada=s512" || r.Header.Get("Authorization") != "" {
			writeError(w, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(photo)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+token {
		w.Header().Set("WWW-Authenticate", `Bearer realm="https://accounts.google.com/", error="invalid_token"`)
		writeError(w, http.StatusUnauthorized)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if f.write(w, r, parts) {
		return
	}
	switch {
	case len(parts) == 3 && parts[0] == "people" && parts[1] == "me" && parts[2] == "connections" && r.URL.Query().Get("syncToken") != "":
		// What changed since the token: those which are new, or whose entity
		// tags changed, and those which are deleted, or gone
		f.synced = append(f.synced, r.URL.Query().Get("syncToken"))
		snapshot, exists := f.snapshots[r.URL.Query().Get("syncToken")]
		if f.expired || !exists {
			writeError(w, http.StatusGone)
			return
		}
		var changed []httpclient.Person
		seen := make(map[string]bool)
		for _, person := range f.people {
			id := strings.TrimPrefix(person.ResourceName, "people/")
			seen[id] = true
			if etag, had := snapshot[id]; !had || etag != person.ETag || person.Metadata.Deleted {
				if !(person.Metadata.Deleted && !had) {
					changed = append(changed, person)
				}
			}
		}
		for id := range snapshot {
			if !seen[id] {
				changed = append(changed, httpclient.Person{ResourceName: "people/" + id, Metadata: httpclient.PersonMetadata{Deleted: true}})
			}
		}
		writeJSON(w, map[string]any{"connections": changed, "nextSyncToken": f.snapshot()})
	case len(parts) == 3 && parts[0] == "people" && parts[1] == "me" && parts[2] == "connections":
		f.pages++
		start, _ := strconv.Atoi(r.URL.Query().Get("pageToken"))
		if start == 0 && r.URL.Query().Get("requestSyncToken") == "true" {
			f.fullSyncs++
		}
		end := min(start+2, len(f.people))
		page := map[string]any{"connections": f.people[start:end]}
		if end < len(f.people) {
			page["nextPageToken"] = strconv.Itoa(end)
		} else if r.URL.Query().Get("requestSyncToken") == "true" {
			page["nextSyncToken"] = f.snapshot()
		}
		writeJSON(w, page)
	case len(parts) == 2 && parts[0] == "people":
		f.gets++
		if profile, exists := f.profiles[parts[1]]; exists {
			writeJSON(w, profile)
			return
		}
		for _, person := range f.people {
			if person.ResourceName == "people/"+parts[1] {
				writeJSON(w, person)
				return
			}
		}
		writeError(w, http.StatusNotFound)
	default:
		writeError(w, http.StatusNotFound)
	}
}

// write creates, updates or deletes a contact, as the People API does, and
// returns false when the request isn't a write
func (f *fakePeople) write(w http.ResponseWriter, r *http.Request, parts []string) bool {
	find := func(id string) int {
		for i := range f.people {
			if f.people[i].ResourceName == "people/"+id {
				return i
			}
		}
		return -1
	}
	switch {
	case r.Method == http.MethodPost && len(parts) == 1 && parts[0] == "people:createContact":
		var person httpclient.Person
		if err := json.NewDecoder(r.Body).Decode(&person); err != nil {
			writeError(w, http.StatusBadRequest)
			return true
		}
		id := "n" + strconv.Itoa(len(f.people)+1)
		person.ResourceName, person.ETag = "people/"+id, id+"-1"
		person.Metadata = httpclient.PersonMetadata{Sources: []httpclient.PersonSource{{Type: "CONTACT", ID: id, ETag: "s" + id + "-1"}}}
		f.people = append(f.people, person)
		writeJSON(w, person)
	case r.Method == http.MethodPatch && len(parts) == 2 && parts[0] == "people" && strings.HasSuffix(parts[1], ":updateContact"):
		i := find(strings.TrimSuffix(parts[1], ":updateContact"))
		var person httpclient.Person
		if i < 0 {
			writeError(w, http.StatusNotFound)
			return true
		} else if err := json.NewDecoder(r.Body).Decode(&person); err != nil || person.ETag != f.people[i].ETag || !slices.Equal(contactSources(person), contactSources(f.people[i])) || len(contactSources(person)) == 0 {
			// As the People API, which needs the contact's entity tag, and
			// its CONTACT source, with its entity tag
			writeError(w, http.StatusBadRequest)
			return true
		}
		f.updated = strings.Split(r.URL.Query().Get("updatePersonFields"), ",")
		current := &f.people[i]
		for _, field := range f.updated {
			switch field {
			case "names":
				current.Names = person.Names
			case "emailAddresses":
				current.EmailAddresses = person.EmailAddresses
			case "phoneNumbers":
				current.PhoneNumbers = person.PhoneNumbers
			case "organizations":
				current.Organizations = person.Organizations
			case "birthdays":
				current.Birthdays = person.Birthdays
			case "biographies":
				current.Biographies = person.Biographies
			case "addresses":
				current.Addresses = person.Addresses
			}
		}
		current.ETag = strings.TrimPrefix(current.ResourceName, "people/") + "-" + strconv.Itoa(len(f.updated)+2)
		for j := range current.Metadata.Sources {
			current.Metadata.Sources[j].ETag = "s" + current.ETag
		}
		writeJSON(w, *current)
	case r.Method == http.MethodDelete && len(parts) == 2 && parts[0] == "people" && strings.HasSuffix(parts[1], ":deleteContact"):
		i := find(strings.TrimSuffix(parts[1], ":deleteContact"))
		if i < 0 {
			writeError(w, http.StatusNotFound)
			return true
		}
		f.people = append(f.people[:i], f.people[i+1:]...)
		writeJSON(w, map[string]any{})
	default:
		return false
	}
	return true
}

// contactSources returns the CONTACT sources of a person
func contactSources(person httpclient.Person) []httpclient.PersonSource {
	var result []httpclient.PersonSource
	for _, source := range person.Metadata.Sources {
		if source.Type == "CONTACT" {
			result = append(result, source)
		}
	}
	return result
}

// snapshot returns a sync token, of the contacts' entity tags now, which
// the fake must be locked for
func (f *fakePeople) snapshot() string {
	if f.snapshots == nil {
		f.snapshots = make(map[string]map[string]string)
	}
	snapshot := make(map[string]string, len(f.people))
	for _, person := range f.people {
		if !person.Metadata.Deleted {
			snapshot[strings.TrimPrefix(person.ResourceName, "people/")] = person.ETag
		}
	}
	token := "s" + strconv.Itoa(len(f.snapshots))
	f.snapshots[token] = snapshot
	return token
}

// add adds a contact, of its id, and changes it with fn
func (f *fakePeople) add(id string, fn func(*httpclient.Person)) {
	f.Lock()
	defer f.Unlock()
	person := httpclient.Person{ResourceName: "people/" + id, ETag: id + "-1", Metadata: httpclient.PersonMetadata{Sources: []httpclient.PersonSource{{Type: "CONTACT", ID: id, ETag: "s" + id + "-1"}}}}
	if fn != nil {
		fn(&person)
	}
	f.people = append(f.people, person)
}

// contactCredentials returns the provider of a sign-in which can read
// contacts, and tasks
func contactCredentials(t *testing.T, endpoint string) *credentials {
	return newCredentials(t, endpoint, func(g *authclient.Grant) {
		g.Scopes = []string{httpclient.TasksScope, httpclient.ContactsReadonlyScope}
	})
}

// newContactFiler returns a filer of the fake, with the parameters
func newContactFiler(t *testing.T, endpoint string, params url.Values) *google.ContactFiler {
	t.Helper()
	if params == nil {
		params = url.Values{}
	}
	params.Set("quota", "0")
	filer, err := newContactFilerOf(t.Context(), "contacts", params, contactCredentials(t, endpoint))
	require.NoError(t, err)
	return filer
}

func Test_NewContactFiler(t *testing.T) {
	ctx := t.Context()
	_, endpoint := newFakePeople(t)

	// A sign-in without the contacts' scope signs in again, asking for it,
	// and keeping those it has, such as of tasks
	_, err := newContactFilerOf(ctx, "contacts", url.Values{"quota": {"0"}}, newCredentials(t, endpoint, nil))
	require.Equal(t, http.StatusUnauthorized, status(err))
	signIn := authclient.SignInOf(err)
	require.NotNil(t, signIn)
	assert.Equal(t, []string{httpclient.ContactsReadonlyScope, httpclient.EmailScope, httpclient.TasksScope}, signIn.Scopes)

	// A writable volume of a sign-in which can only read contacts signs in
	// again, asking for the scope of writing them
	_, err = newContactFilerOf(ctx, "contacts", url.Values{"readonly": {"false"}, "quota": {"0"}}, contactCredentials(t, endpoint))
	require.Equal(t, http.StatusUnauthorized, status(err))
	signIn = authclient.SignInOf(err)
	require.NotNil(t, signIn)
	assert.Contains(t, signIn.Scopes, httpclient.ContactsScope)

	// Parameters which aren't known, or can't be parsed, are bad
	for _, params := range []url.Values{{"hidden": {"true"}}, {"quota": {"-1"}}, {"refresh_interval": {"soon"}}, {"credential": {"S3_SECRET"}}, {"readonly": {"maybe"}}} {
		_, err := newContactFilerOf(ctx, "contacts", params, contactCredentials(t, endpoint))
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, params)
	}

	// Its URL has each of its parameters
	filer := newContactFiler(t, endpoint, nil)
	assert.Equal(t, "gcontacts://contacts?credential=GOOGLE_TOKEN&description=Google+Contacts&quota=0&readonly=true&refresh_interval=1m0s", filer.URL().String())
	assert.True(t, filer.Readonly())
}

func Test_ContactObjects(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFakePeople(t)
	updated := time.Date(2026, 11, 18, 9, 30, 0, 0, time.UTC)
	fake.add("c2", func(p *httpclient.Person) {
		p.Metadata.Sources = []httpclient.PersonSource{{Type: "PROFILE", UpdateTime: updated.Add(time.Hour)}, {Type: "CONTACT", UpdateTime: updated}}
		p.Names = []httpclient.PersonName{{DisplayName: "Ada Lovelace", GivenName: "Ada", FamilyName: "Lovelace", Metadata: httpclient.FieldMetadata{Primary: true}}}
		p.EmailAddresses = []httpclient.PersonValue{{Value: "ada@work.example.com", Type: "Work"}, {Value: "ada@example.com", Type: "home", Metadata: httpclient.FieldMetadata{Primary: true}}}
		p.PhoneNumbers = []httpclient.PersonValue{{Value: "+44 20 7946 0000", Type: "mobile"}, {Value: "+44 20 7946 0001", Type: "homeFax"}}
		p.Organizations = []httpclient.PersonOrg{{Name: "Analytical Engines", Department: "Research", Title: "Countess"}}
		p.Birthdays = []httpclient.PersonDate{{}}
		p.Birthdays[0].Date.Month, p.Birthdays[0].Date.Day = 12, 10
		p.Biographies = []httpclient.PersonValue{{Value: "The first programmer"}}
		p.Photos = []httpclient.PersonPhoto{{URL: endpoint + "/photos/default=s100", Default: true}, {URL: endpoint + "/photos/ada=s100"}}
	})
	fake.add("c1", func(p *httpclient.Person) {
		p.Names = []httpclient.PersonName{{DisplayName: "Charles Babbage"}}
		p.Photos = []httpclient.PersonPhoto{{URL: endpoint + "/photos/missing=s100"}}
	})
	fake.add("c3", func(p *httpclient.Person) { p.Metadata.Deleted = true })
	fake.add("c4", func(p *httpclient.Person) {
		p.EmailAddresses = []httpclient.PersonValue{{Value: "other@example.com"}, {Value: "primary@example.com", Metadata: httpclient.FieldMetadata{Primary: true}}}
	})
	fake.profiles = map[string]httpclient.Person{
		"me":    {ResourceName: "people/me", Metadata: httpclient.PersonMetadata{Sources: []httpclient.PersonSource{{Type: "PROFILE"}}}},
		"alias": {ResourceName: "people/c1", Metadata: httpclient.PersonMetadata{Sources: []httpclient.PersonSource{{Type: "CONTACT"}}}},
	}
	filer := newContactFiler(t, endpoint, nil)

	// The root is a directory, of the count of its contacts, which aren't
	// deleted, and its contacts are files, in the order of their paths,
	// which are read a page at a time
	root, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	require.NoError(t, err)
	assert.True(t, root.IsDir)
	assert.Contains(t, root.Meta, schema.Meta{Namespace: schema.FolderNamespace, Key: "children-count", Value: []byte(`3`)})
	assert.Equal(t, []string{"/c1.vcf", "/c2.vcf", "/c4.vcf"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}}))
	assert.Equal(t, []string{"/c1.vcf", "/c2.vcf", "/c4.vcf"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}))
	assert.Equal(t, []string{"/c4.vcf"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, StartAfter: "/c2.vcf"}))
	assert.Equal(t, []string{"/c2.vcf"}, list(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/c2.vcf"}}))

	// A contact is a vCard, of its name, when it was updated, of its contact,
	// rather than its profile, and its entity tag
	object, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c2.vcf"}})
	require.NoError(t, err)
	assert.Equal(t, vcard.ContentType, object.ContentType)
	assert.Equal(t, "c2-1", *object.ETag)
	assert.True(t, object.ModTime.Equal(updated))
	assert.Contains(t, object.Meta, schema.Meta{Namespace: google.DCNamespace, Key: "title", Value: []byte(`"Ada Lovelace"`)})
	assert.Contains(t, object.Meta, schema.Meta{Namespace: google.ContactNamespace, Key: "url", Value: []byte(`"https://contacts.google.com/person/c2"`)})
	_, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c2.vcf"}, IfNoneMatch: []string{"c2-1"}})
	assert.ErrorIs(t, err, gofiler.ErrNotModified)

	// Its content, whose size is its file's, of its fields, with its primary
	// email preferred, its birthday, its photo, rather than its default one,
	// and its notes
	read, object, err := filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c2.vcf"}})
	require.NoError(t, err)
	var content string
	require.NoError(t, read(ctx, func(r io.Reader) error {
		data, err := io.ReadAll(r)
		content = string(data)
		return err
	}))
	assert.Equal(t, int64(len(content)), *object.Size)
	cards, err := vcard.Decode(strings.NewReader(content))
	require.NoError(t, err)
	require.Len(t, cards, 1)
	card := cards[0]
	assert.Equal(t, "c2", card.UID)
	assert.Equal(t, "Ada Lovelace", card.FN)
	assert.Equal(t, vcard.Name{Family: "Lovelace", Given: "Ada"}, card.Name)
	require.Len(t, card.Emails, 2)
	assert.Equal(t, vcard.Value{Value: "ada@example.com", Types: []string{"home"}, Pref: true}, card.Emails[0], "the primary email is first")
	assert.Equal(t, []string{"work"}, card.Emails[1].Types)
	require.Len(t, card.Phones, 2)
	assert.Equal(t, vcard.Value{Value: "+44 20 7946 0000", Types: []string{"cell"}}, card.Phones[0], "mobile is cell")
	assert.ElementsMatch(t, []string{"home", "fax"}, card.Phones[1].Types)
	assert.Equal(t, []vcard.Organization{{Name: "Analytical Engines", Units: []string{"Research"}}}, card.Orgs)
	assert.Equal(t, []string{"Countess"}, card.Titles)
	require.NotNil(t, card.Birthday)
	assert.Equal(t, vcard.Date{Month: 12, Day: 10}, *card.Birthday)
	require.NotNil(t, card.Photo)
	assert.Equal(t, photo, card.Photo.Data, "its photo, at 512 pixels, is embedded, rather than a link")
	assert.Equal(t, "image/png", card.Photo.MediaType)
	assert.Empty(t, card.Photo.URI)

	// It has no size until it's read, as its photo isn't got for a listing,
	// or a get
	object, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c2.vcf"}})
	require.NoError(t, err)
	assert.Nil(t, object.Size)

	// A photo which can't be got is a link
	read, _, err = filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c1.vcf"}})
	require.NoError(t, err)
	require.NoError(t, read(ctx, func(r io.Reader) error {
		data, err := io.ReadAll(r)
		content = string(data)
		return err
	}))
	cards, err = vcard.Decode(strings.NewReader(content))
	require.NoError(t, err)
	require.Len(t, cards, 1)
	require.NotNil(t, cards[0].Photo)
	assert.Equal(t, endpoint+"/photos/missing=s100", cards[0].Photo.URI)
	assert.Equal(t, []string{"The first programmer"}, card.Notes)

	// A contact which is deleted, or isn't there, or a path which isn't a
	// contact's, isn't found, and the root can't be read
	// A contact without a name is of its primary email
	object, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c4.vcf"}})
	require.NoError(t, err)
	assert.Contains(t, object.Meta, schema.Meta{Namespace: google.DCNamespace, Key: "title", Value: []byte(`"primary@example.com"`)})

	// A profile, which isn't a contact, such as /me.vcf, or a person of
	// another id, isn't found, as neither is listed
	for _, path := range []string{"/me.vcf", "/alias.vcf"} {
		_, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: path}})
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
	}
	for _, path := range []string{"/c3.vcf", "/c9.vcf", "/c1.vcf/", "/c1.txt", "/a/c1.vcf", "/~new.vcf"} {
		_, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: path}})
		assert.ErrorIs(t, err, fs.ErrNotExist, path)
	}
	_, _, err = filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	assert.ErrorIs(t, err, fs.ErrInvalid)

	// It's read-only
	_, err = filer.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c1.vcf"}})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	_, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/c1.vcf"}})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
}

func Test_ContactWatch(t *testing.T) {
	fake, endpoint := newFakePeople(t)
	fake.add("c1", nil)
	fake.add("c2", nil)
	filer := newContactFiler(t, endpoint, url.Values{"refresh_interval": {"20ms"}})
	ctx, cancel := context.WithCancel(t.Context())
	events, done := make(chan schema.Event, 16), make(chan error, 1)
	go func() { done <- filer.WatchObjects(ctx, events) }()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case event := <-events:
		require.Equal(t, schema.Rescan("contacts"), event)
	case <-time.After(5 * time.Second):
		t.Fatal("no rescan")
	}

	// A contact which is created, one which is updated, and one which is
	// deleted
	fake.Lock()
	fake.people[0].ETag = "c1-2"
	fake.people[1].Metadata.Deleted = true
	fake.people = append(fake.people, httpclient.Person{ResourceName: "people/c3", ETag: "c3-1"})
	fake.Unlock()
	var got []string
	for len(got) < 3 {
		select {
		case event := <-events:
			got = append(got, event.EventType.String()+" "+event.Path)
		case <-time.After(5 * time.Second):
			t.Fatalf("waiting for events, got %v", got)
		}
	}
	assert.Equal(t, []string{"updated /c1.vcf", "deleted /c2.vcf", "created /c3.vcf"}, got)

	// Without a refresh interval, it isn't watched
	unwatched := newContactFiler(t, endpoint, url.Values{"refresh_interval": {"0s"}})
	assert.ErrorIs(t, unwatched.WatchObjects(t.Context(), events), gofiler.ErrNotImplemented)
}

func Test_ContactReadCancelled(t *testing.T) {
	fake, endpoint := newFakePeople(t)
	fake.add("c1", func(p *httpclient.Person) {
		p.Biographies = []httpclient.PersonValue{{Value: strings.Repeat("notes ", 100)}}
	})
	filer := newContactFiler(t, endpoint, nil)
	read, _, err := filer.ReadObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c1.vcf"}})
	require.NoError(t, err)

	// It's read, until the context is cancelled, when the rest of it isn't
	ctx, cancel := context.WithCancel(t.Context())
	err = read(ctx, func(r io.Reader) error {
		buf := make([]byte, 16)
		if _, err := r.Read(buf); err != nil {
			return err
		}
		cancel()
		_, err := r.Read(buf)
		return err
	})
	assert.ErrorIs(t, err, context.Canceled)
}

func Test_ContactCache(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFakePeople(t)
	fake.add("c1", func(p *httpclient.Person) {
		p.Names = []httpclient.PersonName{{DisplayName: "Ada"}}
	})
	filer := newContactFiler(t, endpoint, url.Values{"refresh_interval": {"1h"}})
	counts := func() (int, int) {
		fake.Lock()
		defer fake.Unlock()
		return fake.pages, fake.gets
	}

	// Once the contacts are listed, a contact is read of them, rather than
	// got again, for the refresh interval, until it's cleared, as a watch
	// does when it finds what changed
	_, err := filer.ListObjects(ctx, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	require.NoError(t, err)
	pages, gets := counts()
	object, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c1.vcf"}})
	require.NoError(t, err)
	assert.Contains(t, object.Meta, schema.Meta{Namespace: google.DCNamespace, Key: "title", Value: []byte(`"Ada"`)})
	_, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	require.NoError(t, err)
	p, g := counts()
	assert.Equal(t, []int{pages, gets}, []int{p, g}, "nothing is got again")
	google.ClearContactCache(filer)
	_, err = filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c1.vcf"}})
	require.NoError(t, err)
	_, g = counts()
	assert.Equal(t, gets+1, g)
}

// A writable volume's contact action creates a contact, updates the fields
// of one which are given, and deletes one, and needs a sign-in which can
// write contacts, and a read-only volume has no actions
func Test_ContactAction(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFakePeople(t)
	fake.add("ada", func(p *httpclient.Person) {
		p.Names = []httpclient.PersonName{{GivenName: "Ada", FamilyName: "Lovelace", DisplayName: "Ada Lovelace"}}
		p.EmailAddresses = []httpclient.PersonValue{{Value: "ada@example.com"}}
		p.Organizations = []httpclient.PersonOrg{{Name: "Royal Society"}, {Metadata: httpclient.FieldMetadata{Primary: true}, Name: "Analytical Engines", Title: "Programmer"}}
	})
	assert.Nil(t, newContactFiler(t, endpoint, nil).Actions(), "a read-only volume has none")
	_, err := newContactFilerOf(ctx, "contacts", url.Values{"readonly": {"false"}, "quota": {"0"}}, contactCredentials(t, endpoint))
	assert.Error(t, err, "a sign-in which can only read contacts can't write them")

	writer := newCredentials(t, endpoint, func(g *authclient.Grant) { g.Scopes = []string{httpclient.ContactsScope} })
	filer, err := newContactFilerOf(ctx, "contacts", url.Values{"readonly": {"false"}, "quota": {"0"}}, writer)
	require.NoError(t, err)
	assert.Equal(t, "false", filer.URL().Query().Get("readonly"))
	assert.False(t, filer.Readonly())
	actions := filer.Actions()
	require.Len(t, actions, 1)
	assert.Equal(t, "contact", actions[0].Name())
	run := func(input string) (*schema.Object, error) {
		return actions[0].Run(ctx, json.RawMessage(input))
	}
	person := func(id string) httpclient.Person {
		fake.Lock()
		defer fake.Unlock()
		for _, p := range fake.people {
			if p.ResourceName == "people/"+id {
				return p
			}
		}
		t.Fatalf("no contact %q", id)
		return httpclient.Person{}
	}

	// A contact is created, of its fields
	created, err := run(`{"op":"create","given_name":"Grace","family_name":"Hopper","emails":["grace@example.com"," "],"birthday":"1906-12-09","addresses":["1 Navy Way, Arlington"]}`)
	require.NoError(t, err)
	assert.Equal(t, "/n2.vcf", created.Path)
	grace := person("n2")
	require.Len(t, grace.Names, 1)
	assert.Equal(t, "Grace", grace.Names[0].GivenName)
	assert.Equal(t, []httpclient.PersonValue{{Value: "grace@example.com"}}, grace.EmailAddresses)
	require.Len(t, grace.Birthdays, 1)
	assert.Equal(t, 1906, grace.Birthdays[0].Date.Year)
	assert.Equal(t, "1 Navy Way, Arlington", grace.Addresses[0].FormattedValue)
	cards := readCards(t, filer, "/n2.vcf")
	require.Len(t, cards, 1)
	require.Len(t, cards[0].Addresses, 1)
	assert.Equal(t, "1 Navy Way, Arlington", cards[0].Addresses[0].Street, "an address of one line reads back")

	// It's updated, of the fields which are given, alone, and its name, and
	// organization, are merged with what it had
	_, err = run(`{"op":"update","path":"/ada.vcf","given_name":"Augusta"}`)
	require.NoError(t, err)
	fake.Lock()
	assert.Equal(t, []string{"names"}, fake.updated)
	fake.Unlock()
	ada := person("ada")
	assert.Equal(t, "Augusta", ada.Names[0].GivenName)
	assert.Equal(t, "Lovelace", ada.Names[0].FamilyName, "its family name is kept")
	assert.Equal(t, []httpclient.PersonValue{{Value: "ada@example.com"}}, ada.EmailAddresses, "its emails are kept")
	_, err = run(`{"op":"update","path":"/ada.vcf","emails":[],"job_title":"Analyst","birthday":"--12-10"}`)
	require.NoError(t, err)
	ada = person("ada")
	assert.Empty(t, ada.EmailAddresses, "an empty list clears them")
	assert.Equal(t, []httpclient.PersonOrg{{Name: "Royal Society"}, {Name: "Analytical Engines", Title: "Analyst"}}, ada.Organizations, "the primary one is updated, and the others kept")
	cards = readCards(t, filer, "/ada.vcf")
	require.Len(t, cards, 1)
	assert.Equal(t, []string{"Analyst"}, cards[0].Titles)
	require.Len(t, ada.Birthdays, 1)
	assert.Zero(t, ada.Birthdays[0].Date.Year)
	assert.Equal(t, 12, ada.Birthdays[0].Date.Month)

	// And deleted
	deleted, err := run(`{"op":"delete","path":"/n2.vcf"}`)
	require.NoError(t, err)
	assert.Equal(t, "/n2.vcf", deleted.Path)
	fake.Lock()
	assert.Len(t, fake.people, 1)
	fake.Unlock()

	// Input which isn't valid for its op
	for _, input := range []string{
		`{"op":"move","path":"/ada.vcf"}`,
		`{"op":"create"}`,
		`{"op":"create","path":"/ada.vcf","given_name":"x"}`,
		`{"op":"update","given_name":"x"}`,
		`{"op":"update","path":"/ada.vcf"}`,
		`{"op":"update","path":"/","given_name":"x"}`,
		`{"op":"update","path":"/ada.vcf","birthday":"tomorrow"}`,
		`{"op":"delete","path":"/ada.vcf","notes":"x"}`,
		`{"op":"create","given_name":"x","colour":"red"}`,
		`{"op":"delete","path":"/ada.vcf"} garbage`,
	} {
		_, err := run(input)
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, input)
	}
	_, err = run(`{"op":"update","path":"/zz.vcf","given_name":"x"}`)
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

// readCards returns the cards of a contact's content
func readCards(t *testing.T, filer *google.ContactFiler, path string) []*vcard.Card {
	t.Helper()
	read, _, err := filer.ReadObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: path}})
	require.NoError(t, err)
	var cards []*vcard.Card
	require.NoError(t, read(t.Context(), func(r io.Reader) error {
		cards, err = vcard.Decode(r)
		return err
	}))
	return cards
}

// Test_ContactWatchRestart tests a watch which starts again, of the
// volume's cache, syncs what changed while it wasn't watched, rather than
// getting every contact again, and reads the contacts it knows
func Test_ContactWatchRestart(t *testing.T) {
	fake, endpoint := newFakePeople(t)
	fake.add("c1", func(p *httpclient.Person) {
		p.Names = []httpclient.PersonName{{DisplayName: "Ada", GivenName: "Ada"}}
	})
	fake.add("c2", nil)
	cache := testCache()
	filer := func() *google.ContactFiler {
		t.Helper()
		f, err := google.NewContactFiler(t.Context(), "contacts", url.Values{"quota": {"0"}, "refresh_interval": {"10ms"}}, cache, gofiler.Opts{Credentials: contactCredentials(t, endpoint)})
		require.NoError(t, err)
		return f
	}
	fullSyncs := func() int {
		fake.Lock()
		defer fake.Unlock()
		return fake.fullSyncs
	}
	watch := func(f *google.ContactFiler) (chan schema.Event, func()) {
		t.Helper()
		ch, done := make(chan schema.Event, 64), make(chan error, 1)
		ctx, cancel := context.WithCancel(t.Context())
		go func() { done <- f.WatchObjects(ctx, ch) }()
		select {
		case event := <-ch:
			require.Equal(t, schema.EventTypeRescan, event.EventType)
		case <-time.After(5 * time.Second):
			t.Fatal("no rescan")
		}
		return ch, func() {
			cancel()
			assert.ErrorIs(t, <-done, context.Canceled)
		}
	}

	// The first watch gets every contact, and saves them, when it stops
	_, stop := watch(filer())
	assert.Equal(t, 1, fullSyncs())
	stop()

	// What changes while it isn't watched is synced, when it's watched
	// again, of the contacts it saved, which aren't got again
	fake.Lock()
	fake.people[0].ETag, fake.people[0].Names = "c1-2", []httpclient.PersonName{{DisplayName: "Augusta", GivenName: "Augusta"}}
	fake.people = fake.people[:1]
	fake.Unlock()
	fake.add("c3", nil)
	f := filer()
	_, stop = watch(f)
	assert.Equal(t, 1, fullSyncs(), "every contact isn't got again")
	assert.Equal(t, []string{"/c1.vcf", "/c3.vcf"}, listed(t, f, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}}))
	object, err := f.GetObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c1.vcf"}})
	require.NoError(t, err)
	assert.Equal(t, "c1-2", *object.ETag)
	stop()

	// A token which isn't accepted, such as one which has expired, gets
	// every contact again
	fake.Lock()
	fake.expired = true
	fake.Unlock()
	_, stop = watch(filer())
	assert.Equal(t, 2, fullSyncs())
	fake.Lock()
	fake.expired = false
	fake.Unlock()
	stop()
}

// Test_ContactActionWatch tests a contact the action writes is sent to the
// watch, as it's written, and isn't sent again, when it's synced
func Test_ContactActionWatch(t *testing.T) {
	fake, endpoint := newFakePeople(t)
	fake.add("ada", func(p *httpclient.Person) {
		p.Names = []httpclient.PersonName{{GivenName: "Ada"}}
	})
	writer := newCredentials(t, endpoint, func(g *authclient.Grant) { g.Scopes = []string{httpclient.ContactsScope} })
	filer, err := google.NewContactFiler(t.Context(), "contacts", url.Values{"readonly": {"false"}, "quota": {"0"}, "refresh_interval": {"10ms"}}, testCache(), gofiler.Opts{Credentials: writer})
	require.NoError(t, err)
	ch, done := make(chan schema.Event, 64), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { done <- filer.WatchObjects(ctx, ch) }()
	defer func() {
		cancel()
		assert.ErrorIs(t, <-done, context.Canceled)
	}()
	events := func() []string {
		t.Helper()
		var result []string
		for {
			select {
			case event := <-ch:
				result = append(result, event.EventType.String()+" "+event.Path)
			case <-time.After(150 * time.Millisecond):
				return result
			}
		}
	}
	assert.Equal(t, []string{"rescan /"}, events())
	run := func(input string) {
		t.Helper()
		_, err := filer.Actions()[0].Run(t.Context(), json.RawMessage(input))
		require.NoError(t, err)
	}
	run(`{"op":"update","path":"/ada.vcf","given_name":"Augusta"}`)
	assert.Equal(t, []string{"updated /ada.vcf"}, events(), "once, as it's written")
	run(`{"op":"delete","path":"/ada.vcf"}`)
	assert.Equal(t, []string{"deleted /ada.vcf"}, events())
}

// Test_ContactPhotoCache tests a contact's photo is kept in the volume's
// cache, so it isn't got each time the contact is read
func Test_ContactPhotoCache(t *testing.T) {
	fake, endpoint := newFakePeople(t)
	fake.add("c1", func(p *httpclient.Person) {
		p.Photos = []httpclient.PersonPhoto{{URL: endpoint + "/photos/ada=s100"}}
	})
	filer := newContactFiler(t, endpoint, nil)
	for range 3 {
		content := read(t, filer, "/c1.vcf")
		assert.Contains(t, content, "PHOTO")
		google.ClearContactCache(filer)
	}
	fake.Lock()
	assert.Equal(t, 1, fake.photos, "it's got once")
	fake.Unlock()
}

// Test_ContactWatchAbsent tests a contact the watch knows is deleted isn't
// read of what was got before it was watched, and a restart syncs of the
// last token, though nothing changed since it was saved
func Test_ContactWatchAbsent(t *testing.T) {
	// They're saved after each sync, so one of a new token, of nothing which
	// changed, is saved, too
	google.SetContactSaveInterval(t, 0)
	fake, endpoint := newFakePeople(t)
	fake.add("c1", nil)
	fake.add("c2", nil)
	cache := testCache()
	filer := func() *google.ContactFiler {
		t.Helper()
		f, err := google.NewContactFiler(t.Context(), "contacts", url.Values{"quota": {"0"}, "refresh_interval": {"10ms"}}, cache, gofiler.Opts{Credentials: contactCredentials(t, endpoint)})
		require.NoError(t, err)
		return f
	}
	f := filer()

	// It's listed before it's watched, so what was got is kept
	assert.Equal(t, []string{"/c1.vcf", "/c2.vcf"}, listed(t, f, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}}))
	ch, done := make(chan schema.Event, 64), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { done <- f.WatchObjects(ctx, ch) }()
	next := func() string {
		select {
		case event := <-ch:
			return event.EventType.String() + " " + event.Path
		case <-time.After(5 * time.Second):
			return ""
		}
	}
	assert.Equal(t, "rescan /", next())

	// A contact which is deleted isn't there, once the watch knows it
	fake.Lock()
	fake.people = fake.people[:1]
	fake.Unlock()
	assert.Equal(t, "deleted /c2.vcf", next())
	_, err := f.GetObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/c2.vcf"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)

	// It syncs, of new tokens, though nothing changes, and stops
	time.Sleep(50 * time.Millisecond)
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
	fake.Lock()
	last := len(fake.snapshots) - 1
	synced := len(fake.synced)
	fake.Unlock()

	// A restart syncs of the last token
	f = filer()
	ctx, cancel = context.WithCancel(t.Context())
	done = make(chan error, 1)
	go func() { done <- f.WatchObjects(ctx, ch) }()
	defer func() {
		cancel()
		<-done
	}()
	require.Eventually(t, func() bool {
		fake.Lock()
		defer fake.Unlock()
		return len(fake.synced) > synced
	}, 5*time.Second, 5*time.Millisecond)
	fake.Lock()
	n, err := strconv.Atoi(strings.TrimPrefix(fake.synced[synced], "s"))
	fake.Unlock()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, last-1, "the token which was saved is the last, or the one before, of a sync which was cancelled as it stopped")
}
