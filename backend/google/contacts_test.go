package google_test

import (
	"context"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	gets     int // How many times a person is got
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
	switch {
	case len(parts) == 3 && parts[0] == "people" && parts[1] == "me" && parts[2] == "connections":
		f.pages++
		start, _ := strconv.Atoi(r.URL.Query().Get("pageToken"))
		end := min(start+2, len(f.people))
		page := map[string]any{"connections": f.people[start:end]}
		if end < len(f.people) {
			page["nextPageToken"] = strconv.Itoa(end)
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

// add adds a contact, of its id, and changes it with fn
func (f *fakePeople) add(id string, fn func(*httpclient.Person)) {
	f.Lock()
	defer f.Unlock()
	person := httpclient.Person{ResourceName: "people/" + id, ETag: id + "-1", Metadata: httpclient.PersonMetadata{Sources: []httpclient.PersonSource{{Type: "CONTACT"}}}}
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

	// Writing isn't implemented, so far, and parameters which aren't known,
	// or can't be parsed, are bad
	_, err = newContactFilerOf(ctx, "contacts", url.Values{"readonly": {"false"}}, contactCredentials(t, endpoint))
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)
	for _, params := range []url.Values{{"hidden": {"true"}}, {"quota": {"-1"}}, {"refresh_interval": {"soon"}}, {"credential": {"S3_SECRET"}}} {
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
