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
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// fakeDrive is the Drive API, of the files a person owns, which it pages two
// at a time, and of the sign-in's token
type fakeDrive struct {
	sync.Mutex
	files    []httpclient.DriveFile
	content  map[string]string // Of a file, or an export, of its id
	lists    int               // How many times the files are listed
	exported []string          // The types files are exported as
}

const (
	driveRoot = "root0"
	docType   = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)

func newFakeDrive(t *testing.T) (*fakeDrive, string) {
	t.Helper()
	modified := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	file := func(id, name, mimeType, parent string) httpclient.DriveFile {
		return httpclient.DriveFile{Id: id, Name: name, MimeType: mimeType, Parents: []string{parent}, ModifiedTime: modified, Version: "7", WebViewLink: "https://drive.google.com/file/d/" + id, Owners: []httpclient.DriveOwner{{EmailAddress: "ada@example.com"}}}
	}
	sized := func(f httpclient.DriveFile, size int) httpclient.DriveFile {
		f.Size, f.MD5Checksum = strconv.Itoa(size), "md5-"+f.Id
		return f
	}
	fake := &fakeDrive{
		files: []httpclient.DriveFile{
			file("f1", "Work", httpclient.DriveFolderType, driveRoot),
			file("f2", "Reports", httpclient.DriveFolderType, "f1"),
			sized(file("a1", "notes.txt", "text/plain", driveRoot), 5),
			sized(file("a2", "photo.jpg", "image/jpeg", "f1"), 4),
			file("d1", "Plan", httpclient.DriveAppsPrefix+"document", "f1"),
			file("d2", "Budget.xlsx", httpclient.DriveAppsPrefix+"spreadsheet", "f1"),
			sized(file("r1abcdefgh", "report.pdf", "application/pdf", "f2"), 3),
			sized(file("r2abcdefgh", "Report.pdf", "application/pdf", "f2"), 3),
			sized(file("s1", "a/b.txt", "text/plain", "f2"), 1),
			sized(file("h1", ".DS_Store", "application/octet-stream", driveRoot), 1),
			file("h2", ".hidden", httpclient.DriveFolderType, driveRoot),
			sized(file("h3", "in-hidden.txt", "text/plain", "h2"), 1),
			file("x1", "Survey", httpclient.DriveAppsPrefix+"form", driveRoot),
			file("x2", "Link", httpclient.DriveAppsPrefix+"shortcut", driveRoot),
			sized(file("o1", "orphan.txt", "text/plain", "elsewhere"), 1),
		},
		content: map[string]string{"a1": "hello", "a2": "jpeg", "d1:" + docType: "docx"},
	}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	google.SetDriveEndpoint(t, server.URL)
	return fake, server.URL
}

func (f *fakeDrive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.Lock()
	defer f.Unlock()
	if r.Header.Get("Authorization") != "Bearer "+token {
		w.Header().Set("WWW-Authenticate", `Bearer realm="https://accounts.google.com/", error="invalid_token"`)
		writeError(w, http.StatusUnauthorized)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	query := r.URL.Query()
	switch {
	case len(parts) == 2 && parts[0] == "files" && parts[1] == "root":
		writeJSON(w, httpclient.DriveFile{Id: driveRoot})
	case len(parts) == 1 && parts[0] == "files":
		if query.Get("q") != "trashed = false and 'me' in owners" || query.Get("spaces") != "drive" {
			writeError(w, http.StatusBadRequest)
			return
		}
		if query.Get("pageToken") == "" {
			f.lists++
		}
		start, _ := strconv.Atoi(query.Get("pageToken"))
		end := min(start+2, len(f.files))
		page := map[string]any{"files": f.files[start:end]}
		if end < len(f.files) {
			page["nextPageToken"] = strconv.Itoa(end)
		}
		writeJSON(w, page)
	case len(parts) == 2 && parts[0] == "files" && query.Get("alt") == "media":
		if content, exists := f.content[parts[1]]; exists {
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = io.WriteString(w, content)
			return
		}
		writeError(w, http.StatusNotFound)
	case len(parts) == 3 && parts[0] == "files" && parts[2] == "export":
		f.exported = append(f.exported, query.Get("mimeType"))
		if content, exists := f.content[parts[1]+":"+query.Get("mimeType")]; exists {
			w.Header().Set("Content-Type", query.Get("mimeType"))
			_, _ = io.WriteString(w, content)
			return
		}
		writeError(w, http.StatusForbidden)
	default:
		writeError(w, http.StatusNotFound)
	}
}

// driveCredentials returns the provider of a sign-in which can read files
func driveCredentials(t *testing.T, endpoint string) *credentials {
	return newCredentials(t, endpoint, func(g *authclient.Grant) {
		g.Scopes = []string{httpclient.DriveReadonlyScope}
	})
}

// newDriveFiler returns a filer of the fake, without a quota
func newDriveFiler(t *testing.T, endpoint string, params url.Values) *google.DriveFiler {
	t.Helper()
	if params == nil {
		params = url.Values{}
	}
	params.Set("quota", "0")
	filer, err := google.NewDriveFiler(t.Context(), "drive", params, gofiler.Opts{Credentials: driveCredentials(t, endpoint)})
	require.NoError(t, err)
	return filer
}

func Test_NewDriveFiler(t *testing.T) {
	ctx := t.Context()
	_, endpoint := newFakeDrive(t)

	// A sign-in without the drive's scope signs in again, asking for it,
	// and keeping those it has, such as of tasks
	_, err := google.NewDriveFiler(ctx, "drive", url.Values{"quota": {"0"}}, gofiler.Opts{Credentials: newCredentials(t, endpoint, nil)})
	require.Equal(t, http.StatusUnauthorized, status(err))
	signIn := authclient.SignInOf(err)
	require.NotNil(t, signIn)
	assert.Equal(t, []string{httpclient.DriveReadonlyScope, httpclient.EmailScope, httpclient.TasksScope}, signIn.Scopes)

	// A sign-in which can write files can read them
	_, err = google.NewDriveFiler(ctx, "drive", url.Values{"quota": {"0"}}, gofiler.Opts{Credentials: newCredentials(t, endpoint, func(g *authclient.Grant) {
		g.Scopes = []string{httpclient.DriveScope}
	})})
	require.NoError(t, err)

	// Parameters which aren't known, or can't be parsed, are bad, and a
	// drive isn't written, so far
	for _, params := range []url.Values{{"hidden": {"true"}}, {"quota": {"-1"}}, {"refresh_interval": {"soon"}}, {"credential": {"S3_SECRET"}}, {"readonly": {"maybe"}}} {
		_, err := google.NewDriveFiler(ctx, "drive", params, gofiler.Opts{Credentials: driveCredentials(t, endpoint)})
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, params)
	}
	_, err = google.NewDriveFiler(ctx, "drive", url.Values{"readonly": {"false"}}, gofiler.Opts{Credentials: driveCredentials(t, endpoint)})
	assert.ErrorIs(t, err, gofiler.ErrNotImplemented)

	// Its URL has each of its parameters, and it's read-only, without
	// actions, and isn't watched
	filer := newDriveFiler(t, endpoint, nil)
	assert.Equal(t, "gdrive://drive?credential=GOOGLE_TOKEN&description=Google+Drive&quota=0&readonly=true&refresh_interval=1m0s", filer.URL().String())
	assert.True(t, filer.Readonly())
	assert.Nil(t, filer.Actions())
	assert.ErrorIs(t, filer.WatchObjects(ctx, make(chan schema.Event)), gofiler.ErrNotImplemented)
	_, err = filer.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/x.txt"}})
	assert.ErrorIs(t, err, gofiler.ErrForbidden)

	// A clone has the patch
	clone, err := filer.Clone(ctx, url.Values{"refresh_interval": {"5m"}})
	require.NoError(t, err)
	assert.Equal(t, "5m0s", clone.URL().Query().Get("refresh_interval"))
}

func Test_DriveObjects(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFakeDrive(t)
	filer := newDriveFiler(t, endpoint, nil)

	// The root lists its folders, and files, which aren't hidden, nor
	// Google's own, other than a Doc, Sheet or Slides, nor what isn't
	// beneath it
	assert.Equal(t, []string{"/Work", "/notes.txt"}, (listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}})))

	// Recursively, every directory, and file, beneath it, of their names in
	// their folders: a Doc has its extension, a name which is another's,
	// ignoring case, has the start of its id, and a "/" is "_"
	assert.Equal(t, []string{
		"/Work",
		"/Work/Budget.xlsx",
		"/Work/Plan.docx",
		"/Work/Reports",
		"/Work/Reports/Report (r2abcd).pdf",
		"/Work/Reports/a_b.txt",
		"/Work/Reports/report (r1abcd).pdf",
		"/Work/photo.jpg",
		"/notes.txt",
	}, (listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true})))
	assert.Equal(t, []string{"/Work/Reports/a_b.txt", "/Work/Reports/report (r1abcd).pdf"}, (listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Reports"}, StartAfter: "/Work/Reports/Report (r2abcd).pdf"})))
	assert.Equal(t, []string{"/notes.txt"}, (listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/notes.txt"}})))

	// A folder is a directory, of how many objects are in it, and a file
	// has its size, and checksum, as its entity tag, and its link, but a
	// Doc has its version, and is exported
	work, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/Work"}})
	require.NoError(t, err)
	assert.True(t, work.IsDir)
	assert.Equal(t, []string{"true", "4"}, metaValues(work, schema.FolderNamespace))
	notes, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/notes.txt"}})
	require.NoError(t, err)
	assert.Equal(t, "text/plain", notes.ContentType)
	assert.Equal(t, int64(5), *notes.Size)
	assert.Equal(t, "md5-a1", *notes.ETag)
	assert.Equal(t, []string{`"a1"`, `"https://drive.google.com/file/d/a1"`, `"ada@example.com"`}, metaValues(notes, google.DriveNamespace))
	plan, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Plan.docx"}})
	require.NoError(t, err)
	assert.Equal(t, docType, plan.ContentType)
	assert.Nil(t, plan.Size)
	assert.Equal(t, "7", *plan.ETag)
	assert.Contains(t, metaValues(plan, google.DriveNamespace), `"document"`)

	// What isn't there, or is hidden, or is a file with a "/", doesn't exist
	for _, p := range []string{"/missing.txt", "/.DS_Store", "/.hidden/in-hidden.txt", "/Survey", "/orphan.txt", "/notes.txt/"} {
		_, err := filer.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: p}})
		assert.ErrorIs(t, err, fs.ErrNotExist, p)
	}
	_, err = filer.ListObjects(ctx, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/missing"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)

	// A file is read as it's downloaded, and a Doc as it's exported, and a
	// directory can't be read, nor a file which hasn't changed
	assert.Equal(t, "hello", read(t, filer, "/notes.txt"))
	assert.Equal(t, "docx", read(t, filer, "/Work/Plan.docx"))
	fake.Lock()
	assert.Equal(t, []string{docType}, fake.exported)
	fake.Unlock()
	_, _, err = filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/Work"}})
	assert.ErrorIs(t, err, fs.ErrInvalid)
	_, _, err = filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/notes.txt"}, IfNoneMatch: []string{"md5-a1"}})
	assert.ErrorIs(t, err, gofiler.ErrNotModified)

	// A Sheet which Google won't export, as it's too large, is forbidden
	readFn, _, err := filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Budget.xlsx"}})
	require.NoError(t, err)
	err = readFn(ctx, func(r io.Reader) error { _, err := io.ReadAll(r); return err })
	assert.ErrorIs(t, err, gofiler.ErrForbidden)

	// The files were listed once, for the refresh interval
	fake.Lock()
	assert.Equal(t, 1, fake.lists)
	fake.Unlock()
}

func Test_DriveCache(t *testing.T) {
	fake, endpoint := newFakeDrive(t)
	lists := func() int {
		fake.Lock()
		defer fake.Unlock()
		return fake.lists
	}

	// Without a refresh interval, the files are listed each time
	filer := newDriveFiler(t, endpoint, url.Values{"refresh_interval": {"0"}})
	listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/Work"}})
	assert.Equal(t, 2, lists())

	// A file which is added is listed once they're listed again
	fake.Lock()
	fake.files = append(fake.files, httpclient.DriveFile{Id: "n1", Name: "new.txt", MimeType: "text/plain", Parents: []string{driveRoot}})
	fake.Unlock()
	assert.Contains(t, (listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}})), "/new.txt")
}

func Test_DriveReadCancelled(t *testing.T) {
	_, endpoint := newFakeDrive(t)
	filer := newDriveFiler(t, endpoint, nil)
	readFn, _, err := filer.ReadObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/notes.txt"}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	assert.ErrorIs(t, readFn(ctx, func(r io.Reader) error { _, err := io.ReadAll(r); return err }), context.Canceled)
}
