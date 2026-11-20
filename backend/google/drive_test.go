package google_test

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
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
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// fakeDrive is the Drive API, of the files a person owns, which it pages two
// at a time, and of the sign-in's token
type fakeDrive struct {
	sync.Mutex
	files    []httpclient.DriveFile
	content  map[string]string // Of a file's revision, <id>@<revision>, or an export, <id>:<type>
	lists    int               // How many times the files are listed
	exported []string          // The types files are exported as
	onExport func()            // Called as a file is exported, such as to change it
	ranges   []string          // The ranges of the downloads, or "" of all of a file
	noRanges bool              // A range is ignored, and all of a file is returned, as a server can

	// The changes, whose tokens are their indexes, and whether a token
	// isn't accepted, as it's expired, or Google is unavailable
	log         []httpclient.DriveChange
	expired     bool
	unavailable bool
	unlistable  bool // Listing the files, or the token from now, is unavailable

	// Uploads, by their sessions, and the bytes of each request of them
	uploads map[string]*fakeUpload
	chunks  []int
	denied  bool // An upload's token isn't accepted
}

// fakeUpload is an upload, of a new file, or of the file of its id
type fakeUpload struct {
	id   string
	meta httpclient.DriveWrite
	data []byte
}

const (
	driveRoot = "root0"
	docType   = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
)

func newFakeDrive(t *testing.T) (*fakeDrive, string) {
	t.Helper()
	modified := time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC)
	file := func(id, name, mimeType, parent string) httpclient.DriveFile {
		return httpclient.DriveFile{Id: id, Name: name, MimeType: mimeType, Parents: []string{parent}, ModifiedTime: modified, Version: "7", WebViewLink: "https://drive.google.com/file/d/" + id, Owners: []httpclient.DriveOwner{{EmailAddress: "ada@example.com"}}, OwnedByMe: true}
	}
	sized := func(f httpclient.DriveFile, size int) httpclient.DriveFile {
		f.Size, f.MD5Checksum, f.HeadRevision = strconv.Itoa(size), "md5-"+f.Id, "rev1"
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
		content: map[string]string{"a1@rev1": "hello", "a2@rev1": "jpeg", "d1:" + docType: "docx"},
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
	case f.unlistable && ((len(parts) == 2 && parts[1] == "startPageToken") || (len(parts) == 1 && parts[0] == "files")):
		writeError(w, http.StatusServiceUnavailable)
	case len(parts) == 2 && parts[0] == "changes" && parts[1] == "startPageToken":
		writeJSON(w, map[string]string{"startPageToken": strconv.Itoa(len(f.log))})
	case len(parts) == 1 && parts[0] == "changes":
		start, err := strconv.Atoi(query.Get("pageToken"))
		switch {
		case f.unavailable:
			writeError(w, http.StatusServiceUnavailable)
			return
		case f.expired, err != nil, start > len(f.log), query.Get("includeRemoved") != "true", query.Get("restrictToMyDrive") != "true":
			writeError(w, http.StatusNotFound)
			return
		}
		end := min(start+2, len(f.log))
		page := map[string]any{"changes": f.log[start:end]}
		if end < len(f.log) {
			page["nextPageToken"] = strconv.Itoa(end)
		} else {
			page["newStartPageToken"] = strconv.Itoa(end)
		}
		writeJSON(w, page)
	case len(parts) == 2 && parts[0] == "files" && parts[1] == "root":
		writeJSON(w, httpclient.DriveFile{Id: driveRoot})
	case len(parts) >= 2 && parts[0] == "upload":
		f.upload(w, r, parts[1:])
	case r.Method == http.MethodPost && len(parts) == 1 && parts[0] == "files":
		// A folder, or a file without content, is created
		var meta httpclient.DriveWrite
		if err := json.NewDecoder(r.Body).Decode(&meta); err != nil || meta.Name == "" || meta.MimeType == "" || len(meta.Parents) != 1 {
			writeError(w, http.StatusBadRequest)
			return
		}
		file := httpclient.DriveFile{Id: "g" + strconv.Itoa(len(f.files)), Name: meta.Name, MimeType: meta.MimeType, Parents: meta.Parents, Version: "1", OwnedByMe: true}
		if meta.MimeType != httpclient.DriveFolderType {
			file.Size, file.MD5Checksum, file.HeadRevision = "0", fmt.Sprintf("%x", md5.Sum(nil)), "rev1"
			f.content[file.Id+"@rev1"] = ""
		}
		f.files = append(f.files, file)
		f.log = append(f.log, httpclient.DriveChange{FileId: file.Id, File: file})
		writeJSON(w, file)
	case r.Method == http.MethodPatch && len(parts) == 2 && parts[0] == "files":
		// A file is binned
		var patch map[string]bool
		if err := json.NewDecoder(r.Body).Decode(&patch); err != nil || !patch["trashed"] {
			writeError(w, http.StatusBadRequest)
			return
		}
		for i := range f.files {
			if f.files[i].Id == parts[1] {
				f.files[i].Trashed, f.files[i].Version = true, nextVersion(f.files[i].Version)
				f.log = append(f.log, httpclient.DriveChange{FileId: parts[1], File: f.files[i]})
				writeJSON(w, f.files[i])
				return
			}
		}
		writeError(w, http.StatusNotFound)
	case len(parts) == 1 && parts[0] == "files":
		if query.Get("q") != "trashed = false and 'me' in owners" || query.Get("spaces") != "drive" {
			writeError(w, http.StatusBadRequest)
			return
		}
		if query.Get("pageToken") == "" {
			f.lists++
		}
		listed := slices.DeleteFunc(slices.Clone(f.files), func(file httpclient.DriveFile) bool { return file.Trashed })
		start, _ := strconv.Atoi(query.Get("pageToken"))
		end := min(start+2, len(listed))
		page := map[string]any{"files": listed[start:end]}
		if end < len(listed) {
			page["nextPageToken"] = strconv.Itoa(end)
		}
		writeJSON(w, page)
	case len(parts) == 4 && parts[0] == "files" && parts[2] == "revisions" && query.Get("alt") == "media":
		if content, exists := f.content[parts[1]+"@"+parts[3]]; exists {
			// A range, of bytes=<from>-, or bytes=<from>-<to>
			header := r.Header.Get("Range")
			f.ranges = append(f.ranges, header)
			status, contentRange := http.StatusOK, ""
			if spec, ok := strings.CutPrefix(header, "bytes="); ok && !f.noRanges {
				from, to, _ := strings.Cut(spec, "-")
				start, _ := strconv.Atoi(from)
				end := len(content) - 1
				if to != "" {
					end, _ = strconv.Atoi(to)
				}
				if start >= len(content) || end < start {
					writeError(w, http.StatusRequestedRangeNotSatisfiable)
					return
				}
				end = min(end, len(content)-1)
				contentRange = fmt.Sprintf("bytes %d-%d/%d", start, end, len(content))
				content, status = content[start:end+1], http.StatusPartialContent
			}

			// It's written without the fake locked, as a reader which seeks
			// doesn't read the rest of it
			f.Unlock()
			defer f.Lock()
			w.Header().Set("Content-Type", "application/octet-stream")
			if contentRange != "" {
				w.Header().Set("Content-Range", contentRange)
			}
			w.WriteHeader(status)
			_, _ = io.WriteString(w, content)
			return
		}
		writeError(w, http.StatusNotFound)
	case len(parts) == 2 && parts[0] == "files" && query.Get("fields") == "version":
		for _, file := range f.files {
			if file.Id == parts[1] {
				writeJSON(w, httpclient.DriveFile{Version: file.Version})
				return
			}
		}
		writeError(w, http.StatusNotFound)
	case len(parts) == 3 && parts[0] == "files" && parts[2] == "export":
		f.exported = append(f.exported, query.Get("mimeType"))
		if f.onExport != nil {
			f.onExport()
		}
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
	filer, err := google.NewDriveFiler(t.Context(), "drive", params, testCache(), gofiler.Opts{Credentials: driveCredentials(t, endpoint)})
	require.NoError(t, err)
	return filer
}

func Test_NewDriveFiler(t *testing.T) {
	ctx := t.Context()
	_, endpoint := newFakeDrive(t)

	// A sign-in without the drive's scope signs in again, asking for it,
	// and keeping those it has, such as of tasks
	_, err := google.NewDriveFiler(ctx, "drive", url.Values{"quota": {"0"}}, testCache(), gofiler.Opts{Credentials: newCredentials(t, endpoint, nil)})
	require.Equal(t, http.StatusUnauthorized, status(err))
	signIn := authclient.SignInOf(err)
	require.NotNil(t, signIn)
	assert.Equal(t, []string{httpclient.DriveReadonlyScope, httpclient.EmailScope, httpclient.TasksScope}, signIn.Scopes)

	// A sign-in which can write files can read them
	_, err = google.NewDriveFiler(ctx, "drive", url.Values{"quota": {"0"}}, testCache(), gofiler.Opts{Credentials: newCredentials(t, endpoint, func(g *authclient.Grant) {
		g.Scopes = []string{httpclient.DriveScope}
	})})
	require.NoError(t, err)

	// Parameters which aren't known, or can't be parsed, are bad
	for _, params := range []url.Values{{"hidden": {"true"}}, {"quota": {"-1"}}, {"refresh_interval": {"soon"}}, {"credential": {"S3_SECRET"}}, {"readonly": {"maybe"}}} {
		_, err := google.NewDriveFiler(ctx, "drive", params, testCache(), gofiler.Opts{Credentials: driveCredentials(t, endpoint)})
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, params)
	}
	// A writable volume of a sign-in which can only read files signs in
	// again, asking for the scope of writing them
	_, err = google.NewDriveFiler(ctx, "drive", url.Values{"readonly": {"false"}}, testCache(), gofiler.Opts{Credentials: driveCredentials(t, endpoint)})
	require.Equal(t, http.StatusUnauthorized, status(err))
	signIn = authclient.SignInOf(err)
	require.NotNil(t, signIn)
	assert.Contains(t, signIn.Scopes, httpclient.DriveScope)

	// Its URL has each of its parameters, and it's read-only, without
	// actions
	filer := newDriveFiler(t, endpoint, nil)
	assert.Equal(t, "gdrive://drive?credential=GOOGLE_TOKEN&description=Google+Drive&quota=0&readonly=true&refresh_interval=1m0s", filer.URL().String())
	assert.True(t, filer.Readonly())
	assert.Nil(t, filer.Actions())
	assert.ErrorIs(t, newDriveFiler(t, endpoint, url.Values{"refresh_interval": {"0"}}).WatchObjects(ctx, make(chan schema.Event)), gofiler.ErrNotImplemented, "without a refresh interval")
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
	assert.Equal(t, "d1/7", *plan.ETag, "its id, and version, as versions are each file's")
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

// change changes a file, of its id, which the fake must be locked for,
// which is a change
func (f *fakeDrive) change(id string, fn func(*httpclient.DriveFile)) {
	for i := range f.files {
		if f.files[i].Id == id {
			fn(&f.files[i])
			f.log = append(f.log, httpclient.DriveChange{FileId: id, File: f.files[i]})
		}
	}
}

// add adds a file, which is a change
func (f *fakeDrive) add(file httpclient.DriveFile) {
	f.Lock()
	defer f.Unlock()
	file.OwnedByMe = true
	f.files = append(f.files, file)
	f.log = append(f.log, httpclient.DriveChange{FileId: file.Id, File: file})
}

// remove deletes a file, which is a change, which says it's removed
func (f *fakeDrive) remove(id string) {
	f.Lock()
	defer f.Unlock()
	f.drop(id)
}

// drop deletes a file, as remove does, which the fake must be locked for
func (f *fakeDrive) drop(id string) {
	f.files = slices.DeleteFunc(f.files, func(file httpclient.DriveFile) bool { return file.Id == id })
	f.log = append(f.log, httpclient.DriveChange{FileId: id, Removed: true})
}

// update changes a file, of its id, which is a change
func (f *fakeDrive) update(id string, fn func(*httpclient.DriveFile)) {
	f.Lock()
	defer f.Unlock()
	f.change(id, fn)
}

func Test_DriveWatch(t *testing.T) {
	fake, endpoint := newFakeDrive(t)
	filer := newDriveFiler(t, endpoint, url.Values{"refresh_interval": {"10ms"}})
	ch, done := make(chan schema.Event, 64), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { done <- filer.WatchObjects(ctx, ch) }()
	defer func() {
		cancel()
		assert.ErrorIs(t, <-done, context.Canceled)
	}()
	// events returns the events which are sent, until none are, for a while
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
	lists := func() int {
		fake.Lock()
		defer fake.Unlock()
		return fake.lists
	}

	// The first change is a rescan of the root, once the files are listed,
	// which the filer lists, while it's watched, rather than getting them
	assert.Equal(t, []string{"rescan /"}, events())
	listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true})
	assert.Equal(t, 1, lists())

	// A file which is added is created, and is there at once
	fake.add(httpclient.DriveFile{Id: "n1", Name: "new.txt", MimeType: "text/plain", Parents: []string{"f1"}, Size: "3", MD5Checksum: "md5-n1"})
	assert.Equal(t, []string{"created /Work/new.txt"}, events())
	object, err := filer.GetObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/Work/new.txt"}})
	require.NoError(t, err)
	assert.Equal(t, "md5-n1", *object.ETag)

	// Its content changes, which updates it, and so does a Doc's version,
	// but not a folder's
	fake.Lock()
	fake.change("n1", func(f *httpclient.DriveFile) { f.MD5Checksum = "md5-n1b" })
	fake.change("d1", func(f *httpclient.DriveFile) { f.Version = "8" })
	fake.change("f2", func(f *httpclient.DriveFile) { f.Version = "8" })
	fake.Unlock()
	assert.Equal(t, []string{"updated /Work/Plan.docx", "updated /Work/new.txt"}, events())

	// A folder which is renamed is deleted, with what's in it, and created
	// where it is
	fake.update("f2", func(f *httpclient.DriveFile) { f.Name = "Archive" })
	assert.Equal(t, []string{
		"deleted /Work/Reports",
		"deleted /Work/Reports/Report (r2abcd).pdf",
		"deleted /Work/Reports/a_b.txt",
		"deleted /Work/Reports/report (r1abcd).pdf",
		"created /Work/Archive",
		"created /Work/Archive/Report (r2abcd).pdf",
		"created /Work/Archive/a_b.txt",
		"created /Work/Archive/report (r1abcd).pdf",
	}, events())

	// A file which is binned, removed, or given to another is deleted, and
	// one which becomes hidden too
	fake.Lock()
	fake.change("a2", func(f *httpclient.DriveFile) { f.Trashed = true })
	fake.drop("n1")
	fake.change("d2", func(f *httpclient.DriveFile) { f.OwnedByMe = false })
	fake.change("a1", func(f *httpclient.DriveFile) { f.Name = ".notes.txt" })
	fake.Unlock()
	assert.Equal(t, []string{"deleted /Work/Budget.xlsx", "deleted /Work/new.txt", "deleted /Work/photo.jpg", "deleted /notes.txt"}, events())
	_, err = filer.GetObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/notes.txt"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)

	// When Google is unavailable, it's polled again, and nothing is missed
	fake.Lock()
	fake.unavailable = true
	fake.Unlock()
	fake.add(httpclient.DriveFile{Id: "n2", Name: "later.txt", MimeType: "text/plain", Parents: []string{driveRoot}})
	assert.Empty(t, events())
	fake.Lock()
	fake.unavailable = false
	fake.Unlock()
	assert.Equal(t, []string{"created /later.txt"}, events())

	// A token which isn't accepted lists the files again, and rescans
	fake.Lock()
	fake.expired = true
	fake.Unlock()
	time.Sleep(50 * time.Millisecond)
	fake.Lock()
	fake.expired = false
	fake.Unlock()
	assert.Contains(t, events(), "rescan /")
	assert.Greater(t, lists(), 1, "once when it was watched, and again")

	// And then follows the changes again
	fake.add(httpclient.DriveFile{Id: "n3", Name: "after.txt", MimeType: "text/plain", Parents: []string{driveRoot}})
	assert.Equal(t, []string{"created /after.txt"}, events())

	// A token which isn't accepted, when the files can't be listed again, as
	// Google is unavailable, keeps the files it knows, and lists them at the
	// next interval, when it's available
	fake.Lock()
	fake.expired, fake.unlistable = true, true
	fake.Unlock()
	assert.Empty(t, events())
	select {
	case err := <-done:
		t.Fatalf("the watch stopped: %v", err)
	default:
	}
	assert.Contains(t, (listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}})), "/after.txt", "it's listed of the files it knows")
	fake.Lock()
	fake.expired, fake.unlistable = false, false
	fake.Unlock()
	assert.Equal(t, []string{"rescan /"}, events())

	// A folder which is another, of the same name, is updated, of its id
	fake.add(httpclient.DriveFile{Id: "e1", Name: "Empty", MimeType: httpclient.DriveFolderType, Parents: []string{driveRoot}})
	assert.Equal(t, []string{"created /Empty"}, events())
	fake.Lock()
	fake.drop("e1")
	fake.files = append(fake.files, httpclient.DriveFile{Id: "e2", Name: "Empty", MimeType: httpclient.DriveFolderType, Parents: []string{driveRoot}, OwnedByMe: true})
	fake.log = append(fake.log, httpclient.DriveChange{FileId: "e2", File: fake.files[len(fake.files)-1]})
	fake.Unlock()
	assert.Equal(t, []string{"updated /Empty"}, events())

	// Another watch, at the same time, is a conflict
	assert.ErrorIs(t, filer.WatchObjects(t.Context(), make(chan schema.Event)), gofiler.ErrConflict)
}

func Test_DriveWatchStops(t *testing.T) {
	fake, endpoint := newFakeDrive(t)
	filer := newDriveFiler(t, endpoint, url.Values{"refresh_interval": {"10ms"}})

	// The files are got, and kept, before it's watched
	assert.Contains(t, listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}}), "/notes.txt")
	ch, done := make(chan schema.Event, 16), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { done <- filer.WatchObjects(ctx, ch) }()
	next := func() string {
		select {
		case event := <-ch:
			return event.EventType.String() + " " + event.Path
		case <-time.After(2 * time.Second):
			return ""
		}
	}
	assert.Equal(t, "rescan /", next())
	fake.remove("a1")
	assert.Equal(t, "deleted /notes.txt", next())

	// Once it stops, the files kept from before it aren't listed, as they're
	// older, but are got again
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
	assert.NotContains(t, listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}}), "/notes.txt")

	// And it can be watched again
	ch2, done2 := make(chan schema.Event, 16), make(chan error, 1)
	ctx2, cancel2 := context.WithCancel(t.Context())
	go func() { done2 <- filer.WatchObjects(ctx2, ch2) }()
	select {
	case event := <-ch2:
		assert.Equal(t, schema.EventTypeRescan, event.EventType)
	case <-time.After(2 * time.Second):
		t.Fatal("no rescan")
	}
	cancel2()
	assert.ErrorIs(t, <-done2, context.Canceled)
}

func Test_DriveReadRevision(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFakeDrive(t)
	filer := newDriveFiler(t, endpoint, url.Values{"refresh_interval": {"1h"}})
	listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true})

	// A file which changes after it's listed is read of the revision it was
	// listed with, so its content is its object's
	fake.Lock()
	fake.change("a1", func(f *httpclient.DriveFile) { f.HeadRevision, f.MD5Checksum, f.Size = "rev2", "md5-new", "9" })
	fake.content["a1@rev2"] = "new hello"
	fake.Unlock()
	assert.Equal(t, "hello", read(t, filer, "/notes.txt"))

	// A Doc which changes after it's listed, or as it's exported, isn't
	// read, as an export is of its current version, and the files are got
	// again, so it's read of its new version
	fake.Lock()
	fake.change("d1", func(f *httpclient.DriveFile) { f.Version = "8" })
	fake.Unlock()
	readFn, object, err := filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Plan.docx"}})
	require.NoError(t, err)
	assert.Equal(t, "d1/7", *object.ETag)
	err = readFn(ctx, func(r io.Reader) error { return nil })
	assert.ErrorIs(t, err, gofiler.ErrPreconditionFailed)
	fake.Lock()
	fake.onExport = func() { fake.change("d1", func(f *httpclient.DriveFile) { f.Version = "9" }) }
	fake.Unlock()
	readFn, object, err = filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Plan.docx"}})
	require.NoError(t, err)
	assert.Equal(t, "d1/8", *object.ETag)
	err = readFn(ctx, func(r io.Reader) error { return nil })
	assert.ErrorIs(t, err, gofiler.ErrPreconditionFailed, "it changed as it was exported")
	fake.Lock()
	fake.onExport = nil
	fake.Unlock()
	assert.Equal(t, "docx", read(t, filer, "/Work/Plan.docx"))

	// An error of the function is returned as it is
	sentinel := errors.New("the destination is full")
	for _, p := range []string{"/Work/photo.jpg", "/Work/Plan.docx"} {
		readFn, _, err := filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: p}})
		require.NoError(t, err)
		err = readFn(ctx, func(r io.Reader) error { return sentinel })
		assert.Equal(t, sentinel, err, p)
	}
}

func Test_DriveReadRanges(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFakeDrive(t)
	content := strings.Repeat("0123456789", 200_000) // 2 MB, of four blocks
	fake.Lock()
	fake.files = append(fake.files, httpclient.DriveFile{Id: "v1", Name: "video.mp4", MimeType: "video/mp4", Parents: []string{driveRoot}, Size: strconv.Itoa(len(content)), MD5Checksum: "md5-v1", HeadRevision: "rev1", OwnedByMe: true})
	fake.content["v1@rev1"] = content
	fake.Unlock()
	filer := newDriveFiler(t, endpoint, nil)
	ranges := func() []string {
		fake.Lock()
		defer fake.Unlock()
		result := fake.ranges
		fake.ranges = nil
		return result
	}
	readFn, _, err := filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/video.mp4"}})
	require.NoError(t, err)
	require.NoError(t, readFn(ctx, func(r io.Reader) error {
		// It can seek, and be read at an offset, as a file can
		ra, ok := r.(io.ReaderAt)
		require.True(t, ok)
		rs, ok := r.(io.ReadSeeker)
		require.True(t, ok)

		// It's requested once it's read, from the start, which reads on
		buf := make([]byte, 10)
		_, err := io.ReadFull(rs, buf)
		require.NoError(t, err)
		assert.Equal(t, "0123456789", string(buf))
		_, err = io.ReadFull(rs, buf)
		require.NoError(t, err)
		assert.Equal(t, "0123456789", string(buf))
		assert.Equal(t, []string{""}, ranges(), "one request, from the start")

		// Seeking requests it from there, once it's read, and seeking where
		// it's read from doesn't
		_, err = rs.Seek(1_500_003, io.SeekStart)
		require.NoError(t, err)
		_, err = io.ReadFull(rs, buf)
		require.NoError(t, err)
		assert.Equal(t, "3456789012", string(buf))
		end, err := rs.Seek(-4, io.SeekEnd)
		require.NoError(t, err)
		assert.Equal(t, int64(len(content)-4), end)
		rest, err := io.ReadAll(rs)
		require.NoError(t, err)
		assert.Equal(t, "6789", string(rest))
		assert.Equal(t, []string{"bytes=1500003-", "bytes=1999996-"}, ranges())

		// ReadAt reads a block, which reads near it are of, and at the end,
		// it's short, and EOF
		n, err := ra.ReadAt(buf, 100)
		require.NoError(t, err)
		assert.Equal(t, 10, n)
		assert.Equal(t, "0123456789", string(buf))
		_, err = ra.ReadAt(buf, 1_000)
		require.NoError(t, err)
		assert.Equal(t, []string{"bytes=100-524387"}, ranges(), "one block")
		n, err = ra.ReadAt(buf, int64(len(content)-3))
		assert.Equal(t, 3, n)
		assert.ErrorIs(t, err, io.EOF)
		assert.Equal(t, "789", string(buf[:n]))
		return nil
	}))

	// A server which ignores a range, and returns all of the file, isn't
	// read as if it were of the range
	fake.Lock()
	fake.noRanges = true
	fake.Unlock()
	require.NoError(t, readFn(ctx, func(r io.Reader) error {
		_, err := r.(io.Seeker).Seek(1_000, io.SeekStart)
		assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable, "of a seek")
		_, err = r.(io.ReaderAt).ReadAt(make([]byte, 10), 2_000)
		assert.ErrorIs(t, err, gofiler.ErrServiceUnavailable, "of ReadAt")
		return nil
	}))
	fake.Lock()
	fake.noRanges = false
	fake.Unlock()

	// A file which can't be got is an error before it's read, and when it's
	// sought, so before a response of the range is written
	require.NoError(t, readFn(ctx, func(r io.Reader) error {
		fake.Lock()
		delete(fake.content, "v1@rev1")
		fake.Unlock()
		_, err := r.(io.Seeker).Seek(1_000, io.SeekStart)
		assert.ErrorIs(t, err, fs.ErrNotExist)
		return nil
	}))
	called := false
	err = readFn(ctx, func(r io.Reader) error { called = true; return nil })
	assert.ErrorIs(t, err, fs.ErrNotExist)
	assert.False(t, called)

	// And so is an empty file, which can't be got
	fake.Lock()
	fake.files = append(fake.files, httpclient.DriveFile{Id: "z1", Name: "empty.txt", MimeType: "text/plain", Parents: []string{driveRoot}, Size: "0", MD5Checksum: "md5-z1", HeadRevision: "rev1", OwnedByMe: true})
	fake.Unlock()
	filer = newDriveFiler(t, endpoint, url.Values{"refresh_interval": {"0"}})
	readFn, _, err = filer.ReadObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/empty.txt"}})
	require.NoError(t, err)
	called = false
	err = readFn(ctx, func(r io.Reader) error { called = true; return nil })
	assert.ErrorIs(t, err, fs.ErrNotExist)
	assert.False(t, called)
}

// upload is an upload's session, which is started, of a new file, or the
// file of its id, or a chunk of it, of its range, which is the last, when
// it says how many bytes the file is, when the file is written
func (f *fakeDrive) upload(w http.ResponseWriter, r *http.Request, parts []string) {
	if f.denied {
		w.Header().Set("WWW-Authenticate", `Bearer realm="https://accounts.google.com/", error="invalid_token"`)
		writeError(w, http.StatusUnauthorized)
		return
	}
	switch {
	case parts[0] == "files" && r.URL.Query().Get("uploadType") == "resumable":
		var meta httpclient.DriveWrite
		if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
			writeError(w, http.StatusBadRequest)
			return
		}
		u := &fakeUpload{meta: meta}
		if len(parts) == 2 && r.Method == http.MethodPatch {
			u.id = parts[1]
		} else if len(parts) != 1 || r.Method != http.MethodPost {
			writeError(w, http.StatusBadRequest)
			return
		}
		if f.uploads == nil {
			f.uploads = make(map[string]*fakeUpload)
		}
		session := "s" + strconv.Itoa(len(f.uploads))
		f.uploads[session] = u
		w.Header().Set("Location", "http://"+r.Host+"/upload/session/"+session)
		w.WriteHeader(http.StatusOK)
	case parts[0] == "session" && len(parts) == 2 && r.Method == http.MethodPut:
		u, exists := f.uploads[parts[1]]
		if !exists {
			writeError(w, http.StatusNotFound)
			return
		}
		data, _ := io.ReadAll(r.Body)
		f.chunks = append(f.chunks, len(data))
		span, total, _ := strings.Cut(strings.TrimPrefix(r.Header.Get("Content-Range"), "bytes "), "/")
		if from, _, _ := strings.Cut(span, "-"); span != "*" && from != strconv.Itoa(len(u.data)) {
			writeError(w, http.StatusBadRequest)
			return
		}
		u.data = append(u.data, data...)
		if total == "*" {
			w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(u.data)-1))
			w.WriteHeader(http.StatusPermanentRedirect)
			return
		} else if total != strconv.Itoa(len(u.data)) {
			writeError(w, http.StatusBadRequest)
			return
		}
		writeJSON(w, f.uploaded(u))
	default:
		writeError(w, http.StatusNotFound)
	}
}

// uploaded writes the file of an upload, which is a change, and returns it
func (f *fakeDrive) uploaded(u *fakeUpload) httpclient.DriveFile {
	i := slices.IndexFunc(f.files, func(file httpclient.DriveFile) bool { return file.Id == u.id })
	if i < 0 {
		f.files = append(f.files, httpclient.DriveFile{Id: "u" + strconv.Itoa(len(f.files)), Name: u.meta.Name, Parents: u.meta.Parents, OwnedByMe: true})
		i = len(f.files) - 1
	}
	file := &f.files[i]
	if u.meta.MimeType != "" {
		file.MimeType = u.meta.MimeType
	}
	file.Version = nextVersion(file.Version)
	file.HeadRevision = "rev" + file.Version
	file.Size, file.MD5Checksum = strconv.Itoa(len(u.data)), fmt.Sprintf("%x", md5.Sum(u.data))
	file.ModifiedTime = u.meta.ModifiedTime
	f.content[file.Id+"@"+file.HeadRevision] = string(u.data)
	f.log = append(f.log, httpclient.DriveChange{FileId: file.Id, File: *file})
	return *file
}

// nextVersion returns the version after a file's
func nextVersion(version string) string {
	v, _ := strconv.Atoi(version)
	return strconv.Itoa(v + 1)
}

// writableDriveFiler returns a filer of the fake, which is writable, of a
// sign-in which can write files, whose uploads are of 256 KiB requests
func writableDriveFiler(t *testing.T, endpoint string, params url.Values) *google.DriveFiler {
	t.Helper()
	if params == nil {
		params = url.Values{}
	}
	params.Set("quota", "0")
	params.Set("readonly", "false")
	filer, err := google.NewDriveFiler(t.Context(), "drive", params, testCache(), gofiler.Opts{Credentials: newCredentials(t, endpoint, func(g *authclient.Grant) {
		g.Scopes = []string{httpclient.DriveScope}
	})})
	require.NoError(t, err)
	google.SetDriveUploadChunk(filer, 256<<10)
	return filer
}

func Test_DriveWrite(t *testing.T) {
	ctx := t.Context()
	fake, endpoint := newFakeDrive(t)
	create := func(filer *google.DriveFiler, p, content string, opts ...func(*schema.CreateObjectRequest)) (*schema.Object, error) {
		req := schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: p}, Body: strings.NewReader(content)}
		for _, opt := range opts {
			opt(&req)
		}
		return filer.CreateObject(ctx, req)
	}

	// A read-only volume isn't written
	_, err := create(newDriveFiler(t, endpoint, nil), "/x.txt", "x")
	assert.ErrorIs(t, err, gofiler.ErrForbidden)
	filer := writableDriveFiler(t, endpoint, nil)
	assert.False(t, filer.Readonly())

	// A file is created, in a folder which is created, as it's read, in
	// requests of 256 KiB, of its type, of its extension, and modification
	// time
	content := strings.Repeat("0123456789abcdef", 40_000) // 640,000 bytes
	modified := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	object, err := create(filer, "/Work/Notes/big.html", content, func(req *schema.CreateObjectRequest) { req.ModTime = modified })
	require.NoError(t, err)
	assert.Equal(t, "/Work/Notes/big.html", object.Path)
	assert.Equal(t, int64(len(content)), *object.Size)
	assert.Equal(t, "text/html; charset=utf-8", object.ContentType)
	assert.True(t, modified.Equal(object.ModTime))
	fake.Lock()
	assert.Equal(t, []int{256 << 10, 256 << 10, len(content) - 512<<10}, fake.chunks)
	fake.chunks = nil
	fake.Unlock()
	assert.Equal(t, content, read(t, filer, "/Work/Notes/big.html"))
	assert.Contains(t, listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/Work"}}), "/Work/Notes")

	// A file which is there is a new revision of it, of the same id, and an
	// empty one is of one request
	object, err = create(filer, "/notes.txt", "updated")
	require.NoError(t, err)
	assert.Equal(t, []string{`"a1"`}, metaValues(object, google.DriveNamespace)[:1])
	assert.Equal(t, "updated", read(t, filer, "/notes.txt"))
	object, err = create(filer, "/empty.txt", "")
	require.NoError(t, err)
	assert.Equal(t, int64(0), *object.Size)
	fake.Lock()
	assert.Equal(t, []int{7, 0}, fake.chunks)
	fake.Unlock()

	// What can't be written
	for p, want := range map[string]error{
		"/Work":                             gofiler.ErrConflict,             // a folder
		"/Work/Plan.docx":                   gofiler.ErrForbidden,            // a Doc
		"/.secret":                          gofiler.ErrUnprocessableContent, // hidden
		"/.hidden/x.txt":                    gofiler.ErrUnprocessableContent, // in a hidden folder
		"/Work/PHOTO.jpg":                   gofiler.ErrConflict,             // another's name, ignoring case
		"/Work/Reports/REPORT (r1abcd).pdf": gofiler.ErrConflict,             // another's path, ignoring case
		"/Work/ new.txt":                    gofiler.ErrBadParameter,         // a space before
		"/Work /new.txt":                    gofiler.ErrBadParameter,         // a space after, of a folder
		"/notes.txt/x":                      gofiler.ErrConflict,             // in a file
		"/":                                 fs.ErrInvalid,
		"/Work/new/":                        fs.ErrInvalid,
	} {
		_, err := create(filer, p, "x")
		assert.ErrorIs(t, err, want, p)
	}
	_, err = create(filer, "/notes.txt", "x", func(req *schema.CreateObjectRequest) { req.IfNotExists = true })
	assert.ErrorIs(t, err, gofiler.ErrConflict)
	_, err = create(filer, "/missing.txt", "x", func(req *schema.CreateObjectRequest) { req.IfExists = true })
	assert.ErrorIs(t, err, gofiler.ErrPreconditionFailed)

	// An upload whose token isn't accepted is a sign-in, as other requests'
	// are, and the new file, without its content, isn't kept
	fake.Lock()
	fake.denied = true
	fake.Unlock()
	_, err = create(filer, "/denied.txt", "x")
	assert.Equal(t, http.StatusUnauthorized, status(err), "%v", err)
	fake.Lock()
	fake.denied = false
	fake.Unlock()
	assert.NotContains(t, listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}}), "/denied.txt")

	// A file which is written over, without a type, is of its extension's
	fake.Lock()
	fake.files = append(fake.files, httpclient.DriveFile{Id: "b1", Name: "page.html", MimeType: "application/octet-stream", Parents: []string{driveRoot}, Version: "1", OwnedByMe: true})
	fake.Unlock()
	object, err = create(filer, "/page.html", "<p>")
	require.NoError(t, err)
	assert.Equal(t, "text/html; charset=utf-8", object.ContentType)

	// Two creates of a new path don't both create it, so one of them, of
	// IfNotExists, is a conflict
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := create(filer, "/Work/Twice/once.txt", "x", func(req *schema.CreateObjectRequest) { req.IfNotExists = true })
			errs <- err
		}()
	}
	results := []error{<-errs, <-errs}
	assert.Equal(t, 1, slices.IndexFunc(results, func(err error) bool { return err == nil })+slices.IndexFunc(results, func(err error) bool { return errors.Is(err, gofiler.ErrConflict) }), "one, and the other: %v", results)
	assert.Contains(t, listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Twice"}}), "/Work/Twice/once.txt")
	assert.Len(t, listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Twice"}}), 1)

	// A file is deleted, to the bin, and a folder, when it's recursive, of
	// the files in it, but not the root
	deleted, err := filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/Work/photo.jpg"}})
	require.NoError(t, err)
	require.Len(t, deleted.Body, 1)
	assert.Equal(t, "/Work/photo.jpg", deleted.Body[0].Path)
	assert.NotContains(t, listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/Work"}}), "/Work/photo.jpg")
	_, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Reports"}})
	assert.ErrorIs(t, err, gofiler.ErrConflict)
	deleted, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Reports"}, Recursive: true})
	require.NoError(t, err)
	assert.Len(t, deleted.Body, 3)
	assert.NotContains(t, listed(t, filer, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true}), "/Work/Reports/a_b.txt")
	fake.Lock()
	trashed := map[string]bool{}
	for _, file := range fake.files {
		trashed[file.Id] = file.Trashed
	}
	fake.Unlock()
	assert.True(t, trashed["a2"], "the file")
	assert.True(t, trashed["f2"], "the folder")
	assert.False(t, trashed["s1"], "but not what's in it, which the folder's bin has")
	_, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}})
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	_, err = filer.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/missing.txt"}})
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

// Test_DriveWriteWatch tests what's written is sent to the watch as it's
// written, of a watch which doesn't poll until long after, and isn't sent
// again, when it's polled, of a watch which polls often
func Test_DriveWriteWatch(t *testing.T) {
	for _, interval := range []string{"1h", "10ms"} {
		t.Run(interval, func(t *testing.T) {
			_, endpoint := newFakeDrive(t)
			filer := writableDriveFiler(t, endpoint, url.Values{"refresh_interval": {interval}})
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
			_, err := filer.CreateObject(t.Context(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Notes/new.txt"}, Body: strings.NewReader("new")})
			require.NoError(t, err)
			assert.Equal(t, []string{"created /Work/Notes", "created /Work/Notes/new.txt", "updated /Work/Notes/new.txt"}, events(), "the file is created, without content, and then uploaded")
			_, err = filer.CreateObject(t.Context(), schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/notes.txt"}, Body: strings.NewReader("updated")})
			require.NoError(t, err)
			assert.Equal(t, []string{"updated /notes.txt"}, events())
			_, err = filer.DeleteObjects(t.Context(), schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Notes"}, Recursive: true})
			require.NoError(t, err)
			assert.Equal(t, []string{"deleted /Work/Notes", "deleted /Work/Notes/new.txt"}, events())
		})
	}
}

// Test_DriveWatchRestart tests a watch which starts again, of the volume's
// cache, carries on from where it was, rather than listing the files again,
// of what changed while it wasn't watched
func Test_DriveWatchRestart(t *testing.T) {
	fake, endpoint := newFakeDrive(t)
	cache := testCache()
	filer := func() *google.DriveFiler {
		t.Helper()
		f, err := google.NewDriveFiler(t.Context(), "drive", url.Values{"quota": {"0"}, "refresh_interval": {"10ms"}}, cache, gofiler.Opts{Credentials: driveCredentials(t, endpoint)})
		require.NoError(t, err)
		return f
	}
	lists := func() int {
		fake.Lock()
		defer fake.Unlock()
		return fake.lists
	}
	// watch watches the filer until the first event, a rescan, is sent,
	// and returns the function which stops it
	watch := func(f *google.DriveFiler) func() {
		t.Helper()
		ch, done := make(chan schema.Event, 64), make(chan error, 1)
		ctx, cancel := context.WithCancel(t.Context())
		go func() { done <- f.WatchObjects(ctx, ch) }()
		select {
		case event := <-ch:
			assert.Equal(t, schema.EventTypeRescan, event.EventType)
		case <-time.After(5 * time.Second):
			t.Fatal("no rescan")
		}
		return func() {
			cancel()
			assert.ErrorIs(t, <-done, context.Canceled)
		}
	}

	// The first watch lists the files, and saves them, when it stops
	stop := watch(filer())
	assert.Equal(t, 1, lists())
	stop()

	// What changes while it isn't watched is applied, when it's watched
	// again, of the files it saved, which aren't listed again
	fake.add(httpclient.DriveFile{Id: "n9", Name: "while-away.txt", MimeType: "text/plain", Parents: []string{driveRoot}, Size: "1", MD5Checksum: "md5-n9"})
	fake.update("f2", func(f *httpclient.DriveFile) { f.Name = "Archive" })
	f := filer()
	stop = watch(f)
	assert.Equal(t, 1, lists(), "the files aren't listed again")
	paths := listed(t, f, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}, Recursive: true})
	assert.Contains(t, paths, "/while-away.txt")
	assert.Contains(t, paths, "/Work/Archive/a_b.txt")
	assert.NotContains(t, paths, "/Work/Reports")
	stop()

	// A token which isn't accepted, when the files can't be listed again,
	// as Google is unavailable, keeps those which were saved, and lists
	// them, once it can
	fake.Lock()
	fake.expired, fake.unlistable = true, true
	fake.Unlock()
	f = filer()
	stop = watch(f)
	assert.Contains(t, listed(t, f, schema.ListObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/"}}), "/while-away.txt", "it's of the files which were saved")
	fake.Lock()
	fake.expired, fake.unlistable = false, false
	fake.Unlock()
	require.Eventually(t, func() bool { return lists() == 2 }, 5*time.Second, 5*time.Millisecond)
	stop()

	// A token which isn't accepted, such as one which has expired, lists
	// the files again
	fake.Lock()
	fake.expired = true
	fake.Unlock()
	f = filer()
	ch, done := make(chan schema.Event, 64), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { done <- f.WatchObjects(ctx, ch) }()
	require.Eventually(t, func() bool { return lists() == 3 }, 5*time.Second, 5*time.Millisecond)
	fake.Lock()
	fake.expired = false
	fake.Unlock()
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
}

// Test_DriveExportCache tests a Doc's export is kept in the volume's
// cache, so it isn't exported again, until its version changes, and can be
// read at an offset, and is deleted, when the watch finds it's removed
func Test_DriveExportCache(t *testing.T) {
	fake, endpoint := newFakeDrive(t)
	cache := testCache()
	filer, err := google.NewDriveFiler(t.Context(), "drive", url.Values{"quota": {"0"}, "refresh_interval": {"10ms"}}, cache, gofiler.Opts{Credentials: driveCredentials(t, endpoint)})
	require.NoError(t, err)
	exports := func() int {
		fake.Lock()
		defer fake.Unlock()
		return len(fake.exported)
	}

	// It's exported once, and then read of the cache, at an offset, too
	assert.Equal(t, "docx", read(t, filer, "/Work/Plan.docx"))
	assert.Equal(t, "docx", read(t, filer, "/Work/Plan.docx"))
	assert.Equal(t, 1, exports())
	readFn, _, err := filer.ReadObject(t.Context(), schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/Work/Plan.docx"}})
	require.NoError(t, err)
	require.NoError(t, readFn(t.Context(), func(r io.Reader) error {
		ra, ok := r.(io.ReaderAt)
		require.True(t, ok, "it can be read at an offset")
		buf := make([]byte, 2)
		_, err := ra.ReadAt(buf, 2)
		assert.Equal(t, "cx", string(buf))
		return err
	}))
	assert.Equal(t, 1, exports())

	// Once its version changes, it's exported again
	fake.Lock()
	fake.change("d1", func(f *httpclient.DriveFile) { f.Version = "8" })
	fake.Unlock()
	google.ClearDriveCache(filer)
	assert.Equal(t, "docx", read(t, filer, "/Work/Plan.docx"))
	assert.Equal(t, 2, exports())

	// And, when it's watched, and it's removed, its export is deleted
	ch, done := make(chan schema.Event, 64), make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	go func() { done <- filer.WatchObjects(ctx, ch) }()
	defer func() {
		cancel()
		assert.ErrorIs(t, <-done, context.Canceled)
	}()
	require.Equal(t, schema.EventTypeRescan, (<-ch).EventType)
	entry, err := cache.Get(t.Context(), "export/d1")
	require.NoError(t, err, "it's kept")
	entry.Close()
	fake.remove("d1")
	gone := func(id string) func() bool {
		return func() bool {
			entry, err := cache.Get(t.Context(), "export/"+id)
			if err == nil {
				entry.Close()
			}
			return errors.Is(err, gofiler.ErrNotFound)
		}
	}
	require.Eventually(t, gone("d1"), 5*time.Second, 5*time.Millisecond)

	// As is one which is gone, when the files are listed again, as the
	// token isn't accepted
	fake.Lock()
	fake.content["d2:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"] = "xlsx"
	fake.Unlock()
	assert.Equal(t, "xlsx", read(t, filer, "/Work/Budget.xlsx"))
	require.False(t, gone("d2")(), "it's kept")
	fake.Lock()
	fake.expired = true
	fake.drop("d2")
	fake.Unlock()
	require.Eventually(t, gone("d2"), 5*time.Second, 5*time.Millisecond)
	fake.Lock()
	fake.expired = false
	fake.Unlock()
}
