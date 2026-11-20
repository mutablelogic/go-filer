package google

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	// Packages
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	sqlite "github.com/mutablelogic/go-filer/store/sqlite"
	gosqlite "github.com/mutablelogic/go-sqlite"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// Test_ApplyChanges tests a change older than a file, or than its removal,
// isn't applied, so a poll which got it before a write doesn't undo it
func Test_ApplyChanges(t *testing.T) {
	file := func(version string, trashed bool) httpclient.DriveFile {
		return httpclient.DriveFile{Id: "a", Name: "a.txt", Version: version, Trashed: trashed, OwnedByMe: true, Parents: []string{"root"}}
	}
	change := func(f httpclient.DriveFile) httpclient.DriveChange {
		return httpclient.DriveChange{FileId: f.Id, File: f}
	}
	files, gone := map[string]httpclient.DriveFile{}, map[string]int64{}

	// Version 8 is written, and then a poll's version 7 isn't applied
	applyChanges(files, gone, []httpclient.DriveChange{change(file("8", false))})
	applyChanges(files, gone, []httpclient.DriveChange{change(file("7", false))})
	assert.Equal(t, "8", files["a"].Version)

	// It's binned, of version 9, and then a poll's version 8 doesn't restore
	// it, nor its version 9, but a restore, of version 10, does
	applyChanges(files, gone, []httpclient.DriveChange{change(file("9", true))})
	assert.NotContains(t, files, "a")
	assert.Equal(t, int64(9), gone["a"])
	applyChanges(files, gone, []httpclient.DriveChange{change(file("8", false)), change(file("9", true))})
	assert.NotContains(t, files, "a")
	applyChanges(files, gone, []httpclient.DriveChange{change(file("10", false))})
	assert.Equal(t, "10", files["a"].Version)
	assert.NotContains(t, gone, "a")

	// A change which says it's removed, without the file, removes it
	applyChanges(files, gone, []httpclient.DriveChange{{FileId: "a", Removed: true}})
	assert.NotContains(t, files, "a")
}

// Test_WroteWhileRelisting tests what the filer writes as the files are
// listed again is kept, to be applied to them once they are, and is applied
// to the files the watch knows, and sent, until then
func Test_WroteWhileRelisting(t *testing.T) {
	root := "root"
	filer := &DriveFiler{name: "drive"}
	w := &driveWatch{root: root, files: map[string]httpclient.DriveFile{}, gone: map[string]int64{}, signal: make(chan struct{}, 1)}
	w.tree = newDriveTree(root, nil)
	filer.live, w.relisting = w, true
	filer.wrote(&httpclient.DriveFile{Id: "n", Name: "new.txt", Version: "1", OwnedByMe: true, Parents: []string{root}})
	assert.Len(t, w.during, 1, "it's kept, for the listing")
	assert.Equal(t, []schema.Event{{EventType: schema.EventTypeCreated, ObjectKey: schema.ObjectKey{Name: "drive", Path: "/new.txt"}}}, w.pending, "and sent, until then")

	// A listing, from before it, which hasn't it, has it, once it's applied
	listed := map[string]httpclient.DriveFile{}
	applyChanges(listed, map[string]int64{}, w.during)
	assert.Contains(t, newDriveTree(root, []httpclient.DriveFile{listed["n"]}).files, "/new.txt")
}

// Test_DriveRestore tests the files a watch saved, with the versions of
// those which were removed, are restored, of the account's My Drive, but
// not of another's, and what the filer wrote as it started is applied
func Test_DriveRestore(t *testing.T) {
	root := "root0"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":%q}`, root)
	}))
	defer server.Close()
	client, err := httpclient.NewDriveClient(server.URL)
	require.NoError(t, err)
	cache, err := sqlite.NewCacheStore(t.Context(), gosqlite.Memory)
	require.NoError(t, err)
	defer cache.Close()
	filer := &DriveFiler{name: "drive", client: client, cache: cache}

	// The files, and those which were removed, are saved
	file := httpclient.DriveFile{Id: "a", Name: "a.txt", Version: "3", OwnedByMe: true, Parents: []string{root}}
	saved := &driveWatch{token: "t1", root: root, files: map[string]httpclient.DriveFile{"a": file}, gone: map[string]int64{"b": 9}, dirty: true}
	saved.tree = newDriveTree(root, []httpclient.DriveFile{file})
	filer.save(t.Context(), saved, true)

	// And restored, of the same account
	w := &driveWatch{}
	require.True(t, filer.restore(t.Context(), w))
	assert.Equal(t, "t1", w.token)
	assert.Equal(t, map[string]int64{"b": 9}, w.gone, "so an older change doesn't restore what was removed")
	assert.Contains(t, w.tree.files, "/a.txt")

	// What the filer wrote as it started is applied, once they're known
	w.relisting, w.during = true, []httpclient.DriveChange{{FileId: "c", File: httpclient.DriveFile{Id: "c", Name: "c.txt", Version: "1", OwnedByMe: true, Parents: []string{root}}}}
	filer.applyDuring(w)
	assert.Contains(t, w.tree.files, "/c.txt")
	assert.False(t, w.relisting)

	// But not of another account's
	root = "root1"
	assert.False(t, filer.restore(t.Context(), &driveWatch{}))
}
