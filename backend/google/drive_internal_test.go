package google

import (
	"testing"

	// Packages
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
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
