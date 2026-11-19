package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	// Packages
	filer "github.com/mutablelogic/go-filer/schema"
	schema "github.com/mutablelogic/go-filer/store/sqlite/schema"
	ncruces "github.com/mutablelogic/go-sqlite/ncruces"
	_ "github.com/mutablelogic/go-sqlite/ncruces/fts5"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// search returns the keys of the objects of the volume which match, best
// first
func (self *Store) search(t *testing.T, volume, match string) []string {
	t.Helper()
	var keys schema.SearchKeys
	require.NoError(t, self.conn.List(context.Background(), &keys, schema.SearchMatch{Volume: volume, Match: match}))
	return keys
}

func Test_Search(t *testing.T) {
	ctx := context.Background()
	s, err := New(ctx, filepath.Join(t.TempDir(), "index.db"))
	require.NoError(t, err)
	defer s.Close()
	for _, name := range []string{"media", "photos"} {
		_, err := s.CreateVolume(ctx, filer.VolumeMeta{Name: name, URL: "file://" + name + "/tmp?index=metadata", Enabled: true})
		require.NoError(t, err)
	}
	object := func(path string, dir bool, meta ...filer.Meta) *filer.Object {
		return &filer.Object{ObjectKey: filer.ObjectKey{Path: path}, ObjectAttr: filer.ObjectAttr{IsDir: dir}, ObjectMeta: filer.ObjectMeta{Meta: meta}}
	}
	meta := func(ns, key, value string) filer.Meta {
		return filer.Meta{Namespace: ns, Key: key, Value: json.RawMessage(value)}
	}
	require.NoError(t, s.PutObject(ctx, "media", object("/", true),
		object("/Bern Photos", true), object("/Bern Photos/Ahrensburg", true),
		object("/Bern Photos/Ahrensburg/IMG_0001.jpg", false),
		object("/Notes", true),
		object("/Notes/zahler.txt", false, meta("dc", "title", `"Zählerstand benötigt"`)),
		object("/Notes/ab/cd/x.txt", false, meta("audio", "artist", `["Arvo Pärt", "https://example.com/arvo"]`)),
	))
	require.NoError(t, s.PutObject(ctx, "photos", object("/", true), object("/Ahrensburg.jpg", false)))

	// An object's name, the main fields of its metadata, the names of its
	// folders, and its other metadata are searched, without diacritics, of
	// the volume, and a folder is, by its name
	assert.Equal(t, []string{"/Bern Photos/Ahrensburg/", "/Bern Photos/Ahrensburg/IMG_0001.jpg"}, s.search(t, "media", "ahrensburg"))
	assert.Equal(t, []string{"/Notes/zahler.txt"}, s.search(t, "media", "zahlerstand"))
	assert.Equal(t, []string{"/Notes/ab/cd/x.txt"}, s.search(t, "media", "part"))
	assert.Equal(t, []string{"/Notes/ab/cd/x.txt"}, s.search(t, "media", "arv*"), "of a prefix")
	assert.Empty(t, s.search(t, "media", "example"), "not links")
	assert.Empty(t, s.search(t, "media", "cd"), "not folders of fewer than three characters")
	assert.Equal(t, []string{"/Ahrensburg.jpg"}, s.search(t, "photos", "ahrensburg"))

	// A name is weighed more than a folder, and a nearer folder more than one
	// further up
	assert.Equal(t, []string{"/Bern Photos/", "/Bern Photos/Ahrensburg/", "/Bern Photos/Ahrensburg/IMG_0001.jpg"}, s.search(t, "media", "bern"))

	// A task's metadata is searched, merged with the backend's, which it
	// replaces, and is removed, with its search, when it's deleted
	require.NoError(t, s.SetMetadata(ctx, "media", "/Notes/zahler.txt", "metadata", nil, filer.TaskMeta{Meta: []filer.Meta{meta("dc", "title", `"Meter reading"`), meta("dc", "creator", `"Stadtwerke"`)}}))
	assert.Equal(t, []string{"/Notes/zahler.txt"}, s.search(t, "media", "stadtwerke"))
	assert.Empty(t, s.search(t, "media", "zahlerstand"), "the task's title replaces the backend's")
	require.NoError(t, s.DeleteMetadata(ctx, "media", "metadata"))
	assert.Empty(t, s.search(t, "media", "stadtwerke"))
	assert.Equal(t, []string{"/Notes/zahler.txt"}, s.search(t, "media", "zahlerstand"))

	// An object which is put again is searched of what it is now, and one
	// which is deleted, or beneath a folder which is, isn't
	require.NoError(t, s.PutObject(ctx, "media", object("/Notes/zahler.txt", false, meta("dc", "title", `"Gas"`))))
	assert.Empty(t, s.search(t, "media", "zahlerstand"))
	assert.Equal(t, []string{"/Notes/zahler.txt"}, s.search(t, "media", "gas"))
	require.NoError(t, s.DeleteObject(ctx, "media", "/Bern Photos/"))
	assert.Empty(t, s.search(t, "media", "ahrensburg"))
	assert.Equal(t, []string{"/Ahrensburg.jpg"}, s.search(t, "photos", "ahrensburg"))
}

func Test_SearchMigrate(t *testing.T) {
	// A database of version 14, which has no search, has it built, of what
	// it has
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "index.db")
	s, err := New(ctx, path)
	require.NoError(t, err)
	_, err = s.CreateVolume(ctx, filer.VolumeMeta{Name: "media", URL: "file://media/tmp?index=objects", Enabled: true})
	require.NoError(t, err)
	require.NoError(t, s.PutObject(ctx, "media", &filer.Object{ObjectKey: filer.ObjectKey{Path: "/"}, ObjectAttr: filer.ObjectAttr{IsDir: true}}, &filer.Object{ObjectKey: filer.ObjectKey{Path: "/Ahrensburg.jpg"}}))
	require.NoError(t, s.Close())

	db, err := ncruces.Open(ctx, path, schema.Functions()...)
	require.NoError(t, err)
	require.NoError(t, db.Exec(ctx, `DROP TABLE object_search; DROP TRIGGER object_search_deleted; PRAGMA user_version = 14;`))
	require.NoError(t, db.Close())

	s, err = New(ctx, path)
	require.NoError(t, err)
	defer s.Close()
	assert.Equal(t, []string{"/Ahrensburg.jpg"}, s.search(t, "media", "ahrensburg"))
}
