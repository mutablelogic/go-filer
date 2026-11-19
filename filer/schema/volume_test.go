package schema

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"
)

func Test_VolumePatch(t *testing.T) {
	// null removes an option, a list replaces its values, where an empty
	// list has none, and a string, number or boolean is the one value
	var patch VolumePatch
	if err := json.Unmarshal([]byte(`{"a": null, "b": ["x", "y"], "c": [], "d": "x", "e": true, "f": 1.5, "g": [false, 2]}`), &patch); err != nil {
		t.Fatal(err)
	}
	values, err := patch.Values()
	if err != nil {
		t.Fatal(err)
	}
	want := url.Values{"a": {}, "b": {"x", "y"}, "c": {""}, "d": {"x"}, "e": {"true"}, "f": {"1.5"}, "g": {"false", "2"}}
	if len(values) != len(want) {
		t.Fatalf("got %v, want %v", values, want)
	}
	for key, want := range want {
		got, exists := values[key]
		if !exists || len(got) != len(want) {
			t.Errorf("%s: got %q, want %q", key, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("%s: got %q, want %q", key, got, want)
			}
		}
	}

	// An object, or a list of them, or of lists, isn't an option
	for _, body := range []string{`{"a": {}}`, `{"a": [{}]}`, `{"a": [[]]}`, `{"a": [null]}`, `{"": true}`} {
		var patch VolumePatch
		if err := json.Unmarshal([]byte(body), &patch); err != nil {
			t.Fatal(err)
		} else if _, err := patch.Values(); err == nil {
			t.Errorf("%s: no error", body)
		}
	}
}

func Test_FormatSize(t *testing.T) {
	for size, want := range map[int64]string{
		0:          "0 B",
		1023:       "1023 B",
		1024:       "1.0 KiB",
		1536:       "1.5 KiB",
		5 << 20:    "5.0 MiB",
		3 << 30:    "3.0 GiB",
		2 << 40:    "2.0 TiB",
		2048 << 40: "2048.0 TiB",
	} {
		if got := FormatSize(size); got != want {
			t.Errorf("FormatSize(%d) = %q, want %q", size, got, want)
		}
	}
}

func Test_MatchType(t *testing.T) {
	dir := &Object{ObjectAttr: ObjectAttr{IsDir: true}}
	typed := &Object{ObjectMeta: ObjectMeta{ContentType: "inode/directory"}, ObjectAttr: ObjectAttr{IsDir: true}}
	task := &Object{ObjectMeta: ObjectMeta{ContentType: "text/calendar; component=VTODO"}}
	image := &Object{ObjectMeta: ObjectMeta{ContentType: "image/png"}}
	untyped := &Object{}
	for _, test := range []struct {
		object *Object
		t      string
		want   bool
	}{
		{dir, "", true},
		{dir, "inode/directory", true},
		{dir, "inode", true},
		{dir, "directory", true},
		{dir, "Directory", true},
		{dir, "inode/directory; charset=binary", true},
		{typed, "INODE/Directory", true},
		{dir, "image", false},
		{task, "text/calendar", true},
		{task, "text", true},
		{task, "calendar", true},
		{task, "text/calendar; component=VTODO", true},
		{task, "text/plain", false},
		{image, "image", true},
		{image, "image/png", true},
		{image, "image/jpeg", false},
		{image, "inode", false},
		{image, "png", true},
		{image, "directory", false},
		{untyped, "inode", false},
		{untyped, "", true},
	} {
		if got := MatchType(test.object, test.t); got != test.want {
			t.Errorf("%q of %+v: got %v, want %v", test.t, test.object.ObjectMeta, got, test.want)
		}
	}
}

func Test_VolumeReadonly(t *testing.T) {
	// It's of the URL's readonly option, which is true when it's empty, or
	// there's none
	for u, want := range map[string]bool{
		"file://media/mnt/media?readonly=false": false,
		"file://media/mnt/media?readonly=true":  true,
		"file://media/mnt/media?readonly=0":     false,
		"file://media/mnt/media?readonly":       true,
		"file://media/mnt/media?readonly=":      true,
		"file://media/mnt/media":                true,
	} {
		readonly, err := Volume{VolumeMeta: VolumeMeta{Name: "media", URL: u}}.Readonly()
		if err != nil {
			t.Errorf("%s: %v", u, err)
		} else if readonly != want {
			t.Errorf("%s: readonly is %v, not %v", u, readonly, want)
		}
	}

	// An option, or a URL, which can't be parsed is an error, and read-only
	for _, u := range []string{"file://media/mnt/media?readonly=maybe", "file://media/mnt/media?readonly=%zz", "://"} {
		readonly, err := Volume{VolumeMeta: VolumeMeta{Name: "media", URL: u}}.Readonly()
		if err == nil || !readonly {
			t.Errorf("%s: readonly is %v, with error %v", u, readonly, err)
		}
	}
}

func Test_VolumeCells(t *testing.T) {
	// The bare URL is without its options, which are listed, without the
	// description, which is its own
	v := Volume{VolumeMeta: VolumeMeta{Name: "media", URL: "file://media/mnt/media?skip=%5E%5C.&description=Family+photos&readonly=true&skip=~%24", Enabled: true}, Mounted: true}
	if got := v.BareURL(); got != "file://media/mnt/media" {
		t.Errorf("bare url: %q", got)
	}
	if got := v.Options(); got != `readonly=true skip=^\. skip=~$` {
		t.Errorf("options: %q", got)
	}
	if got := v.Description(); got != "Family photos" {
		t.Errorf("description: %q", got)
	}

	// The status is of whether it's enabled, mounted, and offline, and
	// whether it's indexed is apart from it
	indexed := func(state IndexState) Volume {
		return Volume{VolumeMeta: VolumeMeta{Name: "media", Enabled: true}, Mounted: true, Index: &VolumeIndex{Mode: "folders", IndexState: state}}
	}
	for want, volume := range map[string]Volume{
		"mounted":                v,
		"disabled":               {VolumeMeta: VolumeMeta{Name: "media"}},
		"mounting":               {VolumeMeta: VolumeMeta{Name: "media", Enabled: true}},
		"offline: sign in again": {VolumeMeta: VolumeMeta{Name: "media"}, Mounted: true, Error: "sign in again"},
	} {
		if got := volume.Status(); got != want {
			t.Errorf("status: got %q, want %q", got, want)
		}
	}
	for want, volume := range map[string]Volume{
		"none":                               v,
		"indexing, 3 folders":                indexed(IndexState{Status: IndexStatusScanning, Folders: 3}),
		"idle, 12 folders":                   indexed(IndexState{Status: IndexStatusReady, Folders: 12, Started: time.Unix(100, 0), Scanned: time.Unix(200, 0)}),
		"indexing, 12 folders":               indexed(IndexState{Status: IndexStatusReady, Folders: 12, Scanned: time.Unix(100, 0), Started: time.Unix(200, 0)}),
		"failed: it isn't implemented":       indexed(IndexState{Error: "it isn't implemented"}),
		"idle, 12 folders: the watch failed": indexed(IndexState{Status: IndexStatusReady, Folders: 12, Scanned: time.Unix(100, 0), Started: time.Unix(200, 0), Error: "the watch failed"}),
		"idle, 4 folders, 9 files":           {VolumeMeta: VolumeMeta{Name: "media", Enabled: true}, Mounted: true, Index: &VolumeIndex{Mode: "objects", IndexState: IndexState{Status: IndexStatusReady, Folders: 4, Files: 9}}},
		"idle, 4 folders, 0 files":           {VolumeMeta: VolumeMeta{Name: "media", Enabled: true}, Mounted: true, Index: &VolumeIndex{Mode: "objects", IndexState: IndexState{Status: IndexStatusReady, Folders: 4}}},
		"indexing, 4 folders, 9 files":       {VolumeMeta: VolumeMeta{Name: "media", Enabled: true}, Mounted: true, Index: &VolumeIndex{Mode: "objects", IndexState: IndexState{Status: IndexStatusReady, Folders: 4, Files: 9, Started: time.Unix(200, 0), Scanned: time.Unix(100, 0)}}},
	} {
		if got := volume.IndexStatus(); got != want {
			t.Errorf("index: got %q, want %q", got, want)
		}
		if got := volume.Status(); volume.Index != nil && got != "mounted" {
			t.Errorf("status of an indexed volume: got %q", got)
		}
	}
}
