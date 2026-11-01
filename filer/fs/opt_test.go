package fs

import (
	"context"
	"errors"
	"io/fs"
	"net/url"
	"testing"
	"testing/fstest"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/filer/schema"
)

func Test_opt(t *testing.T) {
	if _, err := parseOpts(url.Values{"other": {"x"}}); err == nil {
		t.Error("unknown option")
	}
	if _, err := parseOpts(url.Values{"skip": {"("}}); err == nil {
		t.Error("bad expression")
	}
	o, err := parseOpts(url.Values{"skip": {""}})
	if err != nil || len(o.skip) != 0 || (&FS{opt: o}).skip(".git") {
		t.Error("empty skip", err)
	}
	if v := o.values().Encode(); v != "skip=" {
		t.Error(v)
	}
	o, _ = parseOpts(url.Values{"skip": {`\.tmp$`}})
	if !(&FS{opt: o}).skip("a.tmp") || (&FS{opt: o}).skip(".git") {
		t.Error("custom skip")
	}
	o, _ = parseOpts(nil)
	if v := o.values().Encode(); v != "" {
		t.Error(v)
	}
}

func Test_optDefault(t *testing.T) {
	// The default expressions, when they're set, are the defaults, so they
	// aren't in the values
	o, err := parseOpts(url.Values{"skip": defaultSkip})
	if err != nil {
		t.Fatal(err)
	}
	if v := o.values().Encode(); v != "" {
		t.Error(v)
	}
	o, _ = parseOpts(url.Values{"skip": {`\.tmp$`, `^#`}})
	if v := o.values()["skip"]; len(v) != 2 || v[0] != `\.tmp$` || v[1] != `^#` {
		t.Error(v)
	}
}

func Test_optReadonly(t *testing.T) {
	// The option is true, false or empty, which is true
	for value, want := range map[string]bool{"": true, "true": true, "1": true, "false": false, "0": false} {
		if o, err := parseOpts(url.Values{"readonly": {value}}); err != nil || o.readonly != want {
			t.Errorf("readonly=%q: %v, %v", value, o.readonly, err)
		}
	}
	if _, err := parseOpts(url.Values{"readonly": {"maybe"}}); err == nil {
		t.Error("bad value")
	}
	if o, _ := parseOpts(nil); o.readonly {
		t.Error("default")
	}

	// A writable filesystem is read-only with the option, and its URL says so
	ctx := context.Background()
	for value, readonly := range map[string]bool{"true": true, "false": false} {
		self, err := NewFS("test", &memFS{MapFS: fstest.MapFS{"a.txt": {Data: []byte("a")}}}, url.Values{"readonly": {value}})
		if err != nil {
			t.Fatal(err)
		}
		_, createErr := self.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/b.txt"}})
		_, deleteErr := self.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Path: "/a.txt"}})
		if got := errors.Is(createErr, filer.ErrForbidden) && errors.Is(deleteErr, filer.ErrForbidden); got != readonly {
			t.Errorf("readonly=%s: create %v, delete %v", value, createErr, deleteErr)
		} else if got := self.URL().Query().Get("readonly") == "true"; got != readonly {
			t.Errorf("readonly=%s: url %v", value, self.URL())
		}

		// It can still be read
		if _, err := self.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/b.txt"}}); readonly && !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("readonly=%s: get %v", value, err)
		}
		if _, err := self.GetObject(ctx, schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Path: "/a.txt"}}); readonly && err != nil {
			t.Errorf("readonly=%s: get %v", value, err)
		}
	}

	// A filesystem which isn't writable is read-only, whatever the option is
	self, err := NewFS("test", fstest.MapFS{}, url.Values{"readonly": {"false"}})
	if err != nil {
		t.Fatal(err)
	} else if _, err := self.CreateObject(ctx, schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Path: "/b.txt"}}); !errors.Is(err, filer.ErrForbidden) {
		t.Errorf("not writable: %v", err)
	} else if self.URL().Query().Get("readonly") != "true" {
		t.Errorf("not writable: url %v", self.URL())
	}

	// The URL creates the same filesystem again
	again, err := NewFS("test", &memFS{MapFS: fstest.MapFS{}}, self.URL().Query())
	if err != nil {
		t.Fatal(err)
	} else if !again.readonly {
		t.Error("url: not read-only")
	}
}
