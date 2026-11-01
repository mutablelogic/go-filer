package fs

import (
	"mime"
	"net/url"
	"testing"
)

func Test_package(t *testing.T) {
	o, err := parseOpts(nil)
	if err != nil {
		t.Fatal(err)
	}
	fsys := &FS{opt: o}

	// Directories with the default names are packages, in any case
	for name, want := range map[string]bool{
		"Disk.sparsebundle": true,
		"Plugin.BUNDLE":     true,
		"bundle":            false,
		"notes.bundle.txt":  false,
		"Photos.library":    false,
	} {
		if got := fsys.isPackage(name); got != want {
			t.Errorf("isPackage(%q) = %v, want %v", name, got, want)
		}
	}

	// A path is in a package when a directory it's in is one, not when it is one
	for name, want := range map[string]string{
		"a/Disk.sparsebundle/bands/404d": "a/Disk.sparsebundle",
		"Disk.sparsebundle/Info.plist":   "Disk.sparsebundle",
		"a/Disk.sparsebundle":            "",
		"a/b/c":                          "",
	} {
		if got, ok := fsys.packageOf(name); got != want || ok != (want != "") {
			t.Errorf("packageOf(%q) = %q, %v, want %q", name, got, ok, want)
		}
	}

	// A package's type is its extension's
	if got := mime.TypeByExtension(".sparsebundle"); got != "application/x-apple-sparsebundle" {
		t.Errorf("type = %q", got)
	}
}

func Test_packageOpt(t *testing.T) {
	// The option replaces the defaults
	o, err := parseOpts(url.Values{"package": {`(?i)\.photoslibrary$`}})
	if err != nil {
		t.Fatal(err)
	}
	fsys := &FS{opt: o}
	if !fsys.isPackage("Photos.photoslibrary") || fsys.isPackage("Disk.sparsebundle") {
		t.Error("custom package")
	}
	if v := o.values().Encode(); v != "package=%28%3Fi%29%5C.photoslibrary%24" {
		t.Error(v)
	}

	// An empty expression has no packages
	o, _ = parseOpts(url.Values{"package": {""}})
	if (&FS{opt: o}).isPackage("Disk.sparsebundle") {
		t.Error("empty package")
	}
	if _, err := parseOpts(url.Values{"package": {"("}}); err == nil {
		t.Error("bad expression")
	}
}
