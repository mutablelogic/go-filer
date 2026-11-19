package fs

import "testing"

func Test_skip(t *testing.T) {
	o, err := parseOpts(nil)
	if err != nil {
		t.Fatal(err)
	}
	fsys := &FS{opt: o}
	for name, want := range map[string]bool{
		// Hidden files and directories
		".DS_Store": true,
		".git":      true,

		// Files which operating systems and applications leave, in any case
		"~$tline Online Coaching Skills Workshop 1.docx": true,
		"~$book.xlsx":  true,
		"Thumbs.db":    true,
		"thumbs.DB":    true,
		"desktop.ini":  true,
		"Desktop.ini":  true,
		"$RECYCLE.BIN": true,
		"notes.txt~":   true,
		"Makefile~":    true,
		"~":            true,
		"Icon\r":       true,

		// Content
		"Icon":             false,
		"icon.png":         false,
		"report.docx":      false,
		"~notes.txt":       false,
		"a~$b.docx":        false,
		"Thumbs.db.backup": false,
		"$money.xlsx":      false,
		"photo.JPG":        false,
	} {
		if got := fsys.skip(name); got != want {
			t.Errorf("skip(%q) = %v, want %v", name, got, want)
		}
	}
}
