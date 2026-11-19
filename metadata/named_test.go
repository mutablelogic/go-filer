package metadata

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func Test_withName(t *testing.T) {
	// A reader which can seek, and be read at an offset, as a file can, can
	// with its name, so go-metadata reads it in place
	r := withName(bytes.NewReader([]byte("abc")), "a.txt")
	if r.Name() != "a.txt" {
		t.Errorf("name: %q", r.Name())
	}
	if _, ok := r.(io.ReaderAt); !ok {
		t.Error("it can't be read at an offset")
	}
	if _, ok := r.(io.Seeker); !ok {
		t.Error("it can't seek")
	}

	// And one which can't, can't
	r = withName(io.MultiReader(strings.NewReader("abc")), "c.txt")
	if _, ok := r.(io.ReaderAt); ok {
		t.Error("a reader which can't be read at an offset can")
	}
}
