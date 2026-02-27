package httphandler

import (
	"testing"

	// Packages
	types "github.com/mutablelogic/go-server/pkg/types"
)

func Test_resolveContentType_table(t *testing.T) {
	tests := []struct {
		name, stored, sniffed, ext, want string
	}{
		// 1. Explicit stored type wins over everything.
		{"stored explicit", "text/plain", "", "", "text/plain"},
		{"stored explicit over sniffed", "text/plain", "application/pdf", "", "text/plain"},
		// 2. Sniffed type used when stored is absent or binary fallback.
		{"sniffed used when stored empty", "", "text/html", "", "text/html"},
		{"stored binary fallback: sniff wins", types.ContentTypeBinary, "text/html", "", "text/html"},
		// 3. Extension used when stored and sniffed are both the binary fallback or absent.
		{"ext used when stored+sniffed absent", "", "", ".json", "application/json"},
		{"ext used when sniffed binary", types.ContentTypeBinary, types.ContentTypeBinary, ".txt", "text/plain; charset=utf-8"},
		// 4. stored=octet-stream propagated when there is no better option.
		{"stored octet-stream propagated", types.ContentTypeBinary, types.ContentTypeBinary, "", types.ContentTypeBinary},
		// 5. Final hardcoded fallback when stored is empty.
		{"empty stored fallback", "", "", "", types.ContentTypeBinary},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveContentType(tc.stored, tc.sniffed, tc.ext)
			if got != tc.want {
				t.Errorf("resolveContentType(%q,%q,%q) = %q, want %q",
					tc.stored, tc.sniffed, tc.ext, got, tc.want)
			}
		})
	}
}

func Test_matchETags_table(t *testing.T) {
	tests := []struct {
		name, header, etag string
		strong, want       bool
	}{
		{"wildcard non-empty", "*", `"a"`, true, true},
		{"wildcard weak under weak compare", "*", `W/"a"`, false, true},
		{"wildcard weak under strong compare", "*", `W/"a"`, true, true},
		{"wildcard empty", "*", "", true, false},
		{"weak stored fails strong IfMatch", `"a"`, `W/"a"`, true, false},
		{"weak header skipped strong compare", `W/"a"`, `"a"`, true, false},
		{"strong matches strong", `"a"`, `"a"`, true, true},
		{"different values strong", `"x"`, `"a"`, true, false},
		{"weak stored matches weak header", `W/"a"`, `W/"a"`, false, true},
		{"weak stored matches strong header", `"a"`, `W/"a"`, false, true},
		{"strong stored matches weak header", `W/"a"`, `"a"`, false, true},
		{"different values weak", `"x"`, `W/"a"`, false, false},
		{"list second item strong", `"x", "a"`, `"a"`, true, true},
		{"list skips weak strong", `W/"a", "a"`, `"a"`, true, true},
		{"list all weak no match strong", `W/"a", W/"b"`, `"a"`, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchETags(tc.header, tc.etag, tc.strong); got != tc.want {
				t.Errorf("matchETags(%q,%q,strong=%v)=%v want %v",
					tc.header, tc.etag, tc.strong, got, tc.want)
			}
		})
	}
}
