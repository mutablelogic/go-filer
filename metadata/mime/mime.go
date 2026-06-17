package mime

import (
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	// Packages
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// wellKnownMIME maps file extensions that Go's mime package may not know about
// to their canonical MIME type.
var wellKnownMIME = map[string]string{
	".go":    "text/x-go",
	".mod":   types.ContentTypeTextPlain,
	".sum":   types.ContentTypeTextPlain,
	".md":    "text/markdown",
	".sh":    "text/x-shellscript",
	".py":    "text/x-python",
	".rb":    "text/x-ruby",
	".rs":    "text/x-rust",
	".java":  "text/x-java-source",
	".m":     "text/x-objective-c",
	".ts":    "text/typescript",
	".tsx":   "text/typescript",
	".js":    "text/javascript",
	".jsx":   "text/javascript",
	".yaml":  "application/yaml",
	".yml":   "application/yaml",
	".toml":  "application/toml",
	".proto": "application/protobuf",
	".m4a":   "audio/mp4",
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Type returns the MIME type of the content read from the provided reader. It first
// checks if the reader is an *os.File and attempts to determine the MIME type based
// on the file name and content, falling back to content sniffing if necessary.
// Returns application/octet-stream if the MIME type cannot be determined.
func Type(r io.Reader) string {
	if r, ok := r.(*os.File); ok {
		if ct, err := byReader(r.Name(), r); err == nil {
			return ct
		}
	} else if ct, err := byReader("", r); err == nil {
		return ct
	}
	return types.ContentTypeBinary
}

func TypeByExtension(ext string) string {
	ext = strings.ToLower(ext)
	if ct, ok := wellKnownMIME[ext]; ok {
		return ct
	}
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return types.ContentTypeBinary
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// MIMEByExt returns the MIME type for a file extension, consulting wellKnownMIME
// first and then the system MIME database.
func byExt(ext string) string {
	if ct, ok := wellKnownMIME[ext]; ok {
		return ct
	}
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return types.ContentTypeBinary
}

func byReader(name string, r io.Reader) (string, error) {
	// Get by extension first to see if we can get a more specific type than what http.DetectContentType will return
	t := byExt(filepath.Ext(name))

	// Read the first 512 bytes to sniff the content type
	var buf [512]byte
	if n, err := io.ReadFull(r, buf[:]); err != nil && err != io.ErrUnexpectedEOF {
		return "", err
	} else if sniffed := http.DetectContentType(buf[:n]); sniffed != types.ContentTypeBinary {
		// If text/plain then check by extension to see if we can get a more specific type
		if mediatype, _, err := mime.ParseMediaType(sniffed); err == nil && mediatype == types.ContentTypeTextPlain && t != types.ContentTypeBinary {
			return t, nil
		} else if mediatype == "video/mp4" && strings.HasPrefix(t, "audio/") {
			return t, nil
		}
		// If video/mp4
		return sniffed, nil
	}

	// If we couldn't sniff a more specific type, return the one from the extension (which may be empty)
	return t, nil
}
