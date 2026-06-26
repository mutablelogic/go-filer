package mime

import (
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"

	// Packages

	"github.com/mutablelogic/go-filer/filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// wellKnownMIME maps file extensions that Go's mime package may not know about
// to their canonical MIME type.
var wellKnownMIME = map[string]string{
	".go":      "text/x-go",
	".mod":     types.ContentTypeTextPlain,
	".sum":     types.ContentTypeTextPlain,
	".md":      "text/markdown",
	".sh":      "text/x-shellscript",
	".py":      "text/x-python",
	".rb":      "text/x-ruby",
	".rs":      "text/x-rust",
	".java":    "text/x-java-source",
	".m":       "text/x-objective-c",
	".ts":      "text/typescript",
	".tsx":     "text/typescript",
	".js":      "text/javascript",
	".jsx":     "text/javascript",
	".yaml":    "application/yaml",
	".yml":     "application/yaml",
	".toml":    "application/toml",
	".proto":   "application/protobuf",
	".m4a":     "audio/mp4",
	".flac":    "audio/flac",
	".xmp":     "application/xmp+xml",
	".mov":     "video/quicktime",
	".geojson": "application/geo+json",
	".tf":      "application/x-terraform",
	".heic":    "image/heic",
	".ics":     "text/calendar",
	".ical":    "text/calendar",
}

type namedReader interface {
	io.Reader
	Name() string
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Type returns the MIME type of the content read from the provided reader. It first
// checks if the reader provides Name() and uses both file extension and content sniffing,
// falling back to content sniffing only when no name is available.
//
// This function consumes up to 512 bytes from r while sniffing content type. If
// callers need to read the full stream afterward, they should buffer and replay
// those bytes (or use a seekable reader and rewind).
// Returns application/octet-stream if the MIME type cannot be determined.
func Type(r io.Reader) (string, []schema.Meta, error) {
	if r == nil {
		return types.ContentTypeBinary, nil, io.ErrUnexpectedEOF
	}
	if r, ok := r.(namedReader); ok {
		if ct, meta, err := byReader(r.Name(), r); err == nil {
			return ct, meta, nil
		}
	} else if ct, meta, err := byReader("", r); err == nil {
		return ct, meta, nil
	}
	return types.ContentTypeBinary, nil, nil
}

func TypeByExtension(ext string) string {
	ext = strings.ToLower(ext)
	if ct, ok := wellKnownMIME[ext]; ok {
		return ct
	}
	if t := mime.TypeByExtension(ext); t != "" {
		if mediatype, _, err := mime.ParseMediaType(t); err == nil {
			return mediatype
		}
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
		// Strip any parameters (e.g. "; charset=utf-8") the OS MIME database may include
		if mediatype, _, err := mime.ParseMediaType(t); err == nil {
			return mediatype
		}
		return t
	}
	return types.ContentTypeBinary
}

func byReader(name string, r io.Reader) (string, []schema.Meta, error) {
	if r == nil {
		return "", nil, io.ErrUnexpectedEOF
	}

	// Get by extension first to see if we can get a more specific type than what http.DetectContentType will return
	t := byExt(filepath.Ext(name))

	// Read the first 512 bytes to sniff the content type
	var buf [512]byte
	n, err := io.ReadFull(r, buf[:])
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return "", nil, err
	}
	if n == 0 {
		return t, nil, nil
	}
	if sniffed := http.DetectContentType(buf[:n]); sniffed != types.ContentTypeBinary {
		mediatype, params, err := mime.ParseMediaType(sniffed)
		if err != nil {
			return sniffed, nil, nil
		}

		// Collect any params (e.g. charset) as metadata
		var meta []schema.Meta
		for k, v := range params {
			meta = append(meta, schema.Meta{Key: k, Value: []byte(strconv.Quote(v))})
		}

		// If text/plain, prefer the extension-derived type if it's more specific
		if mediatype == types.ContentTypeTextPlain && t != types.ContentTypeBinary {
			return t, meta, nil
		}
		// If video/mp4 but the extension says audio, trust the extension
		if mediatype == "video/mp4" && strings.HasPrefix(t, "audio/") {
			return t, nil, nil
		}

		return mediatype, meta, nil
	}

	// If we couldn't sniff a more specific type, return the one from the extension (which may be empty)
	return t, nil, nil
}
