package httpclient

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/textproto"
	"path"
	"strconv"
	"time"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-filer/pkg/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// wellKnownMIME maps file extensions that Go's mime package may not know about
// (especially on macOS) to their canonical MIME type.
var wellKnownMIME = map[string]string{
	".go":    "text/x-go",
	".mod":   "text/plain",
	".sum":   "text/plain",
	".md":    "text/markdown",
	".sh":    "text/x-shellscript",
	".py":    "text/x-python",
	".rb":    "text/x-ruby",
	".rs":    "text/x-rust",
	".ts":    "text/typescript",
	".tsx":   "text/typescript",
	".jsx":   "text/javascript",
	".yaml":  "application/yaml",
	".yml":   "application/yaml",
	".toml":  "application/toml",
	".proto": "text/plain",
}

// MIMEByExt returns the MIME type for a file extension, consulting wellKnownMIME
// first and then the system MIME database.
func MIMEByExt(ext string) string {
	if ct, ok := wellKnownMIME[ext]; ok {
		return ct
	}
	return mime.TypeByExtension(ext)
}

///////////////////////////////////////////////////////////////////////////////
// TYPES

// UploadOpt is a functional option for CreateObjects.
type UploadOpt func(*uploadOpts) error

type uploadOpts struct {
	prefix   string
	filter   func(fs.DirEntry) bool
	check    func(fs.FileInfo, *schema.Object) bool
	progress func(index, count int, path string, written, bytes int64)
}

// walkEntry holds the path of a discovered file (relative to the fs.FS root)
// and its fs.FileInfo captured during the walk.
type walkEntry struct {
	path string
	info fs.FileInfo
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithPrefix sets the remote destination prefix under which all uploaded files
// are stored. For example, WithPrefix("photos/2026") uploads a.txt as
// /photos/2026/a.txt. The default is the backend root.
func WithPrefix(prefix string) UploadOpt {
	return func(o *uploadOpts) error {
		o.prefix = prefix
		return nil
	}
}

// WithFilter sets a function that controls which entries are walked. Return
// false to skip the entry (and its subtree when it is a directory).
func WithFilter(fn func(fs.DirEntry) bool) UploadOpt {
	return func(o *uploadOpts) error {
		o.filter = fn
		return nil
	}
}

// WithCheck overrides the default skip check. The function receives the local
// fs.FileInfo and the remote schema.Object (nil when the object does not exist);
// return true to skip the upload for that file. Pass nil to disable skipping
// entirely.
func WithCheck(fn func(fs.FileInfo, *schema.Object) bool) UploadOpt {
	return func(o *uploadOpts) error {
		o.check = fn
		return nil
	}
}

// WithProgress sets a callback that is invoked for byte-progress
// updates and once per committed file. index is the 0-based file position;
// count is the total number of files in this upload batch. written and
// bytes are the per-file byte counters.
func WithProgress(fn func(index, count int, path string, written, bytes int64)) UploadOpt {
	return func(o *uploadOpts) error {
		o.progress = fn
		return nil
	}
}

// SkipUnchanged is the default check function used by CreateObjects. It skips
// a file when the remote object already exists with the same size. When both
// the local ModTime and the remote ModTime are non-zero, they must also match
// (compared at second precision, since HTTP-date has no sub-second resolution).
func SkipUnchanged(localInfo fs.FileInfo, remote *schema.Object) bool {
	if remote == nil {
		return false // object does not exist remotely — upload it
	}
	if localInfo.Size() != remote.Size {
		return false
	}
	lmt := localInfo.ModTime().Truncate(time.Second)
	rmt := remote.ModTime.Truncate(time.Second)
	if !lmt.IsZero() && !rmt.IsZero() {
		return lmt.Equal(rmt)
	}
	// One or both modtimes unknown — size match is sufficient.
	return true
}

// CreateObjects walks fsys and uploads every matching file to the named backend
// as a single streaming multipart POST. To upload a subtree, pass fs.Sub(fsys,
// "subdir") as fsys. Options control the remote prefix, entry filter,
// pre-upload skip check, and progress callback; all are optional.
// By default, files that already exist remotely with the same size (and modtime
// when available) are skipped. Use WithCheck(nil) to disable this behaviour.
func (c *Client) CreateObjects(ctx context.Context, name string, fsys fs.FS, opts ...UploadOpt) ([]schema.Object, error) {
	o := &uploadOpts{check: SkipUnchanged}
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, err
		}
	}

	entries, err := walkFS(fsys, o.filter)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}

	// Pre-filter: HEAD all entries in parallel and ask the check function
	// whether each upload should be skipped.
	if o.check != nil {
		reqs := make([]schema.GetObjectRequest, len(entries))
		for i, e := range entries {
			reqs[i] = schema.GetObjectRequest{Path: path.Join("/", o.prefix, e.path)}
		}
		remotes, err := c.GetObjects(ctx, name, reqs)
		if err != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		filtered := entries[:0]
		for i, e := range entries {
			if !o.check(e.info, remotes[i]) {
				filtered = append(filtered, e)
			}
		}
		entries = filtered
	}
	if len(entries) == 0 {
		return nil, nil
	}

	// Open every file. Keep track of all opened handles so we can close them
	// after the HTTP round-trip completes (the streaming encoder reads bodies
	// lazily as the HTTP client sends data, so files must stay open until
	// DoWithContext returns).
	parts := make([]types.File, 0, len(entries))
	var totalBytes int64
	for i, e := range entries {
		f, err := fsys.Open(e.path)
		if err != nil {
			for _, p := range parts {
				p.Body.Close()
			}
			return nil, err
		}
		remotePath := path.Join("/", o.prefix, e.path)
		// Determine content type: prefer extension-based lookup; fall back to
		// sniffing the first 512 bytes when the extension yields nothing useful.
		var body io.ReadCloser = f
		ct := MIMEByExt(path.Ext(e.path))
		if ct == "" || ct == "application/octet-stream" {
			var buf [512]byte
			n, _ := io.ReadFull(f, buf[:])
			if sniffed := http.DetectContentType(buf[:n]); sniffed != "application/octet-stream" {
				ct = sniffed
			}
			// Stitch the peeked bytes back onto the front of the reader,
			// keeping the original file as the Closer so it is not leaked.
			body = struct {
				io.Reader
				io.Closer
			}{io.MultiReader(bytes.NewReader(buf[:n]), f), f}
		}
		// Stamp Last-Modified so the server stores the original mod time and
		// future skip checks (SkipUnchanged) can compare it.
		// Stamp Content-Length so the server can populate UploadFile.Bytes and
		// the CLI can display upload progress as a percentage.
		h := textproto.MIMEHeader{}
		if mt := e.info.ModTime(); !mt.IsZero() {
			h.Set("Last-Modified", mt.UTC().Format(http.TimeFormat))
		}
		if sz := e.info.Size(); sz > 0 {
			h.Set(types.ContentLengthHeader, strconv.FormatInt(sz, 10))
		}
		if o.progress != nil {
			total := e.info.Size()
			body = newUploadProgressReadCloser(body, total, func(written, bytes int64) {
				o.progress(i, len(entries), remotePath, written, bytes)
			})
		}
		totalBytes += e.info.Size()
		parts = append(parts, types.File{
			Path:        remotePath,
			Body:        body,
			ContentType: ct,
			Header:      h,
		})
	}
	defer func() {
		for _, p := range parts {
			p.Body.Close()
		}
	}()

	// Build a streaming multipart payload. The encoder reflect-walks the
	// struct and writes each types.File as a separate multipart "file" part.
	upload := struct {
		Files []types.File `json:"file"`
	}{Files: parts}
	payload, err := client.NewStreamingMultipartRequest(&upload, types.ContentTypeJSON)
	if err != nil {
		return nil, err
	}

	results := make([]schema.Object, 0, len(parts))
	_ = totalBytes
	if err := c.DoWithContext(ctx, payload, &results,
		client.OptPath(name),
		client.OptReqHeader("X-Upload-Count", strconv.Itoa(len(parts))),
		client.OptNoTimeout(),
	); err != nil {
		return nil, err
	}
	if o.progress != nil {
		for index, obj := range results {
			o.progress(index, len(results), obj.Path, obj.Size, obj.Size)
		}
	}
	return results, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE HELPERS

// walkFS walks the filesystem from its root (".") and returns one walkEntry
// per regular (non-directory) file. filter is called for every entry; return
// false to skip it (and its subtree when it is a directory). A nil filter
// includes everything.
func walkFS(fsys fs.FS, filter func(fs.DirEntry) bool) ([]walkEntry, error) {
	var entries []walkEntry
	err := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filter != nil && !filter(d) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		entries = append(entries, walkEntry{path: p, info: info})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

type uploadProgressReadCloser struct {
	r        io.ReadCloser
	total    int64
	written  int64
	lastEmit int64
	cb       func(written, total int64)
}

func newUploadProgressReadCloser(r io.ReadCloser, total int64, cb func(written, total int64)) io.ReadCloser {
	return &uploadProgressReadCloser{
		r:     r,
		total: total,
		cb:    cb,
	}
}

func (r *uploadProgressReadCloser) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.written += int64(n)
		if r.written-r.lastEmit >= 64*1024 || (r.total > 0 && r.written >= r.total) {
			r.lastEmit = r.written
			r.cb(r.written, r.total)
		}
	}
	return n, err
}

func (r *uploadProgressReadCloser) Close() error {
	return r.r.Close()
}
