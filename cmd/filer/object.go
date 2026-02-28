package main

import (
	"fmt"
	"io"
	"io/fs"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	// Packages
	httpclient "github.com/mutablelogic/go-filer/pkg/httpclient"
	schema "github.com/mutablelogic/go-filer/pkg/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ListCommand struct {
	Backend   string `arg:"" name:"backend" help:"Backend name"`
	Path      string `arg:"" name:"path" help:"Path prefix to list" optional:"" default:"/"`
	Recursive bool   `name:"recursive" short:"r" help:"List recursively"`
	Limit     int    `name:"limit" short:"n" help:"Maximum number of objects to return (default: all)."`
	Offset    int    `name:"offset" help:"Number of objects to skip (for pagination)." default:"0"`
}

type GetCommand struct {
	Backend string `arg:"" name:"backend" help:"Backend name"`
	Path    string `arg:"" name:"path" help:"Object path (e.g. /dir/file.txt)"`
	Output  string `name:"output" short:"o" help:"Write to file instead of stdout"`
}

type HeadCommand struct {
	Backend string `arg:"" name:"backend" help:"Backend name"`
	Path    string `arg:"" name:"path" help:"Object path"`
}

type DeleteCommand struct {
	Backend   string `arg:"" name:"backend" help:"Backend name"`
	Path      string `arg:"" name:"path" help:"Object path or prefix"`
	Recursive bool   `name:"recursive" short:"r" help:"Delete all objects under path"`
}

type UploadCommand struct {
	Backend string `arg:"" name:"backend" help:"Backend name"`
	Path    string `arg:"" name:"path" help:"Local file or directory to upload (defaults to current directory)." optional:""`
	Prefix  string `name:"prefix" short:"p" help:"Remote path prefix (e.g. backups/2026)."`
	Hidden  bool   `name:"hidden" help:"Include files and directories whose names begin with '.'."`
	Force   bool   `name:"force" short:"f" help:"Upload every file even when the remote copy appears up to date."`
}

type DownloadCommand struct {
	Backend string `arg:"" name:"backend" help:"Backend name"`
	Path    string `arg:"" name:"path" help:"Local directory to download into (defaults to current directory)." optional:""`
	Prefix  string `name:"prefix" short:"p" help:"Remote path prefix to download (e.g. backups/2026)."`
	Force   bool   `name:"force" short:"f" help:"Download every file even when the local copy appears up to date."`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *ListCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}
	limit := cmd.Limit
	if limit == 0 {
		limit = schema.MaxListLimit
	}
	path := cmd.Path
	if path == "" {
		path = "/"
	}
	resp, err := c.ListObjects(ctx.ctx, cmd.Backend, schema.ListObjectsRequest{
		Path:      path,
		Recursive: cmd.Recursive,
		Limit:     limit,
		Offset:    cmd.Offset,
	})
	if err != nil {
		return err
	}
	if ctx.Debug {
		return prettyJSON(resp)
	}
	return printListing(resp)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// printListing renders a schema.ListObjectsResponse in an ls-style table.
func printListing(resp *schema.ListObjectsResponse) error {
	bold := isTerminal(os.Stdout)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, obj := range resp.Body {
		name := strings.TrimPrefix(obj.Path, "/")
		if bold {
			name = "\x1b[1m" + name + "\x1b[0m"
		}
		fmt.Fprintf(w, "%8s\t%s\t%-30s\t%s\n",
			humanSize(obj.Size),
			formatModTime(obj.ModTime),
			shortContentType(obj.ContentType, obj.Path),
			name,
		)
	}
	w.Flush()
	if len(resp.Body) < resp.Count {
		fmt.Fprintf(os.Stdout, "\n  %d of %d object(s)\n", len(resp.Body), resp.Count)
	} else {
		fmt.Fprintf(os.Stdout, "\n  %d object(s)\n", resp.Count)
	}
	return nil
}

// printObjects renders a slice of objects in the same ls-style table used by
// printListing. It is used by DeleteCommand to display the deleted objects.
func printObjects(objs []schema.Object) error {
	bold := isTerminal(os.Stdout)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, obj := range objs {
		name := strings.TrimPrefix(obj.Path, "/")
		if bold {
			name = "\x1b[1m" + name + "\x1b[0m"
		}
		fmt.Fprintf(w, "%8s\t%s\t%-30s\t%s\n",
			humanSize(obj.Size),
			formatModTime(obj.ModTime),
			shortContentType(obj.ContentType, obj.Path),
			name,
		)
	}
	w.Flush()
	fmt.Fprintf(os.Stdout, "\n  %d object(s) deleted\n", len(objs))
	return nil
}

// wellKnownMIME maps file extensions that Go's mime package may not know about
// (especially on macOS) to their canonical MIME type.
var wellKnownMIME = map[string]string{
	".go":       "text/x-go",
	".mod":      "text/plain",
	".sum":      "text/plain",
	".md":       "text/markdown",
	".sh":       "text/x-shellscript",
	".py":       "text/x-python",
	".rb":       "text/x-ruby",
	".rs":       "text/x-rust",
	".ts":       "text/typescript",
	".tsx":      "text/typescript",
	".jsx":      "text/javascript",
	".yaml":     "application/yaml",
	".yml":      "application/yaml",
	".toml":     "application/toml",
	".proto":    "text/plain",
	".Makefile": "text/x-makefile",
}

// mimeByExt returns the MIME type for a file extension, consulting wellKnownMIME
// first and then the system MIME database.
func mimeByExt(ext string) string {
	if ct, ok := wellKnownMIME[ext]; ok {
		return ct
	}
	// Special case: Makefile has no extension
	if ext == "" {
		return ""
	}
	return mime.TypeByExtension(ext)
}

// shortContentType strips parameters from a MIME type. When ct is empty or
// generic (application/octet-stream) it falls back to inferring the type from
// the file extension of path. Returns "-" if neither source yields a useful type.
func shortContentType(ct, path string) string {
	if ct == "" || ct == "application/octet-stream" {
		if inferred := mimeByExt(filepath.Ext(path)); inferred != "" {
			ct = inferred
		}
	}
	if ct == "" {
		return "-"
	}
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	return ct
}

// humanSize formats a byte count as a human-readable string.
func humanSize(n int64) string {
	const (
		KB = int64(1024)
		MB = 1024 * KB
		GB = 1024 * MB
		TB = 1024 * GB
	)
	switch {
	case n >= 1000*GB:
		return fmt.Sprintf("%.1fT", float64(n)/float64(TB))
	case n >= 1000*MB:
		return fmt.Sprintf("%.1fG", float64(n)/float64(GB))
	case n >= 1000*KB:
		return fmt.Sprintf("%.1fM", float64(n)/float64(MB))
	case n >= KB:
		return fmt.Sprintf("%.1fK", float64(n)/float64(KB))
	default:
		return fmt.Sprintf("%dB", n)
	}
}

// formatModTime formats a time in ls-style: "Jan  2 15:04" for the current
// year, or "Jan  2  2006" for older entries. Zero times are rendered as blanks.
func formatModTime(t time.Time) string {
	if t.IsZero() {
		return "            "
	}
	if t.Year() == time.Now().Year() {
		return t.Format("Jan _2 15:04")
	}
	return t.Format("Jan _2  2006")
}

func (cmd *HeadCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}
	obj, err := c.GetObject(ctx.ctx, cmd.Backend, schema.GetObjectRequest{
		Path: cmd.Path,
	})
	if err != nil {
		return err
	}
	return prettyJSON(obj)
}

func (cmd *GetCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}
	var out io.Writer = os.Stdout
	if cmd.Output != "" {
		f, err := os.Create(cmd.Output)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}
	_, err = c.ReadObject(ctx.ctx, cmd.Backend, schema.ReadObjectRequest{
		GetObjectRequest: schema.GetObjectRequest{Path: cmd.Path},
	}, func(chunk []byte) error {
		_, err := out.Write(chunk)
		return err
	})
	return err
}

func (cmd *DeleteCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}
	if cmd.Recursive {
		resp, err := c.DeleteObjects(ctx.ctx, cmd.Backend, schema.DeleteObjectsRequest{
			Path:      cmd.Path,
			Recursive: true,
		})
		if err != nil {
			return err
		}
		if ctx.Debug {
			return prettyJSON(resp)
		}
		return printObjects(resp.Body)
	}
	obj, err := c.DeleteObject(ctx.ctx, cmd.Backend, schema.DeleteObjectRequest{
		Path: cmd.Path,
	})
	if err != nil {
		return err
	}
	if ctx.Debug {
		return prettyJSON(obj)
	}
	if obj == nil {
		return nil
	}
	return printObjects([]schema.Object{*obj})
}

func (cmd *DownloadCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}

	// Resolve local download directory.
	localDir := cmd.Path
	if localDir == "" {
		if localDir, err = os.Getwd(); err != nil {
			return fmt.Errorf("cannot determine working directory: %w", err)
		}
	}
	absLocal, err := filepath.Abs(localDir)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(absLocal, 0o755); err != nil {
		return err
	}

	// List all remote objects under the prefix.
	remotePath := "/"
	if cmd.Prefix != "" {
		remotePath = "/" + strings.TrimPrefix(cmd.Prefix, "/")
	}
	resp, err := c.ListObjects(ctx.ctx, cmd.Backend, schema.ListObjectsRequest{
		Path:      remotePath,
		Recursive: true,
		Limit:     schema.MaxListLimit,
	})
	if err != nil {
		return err
	}

	// Strip the prefix from each remote path to get the local relative path.
	prefixStrip := strings.TrimSuffix(remotePath, "/") + "/"

	// Pre-filter: skip objects whose local copy already has the same size,
	// unless --force.
	type entry struct {
		obj      schema.Object
		localAbs string
	}
	var todo []entry
	for _, obj := range resp.Body {
		rel := strings.TrimPrefix(obj.Path, prefixStrip)
		if rel == "" {
			continue
		}
		localAbs := filepath.Join(absLocal, filepath.FromSlash(rel))
		if !cmd.Force {
			if fi, err := os.Stat(localAbs); err == nil && fi.Size() == obj.Size {
				continue // already up to date
			}
		}
		todo = append(todo, entry{obj, localAbs})
	}

	if len(todo) == 0 {
		return nil
	}

	tty := isTerminal(os.Stderr)
	total := len(todo)
	w := len(fmt.Sprintf("%d", total))

	for i, e := range todo {
		fileTag := fmt.Sprintf("[%*d/%d]", w, i+1, total)
		name := strings.TrimPrefix(e.obj.Path, "/")

		// Show "starting" line (use known obj.Size for placeholder column).
		if tty {
			if e.obj.Size > 0 {
				fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %5d%%  \x1b[1m%s\x1b[0m", fileTag, 0, name)
			} else {
				fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %6s  \x1b[1m%s\x1b[0m", fileTag, "?", name)
			}
		}

		// Create parent directories and destination file before streaming.
		if err := os.MkdirAll(filepath.Dir(e.localAbs), 0o755); err != nil {
			return err
		}
		f, err := os.Create(e.localAbs)
		if err != nil {
			return err
		}

		// Stream the object body in chunks, updating progress as data arrives.
		// fileSize is already known from the earlier ListObjects call.
		fileSize := e.obj.Size
		var written int64
		var lastPct int64 = -1
		_, err = c.ReadObject(ctx.ctx, cmd.Backend, schema.ReadObjectRequest{
			GetObjectRequest: schema.GetObjectRequest{Path: e.obj.Path},
		}, func(chunk []byte) error {
			if _, werr := f.Write(chunk); werr != nil {
				return werr
			}
			written += int64(len(chunk))
			if tty && fileSize > 0 {
				pct := written * 100 / fileSize
				if pct != lastPct {
					lastPct = pct
					fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %5d%%  \x1b[1m%s\x1b[0m", fileTag, pct, name)
				}
			}
			return nil
		})
		f.Close()
		if err != nil {
			return fmt.Errorf("%s: %w", e.obj.Path, err)
		}

		// Committed line.
		size := fmt.Sprintf("%6s", humanSize(written))
		if tty {
			fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %s  \x1b[1m%s\x1b[0m\n", fileTag, size, name)
		} else {
			fmt.Fprintf(os.Stderr, "  %s  %s  %s\n", fileTag, size, name)
		}
	}

	if tty {
		fmt.Fprintf(os.Stderr, "%d object(s) downloaded\n", len(todo))
	}
	return nil
}

func (cmd *UploadCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}

	// Resolve the local path: default to cwd.
	local := cmd.Path
	if local == "" {
		if local, err = os.Getwd(); err != nil {
			return fmt.Errorf("cannot determine working directory: %w", err)
		}
	}
	absLocal, err := filepath.Abs(local)
	if err != nil {
		return err
	}

	// Stat so we can distinguish file vs directory.
	fi, err := os.Stat(absLocal)
	if err != nil {
		return err
	}

	var fsys fs.FS
	var singleFile string // non-empty when uploading exactly one file
	if fi.IsDir() {
		fsys = os.DirFS(absLocal)
	} else {
		// Single file: walk the parent directory but restrict to this file only.
		fsys = os.DirFS(filepath.Dir(absLocal))
		singleFile = fi.Name()
	}

	// Build upload options.
	var uploadOpts []httpclient.UploadOpt

	if cmd.Prefix != "" {
		uploadOpts = append(uploadOpts, httpclient.WithPrefix(cmd.Prefix))
	}

	if cmd.Force {
		uploadOpts = append(uploadOpts, httpclient.WithCheck(nil))
	}

	// Filter: skip hidden entries unless --hidden is set; restrict to singleFile when set.
	uploadOpts = append(uploadOpts, httpclient.WithFilter(func(d fs.DirEntry) bool {
		if singleFile != "" {
			// Include the file itself; always descend the root "."
			return d.Name() == "." || d.Name() == singleFile
		}
		if !cmd.Hidden && strings.HasPrefix(d.Name(), ".") && d.Name() != "." {
			return false
		}
		return true
	}))

	// Progress callback: overwrite the current line on a terminal.
	tty := isTerminal(os.Stderr)
	uploadOpts = append(uploadOpts, httpclient.WithProgress(func(index, count int, path string, written, total int64) {
		w := len(fmt.Sprintf("%d", count)) // digits in total count
		fileTag := fmt.Sprintf("[%*d/%d]", w, index+1, count)
		name := strings.TrimPrefix(path, "/")
		if written == total && total > 0 {
			// File committed — print a permanent line.
			// Size column is right-aligned in 6 chars to match the percentage
			// column width used while the upload is in flight.
			size := fmt.Sprintf("%6s", humanSize(total))
			if tty {
				fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %s  \x1b[1m%s\x1b[0m\n",
					fileTag, size, name)
			} else {
				fmt.Fprintf(os.Stderr, "  %s  %s  %s\n",
					fileTag, size, name)
			}
		} else if tty && total > 0 {
			// Percentage right-aligned in 6 chars ("  100%") — same column
			// width as the size field above.
			pct := fmt.Sprintf("%5d%%", written*100/total)
			fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %s  \x1b[1m%s\x1b[0m", fileTag, pct, name)
		} else if tty {
			// File size unknown — show counter only.
			fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  \x1b[1m%s\x1b[0m", fileTag, name)
		}
	}))

	objs, err := c.CreateObjects(ctx.ctx, cmd.Backend, fsys, uploadOpts...)
	if err != nil {
		return err
	}
	if tty {
		fmt.Fprintf(os.Stderr, "%d object(s) uploaded\n", len(objs))
	}
	return nil
}
