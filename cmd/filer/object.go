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
	Path      string `name:"path" help:"Path prefix to list" default:"/"`
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

type CreateCommand struct {
	Backend     string   `arg:"" name:"backend" help:"Backend name"`
	Path        string   `arg:"" name:"path" help:"Object path"`
	File        string   `name:"file" short:"f" help:"Local file to upload (defaults to stdin)"`
	ContentType string   `name:"type" short:"t" help:"Content-Type (e.g. text/plain)"`
	Meta        []string `name:"meta" help:"Metadata as key=value pairs (repeatable)"`
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
	NoSkip  bool   `name:"no-skip" help:"Upload every file even when the remote copy appears up to date."`
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
	resp, err := c.ListObjects(ctx.ctx, cmd.Backend, schema.ListObjectsRequest{
		Path:      cmd.Path,
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
	case n >= TB:
		return fmt.Sprintf("%.1fT", float64(n)/float64(TB))
	case n >= GB:
		return fmt.Sprintf("%.1fG", float64(n)/float64(GB))
	case n >= MB:
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
	reader, _, err := c.ReadObject(ctx.ctx, cmd.Backend, schema.ReadObjectRequest{
		GetObjectRequest: schema.GetObjectRequest{Path: cmd.Path},
	})
	if err != nil {
		return err
	}
	defer reader.Close()

	var out io.Writer = os.Stdout
	if cmd.Output != "" {
		f, err := os.Create(cmd.Output)
		if err != nil {
			return err
		}
		defer f.Close()
		out = f
	}
	_, err = io.Copy(out, reader)
	return err
}

func (cmd *CreateCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}

	var src io.Reader = os.Stdin
	if cmd.File != "" {
		f, err := os.Open(cmd.File)
		if err != nil {
			return err
		}
		defer f.Close()
		src = f
	}

	var meta schema.ObjectMeta
	for _, kv := range cmd.Meta {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return fmt.Errorf("invalid meta %q: expected key=value", kv)
		}
		if meta == nil {
			meta = make(schema.ObjectMeta)
		}
		meta[k] = v
	}

	obj, err := c.CreateObject(ctx.ctx, cmd.Backend, schema.CreateObjectRequest{
		Path:        cmd.Path,
		Body:        src,
		ContentType: cmd.ContentType,
		Meta:        meta,
	})
	if err != nil {
		return err
	}
	return prettyJSON(obj)
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
		return prettyJSON(resp)
	}
	obj, err := c.DeleteObject(ctx.ctx, cmd.Backend, schema.DeleteObjectRequest{
		Path: cmd.Path,
	})
	if err != nil {
		return err
	}
	return prettyJSON(obj)
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

	if cmd.NoSkip {
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
	uploadOpts = append(uploadOpts, httpclient.WithProgress(func(path string, written, total int64) {
		if written == total && total > 0 {
			// File committed — print a final line.
			if tty {
				fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %s\n",
					humanSize(total), strings.TrimPrefix(path, "/"))
			} else {
				fmt.Fprintf(os.Stderr, "  %s  %s\n",
					humanSize(total), strings.TrimPrefix(path, "/"))
			}
		} else if tty && total > 0 {
			pct := written * 100 / total
			fmt.Fprintf(os.Stderr, "\r\x1b[K  %d%%  %s", pct, strings.TrimPrefix(path, "/"))
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
