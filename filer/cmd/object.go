package cmd

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	// Packages
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ListCommand struct {
	Volume    string `name:"volume" short:"v" help:"Backend volume name (saved as default for future commands)." optional:""`
	Path      string `arg:"" name:"path" help:"Path prefix to list" optional:"" default:"/"`
	Recursive bool   `name:"recursive" short:"r" help:"List recursively"`
	Limit     int    `name:"limit" short:"n" help:"Maximum number of objects to return (default: all)."`
	Offset    int    `name:"offset" help:"Number of objects to skip (for pagination)." default:"0"`
}

type GetCommand struct {
	Volume string `name:"volume" short:"v" help:"Backend volume name (saved as default for future commands)." optional:""`
	Path   string `arg:"" name:"path" help:"Object path (e.g. /dir/file.txt)"`
	Output string `name:"output" short:"o" help:"Write to file instead of stdout"`
}

type HeadCommand struct {
	Volume string `name:"volume" short:"v" help:"Backend volume name (saved as default for future commands)." optional:""`
	Path   string `arg:"" name:"path" help:"Object path"`
}

type DeleteCommand struct {
	Volume    string `name:"volume" short:"v" help:"Backend volume name (saved as default for future commands)." optional:""`
	Path      string `arg:"" name:"path" help:"Object path or prefix"`
	Recursive bool   `name:"recursive" short:"r" help:"Delete all objects under path, including subdirectories"`
}

type UploadCommand struct {
	Volume string `name:"volume" short:"v" help:"Backend volume name (saved as default for future commands)." optional:""`
	Path   string `arg:"" name:"path" help:"Local file or directory to upload (defaults to current directory)." optional:""`
	Prefix string `name:"prefix" short:"p" help:"Remote path prefix (e.g. backups/2026)."`
	Hidden bool   `name:"hidden" help:"Include files and directories whose names begin with '.'."`
	Force  bool   `name:"force" short:"f" help:"Upload every file even when the remote copy appears up to date."`
}

type DownloadCommand struct {
	Volume string `name:"volume" short:"v" help:"Backend volume name (saved as default for future commands)." optional:""`
	Path   string `arg:"" name:"path" help:"Local directory to download into (defaults to current directory)." optional:""`
	Prefix string `name:"prefix" short:"p" help:"Remote path prefix to download (e.g. backups/2026)."`
	Force  bool   `name:"force" short:"f" help:"Download every file even when the local copy appears up to date."`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *ListCommand) Run(globals server.Cmd) (err error) {
	return withClient(globals, "List", nil, func(ctx context.Context, client *httpclient.Client) error {
		volume, err := resolveVolume(ctx, globals, client, cmd.Volume)
		if err != nil {
			return err
		}

		limit := cmd.Limit
		if limit == 0 {
			limit = schema.MaxListLimit
		}
		resp, err := client.ListObjects(ctx, volume, schema.ListObjectsRequest{
			Path:      normalizeRemotePath(cmd.Path),
			Recursive: cmd.Recursive,
			Limit:     limit,
			Offset:    cmd.Offset,
		})
		if err != nil {
			return err
		}
		if globals.IsDebug() {
			fmt.Println(types.Stringify(resp))
		}
		return printListing(globals.IsTerm(), resp)
	})
}

func (cmd *HeadCommand) Run(globals server.Cmd) (err error) {
	return withClient(globals, "Head", nil, func(ctx context.Context, client *httpclient.Client) error {
		volume, err := resolveVolume(ctx, globals, client, cmd.Volume)
		if err != nil {
			return err
		}
		obj, err := client.GetObject(ctx, volume, schema.GetObjectRequest{
			Path: normalizeRemotePath(cmd.Path),
		})
		if err != nil {
			return err
		}
		fmt.Println(types.Stringify(obj))
		return nil
	})
}

func (cmd *GetCommand) Run(globals server.Cmd) (err error) {
	return withClient(globals, "Get", nil, func(ctx context.Context, client *httpclient.Client) error {
		volume, err := resolveVolume(ctx, globals, client, cmd.Volume)
		if err != nil {
			return err
		}

		var out io.Writer = os.Stdout
		var outFile *os.File
		if cmd.Output != "" {
			outFile, err = os.Create(cmd.Output)
			if err != nil {
				return err
			}
			out = outFile
		}
		_, err = client.ReadObject(ctx, volume, schema.ReadObjectRequest{
			GetObjectRequest: schema.GetObjectRequest{Path: normalizeRemotePath(cmd.Path)},
		}, func(chunk []byte) error {
			_, err := out.Write(chunk)
			return err
		})
		if outFile != nil {
			outFile.Close()
			if err != nil {
				os.Remove(cmd.Output)
			}
		}
		return err
	})
}

func (cmd *DeleteCommand) Run(globals server.Cmd) (err error) {
	return withClient(globals, "Delete", nil, func(ctx context.Context, client *httpclient.Client) error {
		volume, err := resolveVolume(ctx, globals, client, cmd.Volume)
		if err != nil {
			return err
		}
		// Route to the appropriate client call based on path and flags:
		//
		//   Single-object path (default):
		//     filer delete media /dir/file.txt
		//       → DeleteObject — server returns 404 if the object does not exist.
		//
		//   Bulk / prefix path (any of the following):
		//     filer delete media /           → non-recursive root sweep (Recursive=false)
		//     filer delete media /dir/       → non-recursive prefix sweep (trailing slash)
		//     filer delete media /dir -r     → recursive subtree wipe   (Recursive=true)
		//       → DeleteObjects — succeeds with 0 results when nothing matches.
		//
		// A plain path without a trailing slash and without -r is treated as a
		// specific object name, not a prefix, so callers get an explicit error on
		// a missing path rather than silent success.
		path := normalizeRemotePath(cmd.Path)
		isPrefix := cmd.Recursive || path == "/" || strings.HasSuffix(path, "/")

		if isPrefix {
			resp, err := client.DeleteObjects(ctx, volume, schema.DeleteObjectsRequest{
				Path:      path,
				Recursive: cmd.Recursive,
			})
			if err != nil {
				return err
			}
			if globals.IsDebug() {
				fmt.Println(types.Stringify(resp))
				return nil
			}
			return printObjects(globals.IsTerm(), resp.Body)
		}

		// Single-object delete: the server returns 404 if the object does not exist.
		obj, err := client.DeleteObject(ctx, volume, schema.DeleteObjectRequest{Path: path})
		if err != nil {
			return err
		}
		if globals.IsDebug() {
			fmt.Println(types.Stringify(obj))
			return nil
		}
		return printObjects(globals.IsTerm(), []schema.Object{*obj})
	})
}

func (cmd *DownloadCommand) Run(globals server.Cmd) (err error) {
	return withClient(globals, "Download", nil, func(ctx context.Context, client *httpclient.Client) error {
		volume, err := resolveVolume(ctx, globals, client, cmd.Volume)
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
		resp, err := client.ListObjects(ctx, volume, schema.ListObjectsRequest{
			Path:      remotePath,
			Recursive: true,
			Limit:     schema.MaxListLimit,
		})
		if err != nil {
			return err
		}

		// Strip the prefix from each remote path to get the local relative path.
		prefixStrip := strings.TrimSuffix(remotePath, "/") + "/"

		// Resolve accurate modtimes for all remote objects via parallel HEAD requests.
		// The list iterator returns the blob write time (not the original file modtime
		// stored in X-Object-Meta metadata), so we need the HEAD response to compare
		// correctly against local file modtimes.
		var accObjs []*schema.Object
		if !cmd.Force && len(resp.Body) > 0 {
			headReqs := make([]schema.GetObjectRequest, len(resp.Body))
			for i, obj := range resp.Body {
				headReqs[i] = schema.GetObjectRequest{Path: obj.Path}
			}
			accObjs, _ = client.GetObjects(ctx, volume, headReqs)
			for i, acc := range accObjs {
				if acc != nil {
					resp.Body[i].ModTime = acc.ModTime
					resp.Body[i].Size = acc.Size
				}
			}
		}

		// Pre-filter: skip objects whose local copy already matches the remote
		// (same size and modtime when both are known), unless --force.
		type entry struct {
			obj      schema.Object
			localAbs string
		}
		var todo []entry
		var skipped int
		for _, obj := range resp.Body {
			rel := strings.TrimPrefix(obj.Path, prefixStrip)
			if rel == "" {
				continue
			}
			localAbs := filepath.Join(absLocal, filepath.FromSlash(rel))
			if !cmd.Force {
				if fi, err := os.Stat(localAbs); err == nil && fi.Size() == obj.Size {
					lmt := fi.ModTime().Truncate(time.Second)
					rmt := obj.ModTime.Truncate(time.Second)
					if lmt.IsZero() || rmt.IsZero() || lmt.Equal(rmt) {
						skipped++
						continue // already up to date
					}
				}
			}
			todo = append(todo, entry{obj, localAbs})
		}

		tty := globals.IsTerm() > 0

		if len(todo) == 0 {
			if skipped > 0 {
				fmt.Fprintf(os.Stderr, "0 object(s) downloaded, %d skipped (up to date)\n", skipped)
			}
			return nil
		}

		total := len(todo)
		w := len(fmt.Sprintf("%d", total))

		for i, e := range todo {
			fileTag := fmt.Sprintf("[%*d/%d]", w, i+1, total)
			name := strings.TrimPrefix(e.obj.Path, "/")

			if tty {
				if e.obj.Size > 0 {
					fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %5d%%  \x1b[1m%s\x1b[0m", fileTag, 0, name)
				} else {
					fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %6s  \x1b[1m%s\x1b[0m", fileTag, "?", name)
				}
			}

			if err := os.MkdirAll(filepath.Dir(e.localAbs), 0o755); err != nil {
				return err
			}
			f, err := os.Create(e.localAbs)
			if err != nil {
				return err
			}

			fileSize := e.obj.Size
			var written int64
			var lastPct int64 = -1
			_, err = client.ReadObject(ctx, volume, schema.ReadObjectRequest{
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
				os.Remove(e.localAbs)
				return fmt.Errorf("%s: %w", e.obj.Path, err)
			}
			if !e.obj.ModTime.IsZero() {
				_ = os.Chtimes(e.localAbs, e.obj.ModTime, e.obj.ModTime)
			}

			size := fmt.Sprintf("%6s", humanSize(written))
			if tty {
				fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %s  \x1b[1m%s\x1b[0m\n", fileTag, size, name)
			} else {
				fmt.Fprintf(os.Stderr, "  %s  %s  %s\n", fileTag, size, name)
			}
		}

		if tty || len(todo) > 0 || skipped > 0 {
			if skipped > 0 {
				fmt.Fprintf(os.Stderr, "%d object(s) downloaded, %d skipped (up to date)\n", len(todo), skipped)
			} else {
				fmt.Fprintf(os.Stderr, "%d object(s) downloaded\n", len(todo))
			}
		}
		return nil
	})
}

func (cmd *UploadCommand) Run(globals server.Cmd) (err error) {
	return withClient(globals, "Upload", nil, func(ctx context.Context, client *httpclient.Client) error {
		volume, err := resolveVolume(ctx, globals, client, cmd.Volume)
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

		fi, err := os.Stat(absLocal)
		if err != nil {
			return err
		}

		var fsys fs.FS
		var singleFile string
		if fi.IsDir() {
			fsys = os.DirFS(absLocal)
		} else {
			fsys = os.DirFS(filepath.Dir(absLocal))
			singleFile = fi.Name()
		}

		var uploadOpts []httpclient.UploadOpt

		if cmd.Prefix != "" {
			uploadOpts = append(uploadOpts, httpclient.WithPrefix(cmd.Prefix))
		}

		if cmd.Force {
			uploadOpts = append(uploadOpts, httpclient.WithCheck(nil))
		}

		uploadOpts = append(uploadOpts, httpclient.WithFilter(func(d fs.DirEntry) bool {
			if singleFile != "" {
				return d.Name() == "." || d.Name() == singleFile
			}
			if !cmd.Hidden && strings.HasPrefix(d.Name(), ".") && d.Name() != "." {
				return false
			}
			return true
		}))

		tty := globals.IsTerm() > 0
		var skipped int
		if !cmd.Force {
			// Wrap the default skip check to count files that are already up to date.
			uploadOpts = append(uploadOpts, httpclient.WithCheck(func(info fs.FileInfo, remote *schema.Object) bool {
				if httpclient.SkipUnchanged(info, remote) {
					skipped++
					return true
				}
				return false
			}))
		}
		uploadOpts = append(uploadOpts, httpclient.WithProgress(func(index, count int, path string, written, total int64) {
			w := len(fmt.Sprintf("%d", count))
			fileTag := fmt.Sprintf("[%*d/%d]", w, index+1, count)
			name := strings.TrimPrefix(path, "/")
			if written == total {
				// File committed (including empty files where written == total == 0).
				size := fmt.Sprintf("%6s", humanSize(total))
				if tty {
					fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %s  \x1b[1m%s\x1b[0m\n", fileTag, size, name)
				} else {
					fmt.Fprintf(os.Stderr, "  %s  %s  %s\n", fileTag, size, name)
				}
			} else if tty && total > 0 {
				pct := fmt.Sprintf("%5d%%", written*100/total)
				fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  %s  \x1b[1m%s\x1b[0m", fileTag, pct, name)
			} else if tty {
				fmt.Fprintf(os.Stderr, "\r\x1b[K  %s  \x1b[1m%s\x1b[0m", fileTag, name)
			}
		}))

		objs, err := client.CreateObjects(ctx, volume, fsys, uploadOpts...)
		if err != nil {
			return err
		}
		if tty || len(objs) > 0 || skipped > 0 {
			switch {
			case len(objs) > 0 && skipped > 0:
				fmt.Fprintf(os.Stderr, "%d object(s) uploaded, %d skipped (up to date)\n", len(objs), skipped)
			case skipped > 0:
				fmt.Fprintf(os.Stderr, "0 object(s) uploaded, %d skipped (up to date)\n", skipped)
			default:
				fmt.Fprintf(os.Stderr, "%d object(s) uploaded\n", len(objs))
			}
		}
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// normalizeRemotePath converts a user-supplied remote path to an absolute
// server path. Shell shortcuts like ".", "./foo", and empty strings are
// mapped to their rooted equivalents so they don't produce dot-segments in
// URLs (which the HTTP router redirects away, breaking DELETE requests).
//
// Trailing slashes are preserved because they carry semantic meaning
// (prefix vs. exact-object selection).
func normalizeRemotePath(p string) string {
	// Strip a single leading "./" produced by shell tab-completion.
	p = strings.TrimPrefix(p, "./")
	// Bare "." or empty means the root of the volume.
	if p == "." || p == "" {
		return "/"
	}
	// Ensure the path is rooted.
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

///////////////////////////////////////////////////////////////////////////////
// OBJECT TABLE

// objectRow adapts a schema.Object for tui.TableFor.
type objectRow struct {
	schema.Object
}

func (r objectRow) Header() []string {
	return []string{"Name", "Size", "Date", "Type"}
}

func (r objectRow) Cell(col int) string {
	switch col {
	case 0:
		name := strings.TrimPrefix(r.Path, "/")
		if r.IsDir {
			name += "/"
		}
		return name
	case 1:
		if r.IsDir {
			return "DIR"
		}
		return humanSize(r.Size)
	case 2:
		return formatModTime(r.ModTime)
	case 3:
		return shortContentType(r.ContentType, r.Path)
	}
	return ""
}

func (r objectRow) Width(col int) int {
	switch col {
	case 3:
		return 30
	}
	return 0
}

// printListing renders a schema.ListObjectsResponse using the tui table.
func printListing(termWidth int, resp *schema.ListObjectsResponse) error {
	table := tui.TableFor[objectRow](tui.SetWidth(termWidth))
	rows := make([]objectRow, len(resp.Body))
	for i, obj := range resp.Body {
		rows[i] = objectRow{obj}
	}
	if _, err := table.Write(os.Stdout, rows...); err != nil {
		return err
	}
	limit := uint64(resp.Count)
	_, err := tui.TableSummary("object(s)", uint(resp.Count), 0, &limit).Write(os.Stdout)
	return err
}

// printObjects renders a slice of objects using the tui table (used by delete).
func printObjects(termWidth int, objs []schema.Object) error {
	table := tui.TableFor[objectRow](tui.SetWidth(termWidth))
	rows := make([]objectRow, len(objs))
	for i, obj := range objs {
		rows[i] = objectRow{obj}
	}
	if _, err := table.Write(os.Stdout, rows...); err != nil {
		return err
	}
	n := uint(len(objs))
	limit := uint64(n)
	_, err := tui.TableSummary("object(s) deleted", n, 0, &limit).Write(os.Stdout)
	return err
}

// shortContentType strips parameters from a MIME type. When ct is empty or
// generic (application/octet-stream) it falls back to inferring the type from
// the file extension of path. Returns "-" if neither source yields a useful type.
func shortContentType(ct, path string) string {
	if ct == "" || ct == "application/octet-stream" {
		if inferred := httpclient.MIMEByExt(filepath.Ext(path)); inferred != "" {
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
