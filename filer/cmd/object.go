package cmd

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	// Packages
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/filer/httpclient"
	schema "github.com/mutablelogic/go-filer/filer/schema"
	server "github.com/mutablelogic/go-server"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ObjectCommands struct {
	ObjectList   ObjectListCmd   `cmd:"" name:"objects" help:"List the objects in a directory of a volume, a page at a time." group:"OBJECT"`
	ObjectHead   ObjectHeadCmd   `cmd:"" name:"object" help:"Get the metadata of an object in a volume." group:"OBJECT"`
	ObjectGet    ObjectGetCmd    `cmd:"" name:"object-get" help:"Download a file in a volume, to a file, or to standard output." group:"OBJECT"`
	ObjectPut    ObjectPutCmd    `cmd:"" name:"object-put" help:"Upload a file, or standard input, to a file in a volume." group:"OBJECT"`
	ObjectUpload ObjectUploadCmd `cmd:"" name:"object-upload" help:"Upload files, and directories, into a directory of a volume, in one request." group:"OBJECT"`
	ObjectDelete ObjectDeleteCmd `cmd:"" name:"object-delete" help:"Delete a file in a volume, or a directory, with --recursive." group:"OBJECT"`
}

type ObjectPutCmd struct {
	Name        string   `arg:"" name:"name" help:"Volume name."`
	Path        string   `arg:"" name:"path" help:"Path of the file in the volume, or of the directory, ending with /, to upload it to with its name."`
	File        string   `arg:"" name:"file" help:"File to upload, or - for standard input."`
	IfNotExists bool     `name:"no-replace" help:"Don't replace a file which exists."`
	IfExists    bool     `name:"replace-only" help:"Don't create a file which doesn't exist."`
	Meta        []string `name:"meta" sep:"none" placeholder:"[NAMESPACE.]KEY=VALUE" help:"Metadata of the file, such as photo.camera=X100V, or rating=5, which can be repeated. A value which is JSON, such as 5 or true, is kept, and otherwise it's a string."`
}

type ObjectUploadCmd struct {
	Name      string   `arg:"" name:"name" help:"Volume name."`
	Path      string   `arg:"" name:"path" help:"Directory in the volume to upload into, which is created, or / for its root."`
	Files     []string `arg:"" name:"file" help:"Files, and directories, to upload, where a directory is uploaded with every file beneath it, into a directory of its name, or, when it ends with /, such as dir/, into the directory itself."`
	Overwrite bool     `name:"overwrite" short:"y" help:"Replace files which exist, rather than skipping them."`
	Meta      []string `name:"meta" sep:"none" placeholder:"[NAMESPACE.]KEY=VALUE" help:"Metadata of each file, such as photo.camera=X100V, or rating=5, which can be repeated. A value which is JSON, such as 5 or true, is kept, and otherwise it's a string."`
}

type ObjectDeleteCmd struct {
	Name      string `arg:"" name:"name" help:"Volume name."`
	Path      string `arg:"" name:"path" help:"Path of the file, or directory, in the volume."`
	Recursive bool   `name:"recursive" short:"r" help:"Delete a directory, and every object beneath it."`
}

type ObjectGetCmd struct {
	Name  string `arg:"" name:"name" help:"Volume name."`
	Path  string `arg:"" name:"path" help:"Path of the file in the volume."`
	File  string `arg:"" name:"file" optional:"" help:"File to download it to, which defaults to the file's name, in the current directory, or - for standard output."`
	Force bool   `name:"force" short:"f" help:"Download the file when the file it's downloaded to exists, and isn't older."`
}

type ObjectHeadCmd struct {
	Name string `arg:"" name:"name" help:"Volume name."`
	Path string `arg:"" name:"path" optional:"" help:"Path of the object, which defaults to the root of the volume."`
}

type ObjectListCmd struct {
	Name       string  `arg:"" name:"name" help:"Volume name."`
	Path       string  `arg:"" name:"path" optional:"" help:"Directory to list, which defaults to the root of the volume."`
	Recursive  bool    `name:"recursive" short:"r" help:"List every object beneath the directory."`
	StartAfter string  `name:"start-after" help:"Return objects whose keys sort after this key, which is the next key of the previous page."`
	Limit      *uint64 `name:"limit" help:"Maximum number of objects to return, which defaults to and is capped at 100."`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ObjectListCmd) Run(ctx server.Cmd) error {
	// Width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	// Perform the request
	return withClient(ctx, "filer.ObjectListCmd", func(ctx context.Context, client *httpclient.Client) error {
		list, err := client.ListObjects(ctx, cmd.request())
		if err != nil {
			return err
		}

		// With debugging
		if debug {
			fmt.Println(list)
			return nil
		}

		// Objects table
		table := tui.TableFor[*schema.Object](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, list.Body...); err != nil {
			return err
		}

		// Objects summary, which is of a page, whose total isn't known, and
		// how to get the next page
		count := uint(len(list.Body))
		if _, err := tui.TableSummary("objects", 0, count, 0, list.Limit).Write(os.Stdout); err != nil {
			return err
		}
		if list.Next != "" {
			fmt.Printf("More objects with --start-after %q\n", list.Next)
		}
		return nil
	})
}

func (cmd *ObjectHeadCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "filer.ObjectHeadCmd", func(ctx context.Context, client *httpclient.Client) error {
		object, err := client.HeadObject(ctx, cmd.Name, cmd.Path)
		if err != nil {
			return err
		}

		fmt.Println(object)
		return nil
	})
}

func (cmd *ObjectGetCmd) Run(ctx server.Cmd) error {
	// The file, which is the file's name without one
	file := cmd.File
	if file == "" {
		if file = path.Base(strings.TrimSuffix(cmd.Path, "/")); file == "." || file == "/" || file == "" {
			return gofiler.ErrBadParameter.Withf("%s/%s has no file name: set the file to download it to", cmd.Name, strings.TrimLeft(cmd.Path, "/"))
		}
	}

	return withClient(ctx, "filer.ObjectGetCmd", func(ctx context.Context, client *httpclient.Client) (err error) {
		req := schema.GetObjectRequest{ObjectKey: schema.ObjectKey{Name: cmd.Name, Path: cmd.Path}}

		// To standard output
		if file == "-" {
			_, err := client.GetObject(ctx, req, os.Stdout)
			return err
		}

		// A file which exists is only downloaded again when the file has been
		// modified since it was, whose modified time is the file's
		if info, err := os.Stat(file); err == nil {
			if !info.Mode().IsRegular() {
				return fmt.Errorf("%q isn't a file", file)
			} else if !cmd.Force {
				req.IfModifiedSince = info.ModTime()
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}

		// Download it to a temporary file, beside the file, which replaces it
		// once it's downloaded, so a download which fails doesn't change it
		tmp, err := createTemp(file)
		if err != nil {
			return err
		}
		defer func() {
			if tmp != nil {
				err = errors.Join(err, tmp.Close(), os.Remove(tmp.Name()))
			}
		}()
		object, err := client.GetObject(ctx, req, tmp)
		if errors.Is(err, gofiler.ErrNotModified) {
			fmt.Printf("%s: not modified\n", file)
			return nil
		} else if errors.Is(err, gofiler.ErrBadParameter) && strings.Contains(err.Error(), "is a directory") && cmd.File != "" && cmd.File != "-" {
			return fmt.Errorf("%w: the path is the file's, with its directory, such as %q", err, path.Join(cmd.Path, filepath.Base(cmd.File)))
		} else if err != nil {
			return err
		} else if err := tmp.Close(); err != nil {
			return err
		}
		if !object.ModTime.IsZero() {
			if err := os.Chtimes(tmp.Name(), time.Time{}, object.ModTime); err != nil {
				return err
			}
		}
		if err := os.Rename(tmp.Name(), file); err != nil {
			return err
		}
		tmp = nil

		// Summarise the download
		fmt.Printf("%s: %s, %s\n", file, object.ContentType, object.Cell(2))
		return nil
	})
}

func (cmd *ObjectPutCmd) Run(ctx server.Cmd) error {
	req, closer, err := cmd.request()
	if err != nil {
		return err
	}
	defer closer.Close()
	width := ctx.IsTerm()
	return withClient(ctx, "filer.ObjectPutCmd", func(ctx context.Context, client *httpclient.Client) error {
		// The progress of the upload, when the file's size is known, and
		// standard error is a terminal
		var progress *transfer
		if f, ok := req.Body.(*os.File); ok && f != os.Stdin {
			if info, err := f.Stat(); err == nil {
				progress = newTransfer(width, info.Size())
				req.Body = progress.Reader(req.Path, f)
			}
		}
		object, err := client.PutObject(ctx, req)
		progress.Close()
		if err != nil {
			return err
		}

		fmt.Println(object)
		return nil
	})
}

// request returns the request to upload the file, whose modified time is the
// file's, and the file, which is closed once it's uploaded. A path ending
// with / is a directory, which the file is uploaded to with its name.
func (cmd *ObjectPutCmd) request() (schema.CreateObjectRequest, io.Closer, error) {
	req := schema.CreateObjectRequest{ObjectKey: schema.ObjectKey{Name: cmd.Name, Path: cmd.Path}, IfNotExists: cmd.IfNotExists, IfExists: cmd.IfExists}
	if cmd.IfNotExists && cmd.IfExists {
		return req, nil, gofiler.ErrBadParameter.With("--no-replace and --replace-only can't both be set")
	}
	meta, err := parseMeta(cmd.Meta)
	if err != nil {
		return req, nil, err
	}
	req.Meta = meta
	if cmd.File == "-" {
		if strings.HasSuffix(cmd.Path, "/") {
			return req, nil, gofiler.ErrBadParameter.Withf("standard input has no name: set the path of the file in the volume, not a directory")
		}
		req.Body = os.Stdin
		return req, io.NopCloser(nil), nil
	}
	f, err := os.Open(cmd.File)
	if err != nil {
		return req, nil, err
	} else if info, err := f.Stat(); err != nil {
		return req, nil, errors.Join(err, f.Close())
	} else if !info.Mode().IsRegular() {
		return req, nil, errors.Join(fmt.Errorf("%q isn't a file", cmd.File), f.Close())
	} else {
		req.ModTime = info.ModTime()
	}
	if strings.HasSuffix(req.Path, "/") {
		req.Path += filepath.Base(cmd.File)
	}
	req.Body = f
	return req, f, nil
}

func (cmd *ObjectUploadCmd) Run(ctx server.Cmd) error {
	local, err := cmd.files()
	if err != nil {
		return err
	}
	meta, err := parseMeta(cmd.Meta)
	if err != nil {
		return err
	}

	// Width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	return withClient(ctx, "filer.ObjectUploadCmd", func(ctx context.Context, client *httpclient.Client) error {
		// The files which exist aren't sent, unless they're overwritten, as
		// the server would read them to skip them
		var skipped []schema.ObjectSkipped
		if !cmd.Overwrite {
			existing, err := existingFiles(ctx, client, cmd.Name, cmd.Path)
			if err != nil {
				return err
			}
			upload := local[:0]
			for _, file := range local {
				if key := path.Join("/", cmd.Path, file.path); existing[key] {
					skipped = append(skipped, schema.ObjectSkipped{Path: key, Exists: true, Reason: "the file exists"})
				} else {
					upload = append(upload, file)
				}
			}
			local = upload
		}

		// The progress of the upload, of the bytes of every file, when
		// standard error is a terminal, as each file is read
		var total int64
		for _, file := range local {
			total += file.size
		}
		progress := newTransfer(width, total)
		files := make([]httpclient.UploadFile, 0, len(local))
		for _, file := range local {
			files = append(files, httpclient.UploadFile{Path: file.path, ModTime: file.modtime, Meta: meta, Open: func() (io.ReadCloser, error) {
				f, err := os.Open(file.name)
				if err != nil {
					return nil, err
				}
				return progress.Reader(file.path, f), nil
			}})
		}
		resp := &schema.ObjectUploadResponse{}
		if len(files) > 0 {
			resp, err = client.UploadObjects(ctx, cmd.Name, cmd.Path, files, cmd.Overwrite)
		}
		progress.Close()
		if err != nil {
			return err
		}
		resp.Skipped = append(skipped, resp.Skipped...)

		// With debugging
		if debug {
			fmt.Println(types.Stringify(resp))
			return nil
		}

		// The files which were written, and those which were skipped
		if len(resp.Body) > 0 {
			table := tui.TableFor[*schema.Object](tui.SetWidth(width))
			if _, err := table.Write(os.Stdout, resp.Body...); err != nil {
				return err
			}
		}
		for _, skipped := range resp.Skipped {
			fmt.Printf("Skipped %s: %s\n", skipped.Path, skipped.Reason)
		}
		fmt.Printf("Uploaded %d of %d files", len(resp.Body), len(local)+len(skipped))
		if exists := countExists(resp.Skipped); exists > 0 && !cmd.Overwrite {
			fmt.Printf(", %d exist: use --overwrite (-y) to replace them", exists)
		}
		fmt.Println()
		return nil
	})
}

// localFile is a file to upload, by its path in the directory, and its name,
// and size, on disk
type localFile struct {
	path, name string
	size       int64
	modtime    time.Time
}

// files returns the files to upload, by their paths in the directory, where a
// directory's files are in a directory of its name, unless it ends with a
// separator, such as dir/, when they're in the directory itself. Files which aren't
// regular files, such as links and devices, aren't uploaded.
func (cmd *ObjectUploadCmd) files() ([]localFile, error) {
	var files []localFile
	for _, arg := range cmd.Files {
		info, err := os.Stat(arg)
		if err != nil {
			return nil, err
		} else if !info.IsDir() {
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("%q isn't a file", arg)
			}
			files = append(files, localFile{path: filepath.Base(arg), name: arg, size: info.Size(), modtime: info.ModTime()})
			continue
		}
		// A directory with a trailing separator, such as dir/, is its files,
		// without the directory, as rsync has
		base := filepath.Base(filepath.Clean(arg))
		if strings.HasSuffix(arg, "/") || strings.HasSuffix(arg, string(filepath.Separator)) {
			base = ""
		}
		if err := filepath.WalkDir(arg, func(name string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			} else if !entry.Type().IsRegular() {
				return nil
			}
			rel, err := filepath.Rel(arg, name)
			if err != nil {
				return err
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			files = append(files, localFile{path: path.Join(base, filepath.ToSlash(rel)), name: name, size: info.Size(), modtime: info.ModTime()})
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if len(files) == 0 {
		return nil, gofiler.ErrBadParameter.With("no files to upload")
	}
	return files, nil
}

// existingFiles returns the paths of the files beneath the directory in the
// volume, from its listing, a page at a time, which are none when it
// doesn't exist
func existingFiles(ctx context.Context, client *httpclient.Client, name, dir string) (map[string]bool, error) {
	existing := map[string]bool{}
	var req schema.ObjectListRequest
	req.Name, req.Path, req.Recursive = name, dir, true
	for {
		list, err := client.ListObjects(ctx, req)
		var resp httpresponse.ErrResponse
		if errors.As(err, &resp) && resp.Code == http.StatusNotFound && req.StartAfter == "" {
			if _, err := client.GetVolume(ctx, name); err != nil {
				return nil, err
			}
			return existing, nil
		} else if err != nil {
			return nil, err
		}
		for _, object := range list.Body {
			if !object.IsDir {
				existing[object.Path] = true
			}
		}
		if list.Next == "" {
			return existing, nil
		}
		req.StartAfter = list.Next
	}
}

// createTemp creates a hidden temporary file beside the file, which replaces
// it once it's written, whose permissions are those of a file which is
// created, which the umask applies to, rather than os.CreateTemp's 0600
func createTemp(file string) (*os.File, error) {
	dir, base := filepath.Dir(file), filepath.Base(file)
	for {
		tmp, err := os.OpenFile(filepath.Join(dir, "."+base+"."+rand.Text()), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
		if !errors.Is(err, fs.ErrExist) {
			return tmp, err
		}
	}
}

// countExists returns how many of the files skipped exist
func countExists(skipped []schema.ObjectSkipped) int {
	n := 0
	for _, skipped := range skipped {
		if skipped.Exists {
			n++
		}
	}
	return n
}

func (cmd *ObjectDeleteCmd) Run(ctx server.Cmd) error {
	// Width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	// Perform the request
	return withClient(ctx, "filer.ObjectDeleteCmd", func(ctx context.Context, client *httpclient.Client) error {
		deleted, err := client.DeleteObjects(ctx, schema.DeleteObjectsRequest{ObjectKey: schema.ObjectKey{Name: cmd.Name, Path: cmd.Path}, Recursive: cmd.Recursive})
		if err != nil {
			return err
		}

		// With debugging
		if debug {
			fmt.Println(types.Stringify(deleted))
			return nil
		}

		// Deleted objects table, and summary
		table := tui.TableFor[*schema.Object](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, deleted.Body...); err != nil {
			return err
		}
		count := uint(len(deleted.Body))
		fmt.Printf("Deleted %d %s\n", count, map[bool]string{true: "object", false: "objects"}[count == 1])
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// request returns the request for the page of objects
func (cmd *ObjectListCmd) request() schema.ObjectListRequest {
	var req schema.ObjectListRequest
	req.Name, req.Path = cmd.Name, cmd.Path
	req.Recursive, req.StartAfter, req.Limit = cmd.Recursive, cmd.StartAfter, cmd.Limit
	return req
}

// parseMeta returns the metadata of the flags, each of which is
// [NAMESPACE.]KEY=VALUE, whose value is JSON, when it is, and otherwise a
// string, or nil when there are none
func parseMeta(flags []string) ([]schema.Meta, error) {
	var result []schema.Meta
	for _, flag := range flags {
		name, value, ok := strings.Cut(flag, "=")
		if !ok || name == "" {
			return nil, gofiler.ErrBadParameter.Withf("invalid metadata %q, which is [NAMESPACE.]KEY=VALUE", flag)
		}
		meta := schema.Meta{Key: name}
		if namespace, key, ok := strings.Cut(name, "."); ok {
			meta.Namespace, meta.Key = namespace, key
			if namespace == "" || key == "" {
				return nil, gofiler.ErrBadParameter.Withf("invalid metadata %q, whose namespace and key can't be empty", flag)
			}
		}
		if json.Valid([]byte(value)) {
			meta.Value = json.RawMessage(value)
		} else {
			meta.Value, _ = json.Marshal(value)
		}
		result = append(result, meta)
	}
	return result, nil
}
