package google

import (
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	// Packages
	client "github.com/mutablelogic/go-client"
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/backend/google/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// DriveFiler is a filer of the files a person owns in their Google Drive's
// My Drive, by their paths, whose folders are directories, and whose Google
// Docs, Sheets and Slides are Word, Excel and PowerPoint documents. It's
// read, and watched, and written, when it's writable, of its files, but
// not of its Docs, Sheets and Slides, and what's deleted goes to its bin.
type DriveFiler struct {
	driveOpt
	name   string
	opts   gofiler.Opts       // Its credentials, and client options, which a clone is created with
	cache  gofiler.CacheStore // The volume's cache, which the registry owns
	tokens *tokens
	client *httpclient.DriveClient

	// The files, by their paths, got from Google, for the refresh interval,
	// which a file is read of, rather than got again, or, while it's
	// watched, those the watch knows, which it keeps up to date
	tree *cache[*driveTree]
	mu   sync.Mutex
	live *driveWatch

	// naming is held by a create, as it checks the names of a new file, and
	// its folders, and creates them, so two don't both create them
	naming sync.Mutex
}

// driveOpt are the options of a drive filer, of its parameters
type driveOpt struct {
	readonly        bool          // Files can't be created, updated or deleted
	upload          int           // The bytes of each request of an upload, or zero, of the client's default
	quota           uint          // The most requests each minute, or zero
	refreshInterval time.Duration // How long the files are kept, once they're got
	endpoint        string        // The endpoint of the API, which is Google's, but a test's
	credential      string        // The key of the user's sign-in
	description     string        // What the volume is, which is of the user who signed in, by default
}

// driveTree is the files, and folders, beneath My Drive, by their paths,
// and the paths in each folder, of its path, which the root's is "/"
type driveTree struct {
	root     string // The id of My Drive's folder
	files    map[string]*httpclient.DriveFile
	children map[string][]string
}

var _ gofiler.Filer = (*DriveFiler)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// DriveScheme is the scheme of the URL of a volume of a person's Google
	// Drive, gdrive://<name>
	DriveScheme = "gdrive"

	// DriveNamespace is the namespace of the metadata of files, which their
	// content can't have: their ids, their links in Google Drive, who owns
	// them, and the type of those which are Google's own, such as document
	DriveNamespace = "drive"
)

// driveExport is the type a Google Doc, Sheet or Slides is exported as, and
// its extension
type driveExport struct {
	contentType, ext string
}

// driveExports are the types of the Google Docs, Sheets and Slides which
// are files, of the types they're exported as. Google's other types, such as
// forms, and shortcuts, aren't files.
var driveExports = map[string]driveExport{
	httpclient.DriveAppsPrefix + "document":     {"application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx"},
	httpclient.DriveAppsPrefix + "spreadsheet":  {"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"},
	httpclient.DriveAppsPrefix + "presentation": {"application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx"},
}

const (
	// driveExportMax is the most of a Google Doc, Sheet or Slides which is
	// exported, which Google's limit is
	driveExportMax = 10 << 20

	// driveExportKey is the prefix of the keys of the exports, in the
	// volume's cache, of their files' ids, and driveExportTTL how long
	// they're kept, once they're exported
	driveExportKey = "export/"
	driveExportTTL = 7 * 24 * time.Hour
)

// driveEndpoint is the endpoint of the Drive API, which is Google's, unless
// a test sets another. It isn't a parameter, as a volume's URL could send
// the sign-in's token to another server.
var driveEndpoint = httpclient.DriveEndpoint

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewDriveFiler returns a filer of the Google Drive of the volume's name,
// with the parameters, the user's sign-in, of the provider of credentials,
// and go-client's options of its requests, such as a tracer. The sign-in,
// and the files, are checked, as for NewTaskFiler, and its parameters are
// readonly, which is true by default, and, when it's false, needs a sign-in
// which can write files, of the drive scope, quota, and
// credential, as a task filer's, and refresh_interval, how often changes
// are polled for, when it's watched, and how long the files are kept, once
// they're got, when it isn't, which is DefaultRefreshInterval, or zero not
// to watch it, nor keep them.
func NewDriveFiler(ctx context.Context, name string, params url.Values, cache gofiler.CacheStore, opts gofiler.Opts) (*DriveFiler, error) {
	if cache == nil {
		return nil, gofiler.ErrBadParameter.Withf("volume %q has no cache", name)
	}
	if !types.IsIdentifier(name) {
		return nil, gofiler.ErrBadParameter.Withf("invalid volume name: %q", name)
	}
	opt, err := parseDriveOpts(params)
	if err != nil {
		return nil, err
	}
	opts.ClientOpts = slices.Clone(opts.ClientOpts)
	self := &DriveFiler{driveOpt: opt, name: name, opts: opts, cache: cache}
	self.tree = newCache[*driveTree](time.Now, func() time.Duration { return self.refreshInterval })

	// The user's sign-in, which can read the files, or write them, too
	required, scopes := []string{httpclient.DriveScope}, []string{httpclient.DriveScope, httpclient.EmailScope}
	if opt.readonly {
		required, scopes = []string{httpclient.DriveReadonlyScope, httpclient.DriveScope}, []string{httpclient.DriveReadonlyScope, httpclient.EmailScope}
	}
	if self.tokens, err = newTokens(ctx, opts.Credentials, opt.credential, required, scopes); err != nil {
		return nil, err
	}

	// The client, whose requests have the sign-in's token, and are limited
	// to the quota, spaced evenly
	api, err := url.Parse(opt.endpoint)
	if err != nil {
		return nil, err
	}
	clientopts := append(slices.Clone(opts.ClientOpts), client.OptTransport(func(parent http.RoundTripper) http.RoundTripper {
		return &tokenTransport{parent: parent, tokens: self.tokens, scheme: api.Scheme, host: api.Host}
	}))
	if opt.quota > 0 {
		clientopts = append(clientopts, client.OptRateLimit(float32(opt.quota)/60))
	}
	if self.client, err = httpclient.NewDriveClient(opt.endpoint, clientopts...); err != nil {
		return nil, err
	}

	// Check the files can be read
	if _, err := self.client.Root(ctx); err != nil {
		return nil, self.apiError("check", "/", err)
	}

	// What the volume is, when it isn't set, is of the user who signed in
	if self.description == "" {
		self.description = describe(ctx, "Google Drive", self.tokens, opts.ClientOpts)
	}
	return self, nil
}

// parseDriveOpts returns the options of the parameters, which are rejected
// when they can't be parsed, or are for writing
func parseDriveOpts(params url.Values) (driveOpt, error) {
	opt := driveOpt{readonly: true, quota: DefaultTasksQuota, refreshInterval: DefaultRefreshInterval, credential: DefaultCredential, endpoint: driveEndpoint}
	for key := range params {
		switch key {
		case paramReadonly, paramQuota, paramRefreshInterval, paramCredential, schema.OptDescription:
		default:
			return opt, gofiler.ErrBadParameter.Withf("unknown parameter: %q", key)
		}
	}
	opt.description = params.Get(schema.OptDescription)
	if value := params.Get(paramReadonly); value != "" {
		if readonly, err := strconv.ParseBool(value); err != nil {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramReadonly, value)
		} else {
			opt.readonly = readonly
		}
	}
	if value := params.Get(paramQuota); value != "" {
		quota, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramQuota, value)
		}
		opt.quota = uint(quota)
	}
	if value := params.Get(paramRefreshInterval); value != "" {
		interval, err := time.ParseDuration(value)
		if err != nil || interval < 0 {
			return opt, gofiler.ErrBadParameter.Withf("%s: %q", paramRefreshInterval, value)
		}
		opt.refreshInterval = interval
	}
	if value := params.Get(paramCredential); value != "" {
		if err := credentialKey(value); err != nil {
			return opt, err
		}
		opt.credential = value
	}
	return opt, nil
}

// Close the filer
func (self *DriveFiler) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - PROPERTIES

// Description returns what the volume is, such as "Google Drive for ada@example.com"
func (self *DriveFiler) Description() string {
	return self.description
}

// URL returns the URL of the filer, such as gdrive://drive, with each of its
// parameters
func (self *DriveFiler) URL() *url.URL {
	return &url.URL{Scheme: DriveScheme, Host: self.name, RawQuery: self.params().Encode()}
}

// Clone returns a new filer of the drive, with the parameters changed by
// the patch, and the same client options, which is checked as a new filer
// is
func (self *DriveFiler) Clone(ctx context.Context, patch url.Values) (gofiler.Filer, error) {
	// A clone of another sign-in, without a description, is described of it
	params := gofiler.PatchValues(self.params(), patch)
	credential := params.Get(paramCredential)
	if credential == "" {
		credential = DefaultCredential
	}
	if credential != self.credential && !patch.Has(schema.OptDescription) {
		params.Del(schema.OptDescription)
	}
	clone, err := NewDriveFiler(ctx, self.name, params, self.cache, self.opts)
	if err != nil {
		return nil, err
	}
	return clone, nil
}

// Readonly returns true unless the volume is writable, with readonly=false
func (self *DriveFiler) Readonly() bool {
	return self.readonly
}

// Actions returns none, as a drive is written of its objects
func (self *DriveFiler) Actions() []gofiler.Action {
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - OBJECTS

// GetObject returns the root, a folder, which is a directory, or a file
func (self *DriveFiler) GetObject(ctx context.Context, req schema.GetObjectRequest) (*schema.Object, error) {
	object, _, err := self.stat(ctx, "get", req.Path)
	if err != nil {
		return nil, err
	} else if object.NotModified(req) {
		return nil, gofiler.ErrNotModified.Withf("%q", object.Path)
	}
	return object, nil
}

// ReadObject returns the function which reads a file, and the file. A file
// is read of its revision when it was listed, so its content is the file's,
// though it's changed since, and can seek, and be read at an offset, so
// it's read in place, of the ranges which are read (see driveReader). A Google Doc, Sheet or
// Slides is exported, which can't be of a version, so it's read into memory,
// and its version is checked before, and after, which is
// ErrPreconditionFailed, when it's changed since it was listed. An error of
// the function is returned as it is. A directory can't be read, which is
// fs.ErrInvalid.
func (self *DriveFiler) ReadObject(ctx context.Context, req schema.GetObjectRequest) (gofiler.ReadObjectFunc, *schema.Object, error) {
	object, file, err := self.stat(ctx, "read", req.Path)
	if err != nil {
		return nil, nil, err
	} else if object.IsDir {
		return nil, nil, &fs.PathError{Op: "read", Path: req.Path, Err: fs.ErrInvalid}
	} else if object.NotModified(req) {
		return nil, nil, gofiler.ErrNotModified.Withf("%q", object.Path)
	}
	return func(ctx context.Context, fn func(io.Reader) error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if export, exists := driveExports[file.MimeType]; exists {
			return self.export(ctx, object.Path, file, export, fn)
		}
		if size, err := strconv.ParseInt(file.Size, 10, 64); err == nil && size > 0 && file.HeadRevision != "" {
			// It's read in place, of the ranges which are read, and requested
			// before it's read, so a request which fails is known first
			r := newDriveReader(ctx, self, object.Path, file, size)
			defer r.Close()
			if err := r.open(0); err != nil {
				return err
			}
			return fn(r)
		}

		// Without a size, or a revision, or of no bytes, it's read as it's
		// downloaded, so a download which fails is known first
		var fnErr error
		if err := self.client.Download(ctx, file.Id, file.HeadRevision, 0, 0, func(r io.Reader) error {
			fnErr = fn(&streamReader{ctx: ctx, r: r})
			return fnErr
		}); fnErr != nil {
			return fnErr
		} else if err != nil {
			return self.apiError("read", object.Path, err)
		}
		return nil
	}, object, nil
}

// ListObjects lists a directory, of the files, and directories, in it, or,
// when it's recursive, beneath it, or a file, in the order of their paths,
// after the path the request starts after
func (self *DriveFiler) ListObjects(ctx context.Context, req schema.ListObjectsRequest) (schema.ListObjectsIterator, error) {
	tree, err := self.files(ctx, "list", req.Path)
	if err != nil {
		return nil, err
	}
	object, _, err := self.lookup(tree, "list", req.Path)
	if err != nil {
		return nil, err
	} else if !object.IsDir {
		var objects []*schema.Object
		if object.Path > req.StartAfter {
			objects = append(objects, object)
		}
		return &objectList{objects: objects}, nil
	}
	var objects []*schema.Object
	var walk func(dir string)
	walk = func(dir string) {
		for _, p := range tree.children[dir] {
			object := self.object(tree, p)
			if sortKey(object) > req.StartAfter {
				objects = append(objects, object)
			}
			if object.IsDir && req.Recursive {
				walk(p)
			}
		}
	}
	walk(object.Path)
	slices.SortFunc(objects, func(a, b *schema.Object) int { return strings.Compare(sortKey(a), sortKey(b)) })
	return &objectList{objects: objects}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// params returns each of the filer's parameters, so its URL says how it's
// set up
func (self *DriveFiler) params() url.Values {
	return url.Values{
		paramReadonly:         {strconv.FormatBool(self.readonly)},
		paramQuota:            {strconv.FormatUint(uint64(self.quota), 10)},
		paramRefreshInterval:  {self.refreshInterval.String()},
		paramCredential:       {self.credential},
		schema.OptDescription: {self.description},
	}
}

// apiError returns the error of a request, as a task filer's does
func (self *DriveFiler) apiError(op, path string, err error) error {
	return apiError(self.tokens, self.credential, op, path, err)
}

// files returns the files, and folders, beneath My Drive, by their paths,
// of the watch, while it's watched, or those which were got, for the
// refresh interval
func (self *DriveFiler) files(ctx context.Context, op, p string) (*driveTree, error) {
	if tree := self.liveTree(); tree != nil {
		return tree, nil
	}
	tree, err := self.tree.get(ctx, "", func() (*driveTree, error) {
		root, err := self.client.Root(ctx)
		if err != nil {
			return nil, err
		}
		files, err := self.client.Files(ctx)
		if err != nil {
			return nil, err
		}
		return newDriveTree(root, files), nil
	})
	if err != nil {
		return nil, self.apiError(op, p, err)
	}
	return tree, nil
}

// stat returns the root, a folder, or a file, and the file, or folder, of
// Google's, which the root has none of. A path which ends with "/" is a
// directory's, and isn't a file's.
func (self *DriveFiler) stat(ctx context.Context, op, p string) (*schema.Object, *httpclient.DriveFile, error) {
	tree, err := self.files(ctx, op, p)
	if err != nil {
		return nil, nil, err
	}
	return self.lookup(tree, op, p)
}

// lookup returns the root, a folder, or a file, of the files, as stat does
func (self *DriveFiler) lookup(tree *driveTree, op, p string) (*schema.Object, *httpclient.DriveFile, error) {
	key := path.Clean("/" + p)
	file, exists := tree.files[key]
	switch {
	case key == "/":
		return self.object(tree, key), nil, nil
	case !exists, file.MimeType != httpclient.DriveFolderType && strings.HasSuffix(p, "/"):
		return nil, nil, &fs.PathError{Op: op, Path: p, Err: fs.ErrNotExist}
	}
	return self.object(tree, key), file, nil
}

// export reads a Google Doc, Sheet or Slides, of the version it was listed
// with, from the volume's cache, where it's kept for driveExportTTL, so one
// which hasn't changed isn't exported again, or, when it isn't there, as
// it's exported into the cache, of at most driveExportMax, whose version is
// checked before, and after, as an export can't be of a version, so its
// content is the object's, or ErrPreconditionFailed, when it's changed, and
// the files are got again, when it isn't kept. It's read of the cache, so
// it can seek, and be read at an offset, without all of it in memory.
func (self *DriveFiler) export(ctx context.Context, p string, file *httpclient.DriveFile, export driveExport, fn func(io.Reader) error) error {
	check := func() error {
		version, err := self.client.Version(ctx, file.Id)
		if err != nil {
			return self.apiError("read", p, err)
		} else if version != file.Version {
			self.tree.clear()
			return gofiler.ErrPreconditionFailed.Withf("%q changed since it was listed", p)
		}
		return nil
	}
	entry, err := self.cache.GetOrCreate(ctx, driveExportKey+file.Id, versionTag(file), driveExportTTL, func(w io.Writer) (gofiler.CacheMeta, error) {
		if err := check(); err != nil {
			return gofiler.CacheMeta{}, err
		}
		var n int64
		if err := self.client.Export(ctx, file.Id, export.contentType, func(r io.Reader) error {
			var err error
			n, err = io.Copy(w, io.LimitReader(r, driveExportMax+1))
			return err
		}); err != nil {
			return gofiler.CacheMeta{}, self.apiError("read", p, err)
		} else if n > driveExportMax {
			return gofiler.CacheMeta{}, gofiler.ErrForbidden.Withf("%q is exported as more than Google exports", p)
		} else if err := check(); err != nil {
			return gofiler.CacheMeta{}, err
		}
		return gofiler.CacheMeta{}, nil
	})
	if err != nil {
		return err
	}
	defer entry.Close()
	return fn(&cachedReader{ctx: ctx, entry: entry})
}

// object returns the root, a folder, which is a directory, of how many
// files, and subfolders, are in it, or a file: one which is Google's own, such as a Doc,
// has no size, nor checksum, until it's exported, so its entity tag is its
// version, and another's is its checksum, with its metadata which its
// content can't have
func (self *DriveFiler) object(tree *driveTree, p string) *schema.Object {
	file := tree.files[p]
	if p == "/" || file.MimeType == httpclient.DriveFolderType {
		var modtime time.Time
		if file != nil {
			modtime = file.ModifiedTime
		}
		files, subfolders := 0, 0
		for _, child := range tree.children[p] {
			if f := tree.files[child]; f != nil && f.MimeType == httpclient.DriveFolderType {
				subfolders++
			} else {
				files++
			}
		}
		dir := dirObject(self.name, p, files, subfolders, modtime)
		if file != nil {
			dir.Meta = append(dir.Meta, driveMeta(file)...)
		}
		return dir
	}
	object := &schema.Object{
		ObjectKey:  schema.ObjectKey{Name: self.name, Path: p},
		ObjectMeta: schema.ObjectMeta{ContentType: file.MimeType, Meta: driveMeta(file)},
		ObjectAttr: schema.ObjectAttr{ModTime: file.ModifiedTime},
	}
	if export, exists := driveExports[file.MimeType]; exists {
		object.ContentType = export.contentType
		if etag := versionTag(file); etag != "" {
			object.ETag = types.Ptr(etag)
		}
		return object
	}
	if size, err := strconv.ParseInt(file.Size, 10, 64); err == nil {
		object.Size = types.Ptr(size)
	}
	if file.MD5Checksum != "" {
		object.ETag = types.Ptr(file.MD5Checksum)
	} else if etag := versionTag(file); etag != "" {
		object.ETag = types.Ptr(etag)
	}
	return object
}

// versionTag returns the entity tag of a file of its version, which is of
// its id too, as each file's versions are its own, so another file at its
// path, of the same version, isn't the same, or empty, without a version
func versionTag(file *httpclient.DriveFile) string {
	if file.Version == "" {
		return ""
	}
	return file.Id + "/" + file.Version
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// newDriveTree returns the files, and folders, beneath the root, of its id,
// by their paths, of the names of their folders, and their own names,
// with an exported file's extension, and the paths in each folder. A file
// in more than one folder, which Drive no longer allows, is in its first.
// A "/" in a name is "_", and names which are the same in a folder, which
// Drive allows, have the start of their ids, such as "Report (1a2b3c).pdf",
// as much as makes the name another's, which is the same however the files
// are listed. Files which are hidden, of a name which starts with ".",
// such as those a Mac's sync uploads, or which are Google's own, but not a
// Doc, Sheet or Slides, such as forms, and shortcuts, or which aren't
// beneath the root, aren't in it, nor is what's in a folder which isn't.
func newDriveTree(root string, files []httpclient.DriveFile) *driveTree {
	tree := &driveTree{root: root, files: make(map[string]*httpclient.DriveFile), children: make(map[string][]string)}

	// The files in each folder, of its id
	in := make(map[string][]*httpclient.DriveFile)
	for i := range files {
		file := &files[i]
		if strings.HasPrefix(strings.TrimSpace(file.Name), ".") || len(file.Parents) == 0 {
			continue
		} else if _, exports := driveExports[file.MimeType]; !exports && file.MimeType != httpclient.DriveFolderType && strings.HasPrefix(file.MimeType, httpclient.DriveAppsPrefix) {
			continue
		}
		in[file.Parents[0]] = append(in[file.Parents[0]], file)
	}

	// Walk from the root, each folder once, so a folder which is its own
	// ancestor isn't walked again
	visited := map[string]bool{root: true}
	var walk func(id, dir string)
	walk = func(id, dir string) {
		files := slices.SortedFunc(slices.Values(in[id]), func(a, b *httpclient.DriveFile) int { return strings.Compare(a.Id, b.Id) })
		for i, name := range driveNames(files) {
			file := files[i]
			p := path.Join(dir, name)
			if file.MimeType == httpclient.DriveFolderType {
				if visited[file.Id] {
					continue
				}
				visited[file.Id] = true
				tree.files[p] = file
				tree.children[dir] = append(tree.children[dir], p)
				walk(file.Id, p)
			} else {
				tree.files[p] = file
				tree.children[dir] = append(tree.children[dir], p)
			}
		}
	}
	walk(root, "/")
	return tree
}

// driveNames returns the names of the files of a folder, in the order of
// their ids, which are unique, ignoring case, as a file system may: a name
// which more than one has is of the start of each one's id, as much as
// makes it another's
func driveNames(files []*httpclient.DriveFile) []string {
	names := make([]string, len(files))
	counts := make(map[string]int, len(files))
	for i, file := range files {
		names[i] = driveName(file)
		counts[strings.ToLower(names[i])]++
	}
	taken := make(map[string]bool, len(files))
	for _, name := range names {
		if counts[strings.ToLower(name)] == 1 {
			taken[strings.ToLower(name)] = true
		}
	}
	for i, name := range names {
		if counts[strings.ToLower(name)] == 1 {
			continue
		}
		id, ext := files[i].Id, path.Ext(name)
		for n := min(6, len(id)); ; n++ {
			suffix := id[:min(n, len(id))]
			if n > len(id) {
				suffix += "-" + strconv.Itoa(n-len(id))
			}
			if unique := strings.TrimSuffix(name, ext) + " (" + suffix + ")" + ext; !taken[strings.ToLower(unique)] {
				names[i] = unique
				break
			}
		}
		taken[strings.ToLower(names[i])] = true
	}
	return names
}

// driveName returns the name of a file, with "/" as "_", or its id, when
// it has none, with the extension it's exported with, when it hasn't it
func driveName(file *httpclient.DriveFile) string {
	name := strings.ReplaceAll(strings.TrimSpace(file.Name), "/", "_")
	if name == "" || name == "." || name == ".." {
		name = file.Id
	}
	if export, exists := driveExports[file.MimeType]; exists && !strings.EqualFold(path.Ext(name), export.ext) {
		name += export.ext
	}
	return name
}

// driveMeta returns the metadata of a file, or folder, which its content
// can't have: its id, its link in Google Drive, the email of who owns it,
// and, of one which is Google's own, its type, such as document
func driveMeta(file *httpclient.DriveFile) []schema.Meta {
	var result []schema.Meta
	add := func(key, value string) {
		if value != "" {
			result = append(result, schema.Meta{Namespace: DriveNamespace, Key: key, Value: json.RawMessage(quote(value))})
		}
	}
	add("id", file.Id)
	add("url", file.WebViewLink)
	if len(file.Owners) > 0 {
		add("owner", file.Owners[0].EmailAddress)
	}
	if _, exists := driveExports[file.MimeType]; exists {
		add("type", strings.TrimPrefix(file.MimeType, httpclient.DriveAppsPrefix))
	}
	return result
}

// streamReader reads a file's content, as it's received, until the context
// is cancelled, when it returns the context's error
type streamReader struct {
	ctx context.Context
	r   io.Reader
}

func (s *streamReader) Read(p []byte) (int, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	return s.r.Read(p)
}

// cachedReader reads an entry of the volume's cache, which can seek, and be
// read at an offset, until the context is cancelled, when it returns the
// context's error
type cachedReader struct {
	ctx   context.Context
	entry gofiler.CacheEntry
}

var _ io.ReadSeeker = (*cachedReader)(nil)
var _ io.ReaderAt = (*cachedReader)(nil)

func (c *cachedReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.entry.Read(p)
}

func (c *cachedReader) Seek(offset int64, whence int) (int64, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.entry.Seek(offset, whence)
}

func (c *cachedReader) ReadAt(p []byte, off int64) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.entry.ReadAt(p, off)
}
