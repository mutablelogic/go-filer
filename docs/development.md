# Development

Building and testing the filer, its Go packages, and using them as a library. See [architecture.md](architecture.md) for how they fit together, and [backends.md](backends.md) for each backend.

## Packages

| Package | Description |
| --------- | ------------- |
| `github.com/mutablelogic/go-filer` | The contract of backends: the `Filer` interface, the errors, and `Opts`, which every backend is created with |
| `github.com/mutablelogic/go-filer/schema` | The objects, requests and events |
| `github.com/mutablelogic/go-filer/backend/fs` | The filesystem backend, for any `fs.FS`, and writable for a directory, with the `WriterFS` interface of a writable filesystem |
| `github.com/mutablelogic/go-filer/backend/fs/fsevents` | Watches a directory for changes, on macOS, Linux and Windows. It has no dependencies on the other packages |
| `github.com/mutablelogic/go-filer/backend/s3` | The S3 backend, watched through a SeaweedFS filer when it's one |
| `github.com/mutablelogic/go-filer/backend/google` | Backends of Google's services: Google Tasks, Contacts, Calendar and Drive |
| `github.com/mutablelogic/go-filer/backend/rss` | The backend of an RSS feed, read with go-rss. It isn't built into the registry; the `filer` server adds it with `registry.WithBackend(rss.Backend())` |
| `github.com/mutablelogic/go-filer/backend/webcal` | The backend of an iCalendar feed, a calendar subscription, read with go-metadata's `ical` |
| `github.com/mutablelogic/go-filer/backend/google/httpclient` | Clients of Google's APIs, with go-auth's `auth/httpclient`, so a request which isn't authorized is an `AuthError` |
| `github.com/mutablelogic/go-filer/store` | The `Store` of volumes and their indexes, and its options: `WithTracer` and `WithBroadcaster` |
| `github.com/mutablelogic/go-filer/store/sqlite` | A store in a SQLite database, in a file, or in memory, and the `CacheStore` of a volume |
| `github.com/mutablelogic/go-filer/store/test` | The conformance tests every store runs |
| `github.com/mutablelogic/go-filer/indexer` | A filer which wraps a backend, and keeps an index of it in a `Store`, which follows its changes |
| `github.com/mutablelogic/go-filer/registry` | The registry of the volumes' backends, which are created from URLs, such as `file://media/mnt/media`, and watched |
| `github.com/mutablelogic/go-filer/registry/httphandler` | The registry's HTTP API |
| `github.com/mutablelogic/go-filer/registry/httpclient` | A client of the registry's HTTP API |
| `github.com/mutablelogic/go-filer/registry/cmd` | The `filer` command's subcommands |
| `github.com/mutablelogic/go-filer/metadata` | The task which enriches files with go-metadata's metadata |
| `github.com/mutablelogic/go-filer/metadata/cmd` | The enrichment flags of a server, and `WithMetadataEnrichment`, its registry option |
| `github.com/mutablelogic/go-filer/frontend` | The web UI, which the server embeds with the `frontend` tag (see [frontend/README.md](../frontend/README.md)) |
| `github.com/mutablelogic/go-filer/frontend/cmd` | Serves the web UI, and `/config.json`, and its `--[no-]ui` flag |

## Building

`make` builds the server with cgo, which the macOS folder watcher needs, and which go-metadata's handlers of audio, video, and photos' EXIF, HEIF and RAW need too. They link go-media's libraries: FFmpeg 8, libexif, libheif, LibRaw, Chromaprint and libfvad.

* `make go-media` downloads the libraries from the release of the go-media version in `go.mod`, into `build/go-media`. The builds and tests with cgo run it first.
* `GO_MEDIA_PREFIX` uses another build of them instead, such as `build/install` in a clone of go-media, or `/opt/go-media` in its dev image. `GO_MEDIA_LICENSE=gpl` uses the GPL build of FFmpeg, rather than the LGPL one.
* On macOS, the libraries link some Homebrew packages, listed in `build/go-media/Brewfile`: `brew bundle --file build/go-media/Brewfile` installs them.
* `make` builds the web UI first, which the server embeds and serves at `/`. It needs npm, and `GITHUB_TOKEN` with `read:packages`, such as `$(gh auth token)`. See [frontend/README.md](../frontend/README.md).
* `make CGO_ENABLED=0` builds without cgo, and so without the libraries. It reads less metadata, and can't watch volumes on macOS, which are polled instead.

## Testing

The packages are vetted and tested with `make`, with `GOWORK=off`:

| Target | Description |
| -------- | ------------- |
| `make test` | Vet, then run the tests, and their coverage, in `build/coverage.txt` |
| `make vet` | Vet the packages for macOS, Linux, Windows and FreeBSD |
| `make race-test` | Run the tests with the race detector, which needs cgo |
| `make tidy` | Tidy the module |
| `make frontend-test` | Type check the web UI, and run its tests |
| `make clean` | Remove `build/`, and the web UI's builds |

The test workflow vets the packages, runs the race tests on Linux, macOS and Windows, and tests and builds the web UI, for pull requests and pushes to `main`.

`make` builds the `filer` server with the store of its indexes chosen by `INDEXER_BACKEND`, as the build tag `indexer_<backend>`:

* **`sqlite`**, the default, is a SQLite database, `index.sqlite`, in the server's folder of the user's data, such as `~/Library/Application Support/filer/index.sqlite`. An index of an earlier version, `<name>-index.db` in the user's cache folder, is moved there when the server starts, unless there's one there already.
* **`memory`** is a SQLite database in memory, so volumes aren't kept, and are scanned again, when the server starts.
* **`pg`** is a Postgres database, of go-pg's flags, `--pg.url` and `--pg.password`, or `PG_URL` and `PG_PASSWORD`.

The Postgres store isn't implemented yet (#72), so its volumes' indexes fail with `ErrNotImplemented`, until it is.

## Usage

`fs.New` returns a writable filer for a directory, and `fs.NewFS` returns a filer for any `fs.FS`, which is read-only unless it implements `fs.WriterFS`. Either can be made read-only with the `readonly` option, when creating and deleting objects is `filer.ErrForbidden`. The filer takes ownership of the filesystem, and closes it when it's closed. `filer.Filer` has no `Close`: a filer which holds resources, as these do, implements `io.Closer`, which its owner calls, rather than the code it's given to.

```go
import (
    "context"
    "fmt"
    "io"
    "os"

    gofiler "github.com/mutablelogic/go-filer"
    filerfs "github.com/mutablelogic/go-filer/backend/fs"
    schema "github.com/mutablelogic/go-filer/schema"
)

func main() {
    ctx := context.Background()

    // A writable filer for the directory, named "media"
    filer, err := filerfs.New("media", "/mnt/media", nil, gofiler.Opts{})
    if err != nil {
        panic(err)
    }
    defer filer.Close()

    // Read an object, within the function returned
    read, obj, err := filer.ReadObject(ctx, schema.GetObjectRequest{
        ObjectKey: schema.ObjectKey{Path: "/music/song.mp3"},
    })
    if err != nil {
        panic(err)
    }
    fmt.Println(obj.ContentType, *obj.Size)
    if err := read(ctx, func(r io.Reader) error {
        _, err := io.Copy(os.Stdout, r)
        return err
    }); err != nil {
        panic(err)
    }
}
```

`ReadObject` returns the object, and a function which reads its content: the file is opened when the function is called, the content is read within the function passed to it, and the file is closed when that returns. Nothing is opened when the function isn't called. The file isn't read, and `filer.ErrPreconditionFailed` is returned, when it changed after the object was returned.

Closing the filer stops listings, reads, writes and watches which are in progress, which return `fs.ErrClosed`. The list iterator can be used from more than one goroutine.

## Object Model

- An object is a file, identified by its path within the volume, which starts with `/`. The root of the volume is `/`.
- A directory exists when an object exists beneath it. Directories have no size or modification time, and their content type is `inode/directory`.
- The key of a file is its path, and the key of a directory is its path followed by `/`. The root's key is `/`, and every key starts with `/`. Listings are in the byte order of keys, as in S3.
- A bucket can have a file `/a` and a directory `/a/`. They're two objects, with different keys, and the index keeps both.
- Empty parent directories are removed by the filesystem backend when an object is deleted, as an S3 prefix with no objects beneath it does not exist.
- Files which are not regular files, such as sockets, named pipes and devices, are not objects.
- Symbolic links are not objects, as S3 has none, nor are files beneath a linked directory. An object can't be created over a link or beneath one. Deleting a directory deletes the links in it, not what they lead to.
- There are no renames, as in S3. A file renamed on disk is a delete and a create.
- Files and directories whose names are skipped (see [the filesystem's options](backends.md#options)) are not objects, and can't be created.
- A package, such as a macOS sparse disk image, is a directory which is an object, as Finder shows it as one file. It has a modification time, and the content type of its extension, but no size, and can't be read. The files in it are not objects, and are deleted with it.
- Paths which aren't valid `fs.FS` paths, with `//`, `.` or `..` elements, or a trailing `/`, are `fs.ErrInvalid`.
- The content type of an object is from its extension, as sniffing the content would mean reading it.

## Errors

Errors are converted to HTTP errors by `filer.HTTPErr`. The filesystem backend returns `filer` errors, and `fs` errors, such as `fs.ErrNotExist`:

| Error | HTTP status |
| ------- | ------------- |
| `fs.ErrNotExist`, `filer.ErrNotFound` | 404 |
| `fs.ErrInvalid`, `filer.ErrBadParameter` | 400 |
| `fs.ErrExist`, `filer.ErrConflict` | 409 |
| `fs.ErrPermission`, `filer.ErrForbidden` | 403 |
| `fs.ErrClosed`, `filer.ErrServiceUnavailable` | 503 |
| `filer.ErrNotModified` | 304 |
| `filer.ErrPreconditionFailed` | 412 |
| `filer.ErrUnprocessableContent` | 422 |
| `filer.ErrNotImplemented` | 501 |

The S3 backend converts S3 errors in the same way:

| S3 error | Error | HTTP status |
| ---------- | ------- | ------------- |
| `NoSuchKey`, `NoSuchBucket` | `fs.ErrNotExist` | 404 |
| `AccessDenied` | `filer.ErrForbidden` | 403 |
| `SlowDown`, `ServiceUnavailable` | `filer.ErrServiceUnavailable` | 503 |
| `PreconditionFailed` | `filer.ErrPreconditionFailed` | 412 |

The registry's object calls of a volume which isn't mounted return:

* `filer.ErrNotFound` (404) when the volume doesn't exist.
* `filer.ErrServiceUnavailable` (503) when it exists but isn't mounted:
  * it's offline, with why it can't be mounted, which is tried again with backoff;
  * it's unmounted (disabled);
  * it's still being mounted.
