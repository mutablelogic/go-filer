# go-filer

[![Go Reference](https://pkg.go.dev/badge/github.com/mutablelogic/go-filer.svg)](https://pkg.go.dev/github.com/mutablelogic/go-filer)
[![Test](https://github.com/mutablelogic/go-filer/actions/workflows/test.yaml/badge.svg)](https://github.com/mutablelogic/go-filer/actions/workflows/test.yaml)

A filer stores the objects in a volume. Each backend implements `filer.Filer`, and must behave the same way, so that a volume can be stored on a filesystem or in S3 without clients seeing a difference. The semantics follow S3, and the filesystem backend emulates S3 where they differ.

## Packages

| Package | Description |
| --------- | ------------- |
| `github.com/mutablelogic/go-filer` | The `Filer` interface, the `WriterFS` interface of a writable filesystem, and the errors |
| `github.com/mutablelogic/go-filer/filer` | The registry of the volumes' backends, which are created from URLs, such as `file://media/mnt/media`, and watched |
| `github.com/mutablelogic/go-filer/filer/schema` | The objects, requests and events |
| `github.com/mutablelogic/go-filer/filer/fs` | The filesystem backend, for any `fs.FS`, and writable for a directory |
| `github.com/mutablelogic/go-filer/filer/fs/fsevents` | Watches a directory for changes, on macOS, Linux and Windows. It has no dependencies on the other packages |

## Usage

`fs.New` returns a writable filer for a directory, and `fs.NewFS` returns a filer for any `fs.FS`, which is read-only unless it implements `filer.WriterFS`. Either can be made read-only with the `readonly` option, when creating and deleting objects is `filer.ErrForbidden`. The filer takes ownership of the filesystem, and closes it when it's closed.

```go
import (
    "context"
    "fmt"
    "io"
    "os"

    gofiler "github.com/mutablelogic/go-filer"
    filerfs "github.com/mutablelogic/go-filer/filer/fs"
    schema "github.com/mutablelogic/go-filer/filer/schema"
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

### Options

The options of the filesystem backend are passed to `New` and `NewFS` as `url.Values`, and are the query parameters of the filer's URL, `fs://<name>`, when they aren't the defaults. They're followed by the options of a backend, `gofiler.Opts`, whose `Tracer` traces each operation, such as `fs.ListObjects`, with the volume and path, when it's set. Each option can be repeated, and setting one replaces its defaults, so an empty value, such as `skip=`, sets none.

| Option | Description | Default |
| -------- | ------------- | --------- |
| `skip` | A regular expression of the names of files and directories which aren't objects, nor is anything in a directory with the name | Hidden files, starting with `.`, and those which operating systems and applications leave alongside content, such as `Thumbs.db`, `desktop.ini`, `Icon\r`, Microsoft Office owner files (`~$*`) and backup files (`*~`) |
| `package` | A regular expression of the names of directories which are packages | `(?i)\.bundle$` and `(?i)\.sparsebundle$` |
| `readonly` | Whether objects can only be read, and not created or deleted, even when the filesystem is writable: `true`, `false`, or empty, which is `true`. A filesystem which isn't writable is read-only whatever it's set to | `false` |

An unknown option, an expression which doesn't compile, or a `readonly` which isn't a boolean, is `filer.ErrBadParameter`.

## Object Model

- An object is a file, identified by its path within the volume, which starts with `/`. The root of the volume is `/`.
- A directory exists when an object exists beneath it. Directories have no size or modification time, and their content type is `inode/directory`.
- The key of a file is its path, and the key of a directory is its path followed by `/`. Listings are in the byte order of keys, as in S3.
- Empty parent directories are removed by the filesystem backend when an object is deleted, as an S3 prefix with no objects beneath it does not exist.
- Files which are not regular files, such as sockets, named pipes and devices, are not objects, nor are symbolic links which are broken, or lead outside the volume.
- Files and directories whose names are skipped (see `skip`) are not objects, and can't be created.
- A package, such as a macOS sparse disk image, is a directory which is an object, as Finder shows it as one file. It has a modification time, and the content type of its extension, but no size, and can't be read. The files in it are not objects, and are deleted with it.
- Paths which aren't valid `fs.FS` paths, with `//`, `.` or `..` elements, or a trailing `/`, are `fs.ErrInvalid`.
- The content type of an object is from its extension, as sniffing the content would mean reading it.

## Operations

| Operation | Filesystem | S3 |
| ----------- | ------------ | ---- |
| `Clone` with options | Open the directory again, at its path | Create a client again, and check the bucket with `HeadBucket`, with the credentials, which the URL doesn't have |
| `GetObject` of a file | `stat` | `HeadObject` |
| `GetObject` of a directory | `stat` | `HeadObject` returns 404, then `ListObjectsV2` with `Prefix="dir/"` and `MaxKeys=1` to check the directory exists |
| `GetObject` with `IfModifiedSince` or `IfNoneMatch` | `filer.ErrNotModified` when it hasn't changed, to the second, or its ETag, or `*`, matches. `IfNoneMatch` is used in place of `IfModifiedSince` when it's set | `HeadObject` with `IfModifiedSince` or `IfNoneMatch`, which returns 304 |
| `ReadObject` | `open`, checking the file hasn't changed | `GetObject`, with `IfMatch` of the ETag returned |
| `CreateObject` | Write a hidden temporary file, then rename it over the object, so it's replaced atomically | `PutObject`, or a multipart upload |
| `CreateObject` with `IfNotExists` | Link the temporary file to the object, which fails when it exists | `PutObject` with `If-None-Match: *` |
| `CreateObject` with `IfExists` | Check the object exists, then rename over it | `PutObject` with `If-Match` of the ETag of the object |
| `ListObjects` | Walk the directory, in key order | `ListObjectsV2` with `Delimiter="/"`, or no delimiter when recursive. Directories are the `CommonPrefixes` |
| `ListObjects` with `StartAfter` | Seek within each directory | `StartAfter`, dropping a `CommonPrefix` equal to it |
| `ListObjects` of a missing path | 404 | An empty listing, so an extra request is needed to return 404 |
| `DeleteObjects` of a file | `remove` | `HeadObject` for the metadata, and to return 404 as `DeleteObject` succeeds for missing keys, then `DeleteObject` |
| `DeleteObjects` of a directory | `RemoveAll`. Returns the files deleted, including hidden files | List the prefix, then `DeleteObjects` in batches of 1000. Returns the keys deleted |
| Remove empty parent directories | After each delete | Not needed, as empty prefixes do not exist |
| `WatchObjects` | Watch the directory (see Watching) | Subscribe to the changes to a SeaweedFS filer's metadata, with the `seaweedfs-filer-grpc` option, and otherwise `filer.ErrNotImplemented`, as S3's event notifications need a queue |

Object metadata isn't stored yet, so `CreateObject` with metadata is `filer.ErrNotImplemented`.

## Watching

`WatchObjects` sends each change to an object to a channel, until the context is cancelled or the filer is closed. A change is an object which is created, updated or deleted, or a directory which should be rescanned, as its changes may have been missed. Changes to files which aren't objects aren't sent, and a change within a package is an update to the package.

Directories are watched with the `fsevents` package, using the efficient mechanism of each platform:

| Platform | Mechanism |
| ---------- | ----------- |
| macOS | FSEvents, which needs cgo |
| Linux | inotify |
| Windows | `ReadDirectoryChangesW` |

On other platforms, and on macOS without cgo, and for a filer of a filesystem which isn't a directory, `WatchObjects` returns `filer.ErrNotImplemented`.

The registry of volumes is watched by its `Run`, which watches each volume when it's added, or updated, until it's removed, and sends their changes, with the names of their volumes, to the calls to its `WatchObjects`, until its context is cancelled, or the registry is closed, which ends them, so it should be run while the registry is used, as the server does. Without it, the volumes aren't watched. A call to `WatchObjects` isn't held up by the others, so when it's slow to receive the changes, a volume's changes are dropped, and it's sent a rescan of the volume, once it can receive it. With a meter (`filer.WithMeter`), `Run` counts the changes in `filer.events`, a counter whose attributes are the volume, `filer.volume`, and the change, `filer.event.type`, such as `created`, which the meter's provider pushes to the metrics server.

## Modification Time

The modification time of an object is stored in the `x-amz-meta-mtime` metadata of an S3 object, which is used in preference to `LastModified`. `LastModified` is the time the object was uploaded, and cannot be set, so it is only used when the metadata is not set.

`ListObjectsV2` does not return metadata, so a listing from S3 has the `LastModified` time, which can differ from the modification time returned by `GetObject`.

## Differences

These cannot be made identical, and are known differences between the backends:

- **Keys.** S3 allows keys which cannot be represented as paths, such as those containing `//`, `.` or `..` elements, `\` or `:`, or a trailing `/`, and a file and a directory with the same name, such as `a` and `a/b`. The S3 backend skips these keys in listings, so that they are not objects, in the same way as hidden files.
- **Directory markers.** Zero-byte objects whose keys end in `/` are created by the S3 console and some tools to represent directories. They are not objects, and are skipped in listings.
- **Atomicity.** Deleting a directory in S3 takes several requests, and can fail part of the way through. Deleting a directory on the filesystem is not atomic either.
- **ETag.** S3 returns an ETag for each object. The filesystem backend does not set it, so only `IfNoneMatch: *` matches.
- **Packages.** S3 has no packages, so a package's files are objects in S3.

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

## Development

The packages are vetted and tested with `make`, with `GOWORK=off`:

| Target | Description |
| -------- | ------------- |
| `make test` | Vet, then run the tests, and their coverage, in `build/coverage.txt` |
| `make vet` | Vet the packages for macOS, Linux, Windows and FreeBSD |
| `make race-test` | Run the tests with the race detector, which needs cgo |
| `make tidy` | Tidy the module |
| `make clean` | Remove `build/` |

The test workflow vets the packages, and runs the race tests on Linux, macOS and Windows, for pull requests and pushes to `main`.
