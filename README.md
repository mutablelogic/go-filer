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
| `github.com/mutablelogic/go-filer/filer/google` | Backends of Google's services: Google Tasks and Google Contacts |
| `github.com/mutablelogic/go-filer/filer/google/httpclient` | Clients of Google's APIs, with go-auth's `auth/httpclient`, so a request which isn't authorized is an `AuthError` |
| `github.com/mutablelogic/go-filer/filer/indexer` | A filer which wraps a backend, and keeps an index of its folders in a `Store`, which follows its changes |
| `github.com/mutablelogic/go-filer/filer/indexer/memory` | An index store in memory |
| `github.com/mutablelogic/go-filer/filer/indexer/storetest` | The conformance tests every index store runs |

## Usage

`fs.New` returns a writable filer for a directory, and `fs.NewFS` returns a filer for any `fs.FS`, which is read-only unless it implements `filer.WriterFS`. Either can be made read-only with the `readonly` option, when creating and deleting objects is `filer.ErrForbidden`. The filer takes ownership of the filesystem, and closes it when it's closed. `filer.Filer` has no `Close`: a filer which holds resources, as these do, implements `io.Closer`, which its owner calls, rather than the code it's given to.

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

The options of the filesystem backend are passed to `New` and `NewFS` as `url.Values`, and are the query parameters of the filer's URL, `fs://<name>`, which has each of them, defaults too, so it says how the filer is set up. They're followed by the options of a backend, `gofiler.Opts`, which every backend is created with, of the registry's `WithTracer`, `WithCredentials` and `WithClientOpts`, whose `Tracer` traces each operation, such as `fs.ListObjects`, with the volume and path, when it's set. Each option can be repeated, and setting one replaces its defaults, so an empty value, such as `skip=`, sets none.

| Option | Description | Default |
| -------- | ------------- | --------- |
| `skip` | A regular expression of the names of files and directories which aren't objects, nor is anything in a directory with the name | Hidden files, starting with `.`, and those which operating systems and applications leave alongside content, such as `Thumbs.db`, `desktop.ini`, `Icon\r`, Microsoft Office owner files (`~$*`) and backup files (`*~`) |
| `package` | A regular expression of the names of directories which are packages | `(?i)\.bundle$` and `(?i)\.sparsebundle$` |
| `readonly` | Whether objects can only be read, and not created or deleted, even when the filesystem is writable: `true`, `false`, or empty, which is `true`. A filesystem which isn't writable is read-only whatever it's set to | `false` |

An unknown option, an expression which doesn't compile, or a `readonly` which isn't a boolean, is `filer.ErrBadParameter`.

## Google Tasks

`google.NewTaskFiler` returns a filer of a person's Google Tasks:

* **Lists** are directories, named by their ids, such as `/MTA3NjI`, with their titles as metadata, as `task:title` and `dc:title`, and `folder:children` and `folder:children-count`, whether they have tasks, and how many. The API doesn't count them, so they're read: one request a list, for each 100 tasks.
* **Tasks** are iCalendar files, `text/calendar; component=VTODO`, named by their ids, such as `/MTA3NjI/Q2xvdWQ.ics`, so a path stays the same when a task is renamed.
* **Content:** one `VTODO`, which calendar and task apps can import: the task's id as its `UID`, its title, notes, due date, status, completion, parent as `RELATED-TO`, URL, and update time.
* **Metadata:** `task:title`, `status`, `due`, `completed` and `parent`, and what iCalendar can't have, `task:hidden`, of a completed task cleared from its list, and `task:links`, such as to the email a task was created from.
* **Writing:** a volume with `readonly=false`, which asks for write access when its user signs in, writes a task as an iCalendar of one `VTODO`:
  * **Creating:** `PUT /LIST/~new.ics` creates a task, beneath its `RELATED-TO`, when it has one, and returns it at its path, of the id Google gives it, as `~` is never in one.
  * **Updating:** `PUT` of a task's path replaces its title, notes, due date and status, clearing those the `VTODO` doesn't have, and moves it, when its `RELATED-TO` changes. Its other fields, such as its links, are kept. Its `UID`, when it has one, must be the task's id, and a task which doesn't exist isn't found, as its id can't be chosen.
  * **Deleting:** a task, with its subtasks, as Google does, or a list, with its tasks, with `recursive`, which returns the list too, after its tasks, as it's an object of its own.
  * **Watching:** what the volume writes, or deletes, is sent to its watches straight away, rather than when they refresh.

To set it up, with the `filer` command, store the OAuth client, a desktop app's, which Google's console downloads, as the server's credential, and create a volume, which signs in with Google, in the browser:

```bash
filer credential-set GOOGLE_CLIENT --json < client_secret.json
filer volume-create-gtasks tasks
```

In the registry, its URL is `gtasks://<name>`, such as `gtasks://tasks?readonly=false`, and its sign-in is of the registry's credentials (`filer.WithCredentials`):

```go
filer, err := google.NewTaskFiler(ctx, "tasks", url.Values{"readonly": {"true"}}, gofiler.Opts{Credentials: credentials, ClientOpts: clientopts})
```

* **`GOOGLE_CLIENT`:** the credential of the OAuth client, a desktop app's, as the JSON of its `client_id` and `client_secret`, or the file Google's console downloads for it.
* **`GOOGLE_TOKEN`:** the credential of the user's sign-in, a go-auth `Grant`, or another, of the `credential` parameter. It must be for `GOOGLE_CLIENT`'s client, and have the `tasks.readonly` or `tasks` scope, or `tasks`, when it isn't read-only. Its token is refreshed, with the client's secret, and stored again, when it expires.
* **Signing in:** when there's no sign-in, or it's for another client, or doesn't have the scopes, or is revoked, it's go-auth's `SignInError`, with how to sign in: Google's server, the client's ID, the scopes, and the credential. The HTTP API returns it as 401, with its `detail`, without the client's secret.
* **Options:** a backend's `gofiler.Opts`: its `Credentials`, of the sign-in, and its `ClientOpts`, go-client's, as they are, such as a user agent, a tracer, of `client.OptTracer`, or a trace.
* **`readonly`:** true by default. With `false`, tasks can be created, updated and deleted, and lists deleted, as under Writing above.
* **`quota`:** the most requests a minute, spaced evenly, which is 120 by default, or 0 for no limit.
* **`refresh_interval`:** how often `WatchObjects` polls for changes, which is a minute by default, or 0 not to watch.
* **`hidden`:** include tasks which are hidden, as they're completed, and cleared from their list, which are left out unless it's `true`.

## Google Contacts

`google.NewContactFiler` returns a filer of a person's Google Contacts, of the People API, with the same Google sign-in as Google Tasks, which asks for the `contacts.readonly` scope, keeping the scopes it has, so one sign-in can have both:

* **Contacts** are vCard files, `text/vcard`, at the root, named by their ids, such as `/c123.vcf`, with `dc:title` and `contact:name`, of their names.
* **Content:** written with go-metadata's `pkg/vcard`: names, nicknames, emails, phones, URLs, addresses, organisations, titles, birthday, notes, a photo, as a link, but not Google's default one, of initials, and when it was updated, as `REV`. A primary value is preferred.
* **No birthdays as objects**, nor calendars, only contacts.
* **Options:** `quota`, `refresh_interval` and `credential`, as Google Tasks has. It's read-only, so far: `readonly=false` isn't implemented.
* **Watching** polls the contacts' entity tags, at the refresh interval.

In the registry, its URL is `gcontacts://<name>`, and the `filer` command creates one with `volume-create-gcontacts`, signing in with Google, in the browser, when it needs to.

## Indexer

`indexer.New(name, backend, store)` returns a filer of a backend whose folders are indexed, in a `Store`, so its folder tree is listed from the index rather than the backend, which can be slow, such as a bucket or a network mount.

* **`Run(ctx)`** indexes it, until the context is cancelled. It watches the backend, and scans it once it's watching, a folder at a time, so a change during the scan isn't missed.
* **Following changes:** a rescan event scans that folder again. A folder which is created, or deleted, is indexed, or removed, with what's beneath it.
* **Backends which can't be watched**, such as a bucket without SeaweedFS, are scanned again at the rescan interval, 15 minutes by default (`WithRescanInterval`).
* **A watch which fails** is watched, and scanned, again after a while, which doubles each time, up to a minute. Why it failed is in the state.
* **`ListFolders`** lists a folder's folders, or every folder beneath it, with `Recursive`, in the order a backend lists them. **`GetObject`** of a folder is the index's, with `folder:children` and `folder:children-count`. Until the first scan finishes, both are the backend's.
* **Writes and deletes** through it update the index as they're made.
* **Everything else**, such as files, is the backend's. Files are indexed later.
* **`State`** is the index's status (never indexed, scanning, or ready), how many folders the scan has indexed, when it started and finished, and why it last failed.
* **Stores:** `indexer/memory` so far. SQLite and Postgres stores come next, and every store passes `indexer/storetest`.

In the registry, `filer.WithIndexerStore(store)` lets volumes be indexed, of their `index` option:

* **`index=folders`** indexes a volume's folders, and `index=none`, or no option, doesn't. The registry takes it off the URL before the backend sees it.
* **Running:** the registry runs each indexer until the volume is removed. Its watchers get the backend's changes through the indexer, so the backend is watched once.
* **`Update`** turns it on, or off, which removes the index, and keeps it through other changes.
* **`Del`** removes the volume's index.
* **The volume** has `index`, with its mode, status, folders scanned, and last error, and its URL has `index=folders`, or `index=none`, when it isn't indexed.
* **Listing folders:** `ListObjects` with `Type` `inode/directory`, `inode`, or `directory`, which is `GET /object/{name}?type=directory`, or `filer objects --type directory`, lists an indexed volume's folders from its index, once it's ready. A listing of a type is filtered for every volume.
* **The `filer` server** keeps its indexes in memory, so they're scanned again when it starts.

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

`WatchObjects` sends each change to an object to a channel, until the context is cancelled or the filer is closed. The first change is a rescan of the root, once the objects are watched, so a caller which lists them then misses no change. A change is an object which is created, updated or deleted, or a directory which should be rescanned, as its changes may have been missed. Changes to files which aren't objects aren't sent, and a change within a package is an update to the package.

Directories are watched with the `fsevents` package, using the efficient mechanism of each platform:

| Platform | Mechanism |
| ---------- | ----------- |
| macOS | FSEvents, which needs cgo |
| Linux | inotify |
| Windows | `ReadDirectoryChangesW` |

On other platforms, and on macOS without cgo, and for a filer of a filesystem which isn't a directory, `WatchObjects` returns `filer.ErrNotImplemented`.

The registry of volumes is watched by its `Run`, which watches each volume when it's added, or updated, until it's removed, and publishes their changes to go-server's broadcaster, as changes of the `filer` schema, whose table is the volume, and whose row is the object's path, until its context is cancelled, when it closes the registry: it removes and closes the volumes, cancelling the calls to `With` which are using them, and the registry can't be used again. It's closed only by `Run`, so it should be run while the registry is used, as the server does. The broadcaster is set with `filer.WithBroadcaster`, and its owner runs and serves it, such as a server, which can share it with its other modules. The server command creates one, served as `/changes`, unless it's given one. Without a broadcaster, the changes aren't published. `Registry.WatchObjects` watches the broadcaster, and converts the changes back to events. Without it, the volumes aren't watched. It holds a change to an object until the object is quiet for 250ms (`filer.WithDebounce`), or at most two seconds, and merges its changes, so an object which changes many times, such as a file which is written in parts, is one change, one which is created and deleted isn't one, and one which is deleted and created again is updated. A watcher isn't held up by the others, so when it's slow to receive the changes, the broadcaster drops a volume's changes, and sends it a rescan of the volume, once it can receive it, which is an `update` without a `row`. With a meter (`filer.WithMeter`), `Run` counts the changes in `filer.events`, a counter whose attributes are the worker, `filer.worker`, which is the hostname, or `filer.WithWorker`'s name, the volume, `filer.volume`, and the change, `filer.event.type`, such as `created`, which the meter's provider pushes to the metrics server.

## Modification Time

The modification time of an object is stored in the `x-amz-meta-mtime` metadata of an S3 object, which is used in preference to `LastModified`. `LastModified` is the time the object was uploaded, and cannot be set, so it is only used when the metadata is not set. Its value is seconds since the Unix epoch, with an optional fraction of a second, such as `1764569708.5`, as rclone and s3fs write it, so the times they write are read too. It isn't part of an object's user metadata, so `mtime` can't be a key of it, outside a namespace.

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
