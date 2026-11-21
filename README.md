# go-filer

[![Go Reference](https://pkg.go.dev/badge/github.com/mutablelogic/go-filer.svg)](https://pkg.go.dev/github.com/mutablelogic/go-filer)
[![Test](https://github.com/mutablelogic/go-filer/actions/workflows/test.yaml/badge.svg)](https://github.com/mutablelogic/go-filer/actions/workflows/test.yaml)

A filer stores the objects in a volume. Each backend implements `filer.Filer`, and must behave the same way, so that a volume can be stored on a filesystem or in S3 without clients seeing a difference. The semantics follow S3, and the filesystem backend emulates S3 where they differ.

## Packages

| Package | Description |
| --------- | ------------- |
| `github.com/mutablelogic/go-filer` | The contract of backends: the `Filer` interface, the errors, and `Opts`, which every backend is created with |
| `github.com/mutablelogic/go-filer/schema` | The objects, requests and events |
| `github.com/mutablelogic/go-filer/backend/fs` | The filesystem backend, for any `fs.FS`, and writable for a directory, with the `WriterFS` interface of a writable filesystem |
| `github.com/mutablelogic/go-filer/backend/fs/fsevents` | Watches a directory for changes, on macOS, Linux and Windows. It has no dependencies on the other packages |
| `github.com/mutablelogic/go-filer/backend/s3` | The S3 backend, watched through a SeaweedFS filer when it's one |
| `github.com/mutablelogic/go-filer/backend/google` | Backends of Google's services: Google Tasks, Contacts, Calendar and Drive |
| `github.com/mutablelogic/go-filer/backend/rss` | The backend of an RSS feed, read with go-rss. It isn't built into the registry; the `filer` server adds it with `registry.WithBackend` |
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
| `github.com/mutablelogic/go-filer/frontend` | The web UI, which the server embeds with the `frontend` tag (see [frontend/README.md](frontend/README.md)) |
| `github.com/mutablelogic/go-filer/frontend/cmd` | Serves the web UI, and `/config.json`, and its `--[no-]ui` flag |

## Building

`make` builds the server with cgo, which the macOS folder watcher needs, and which go-metadata's handlers of audio, video, and photos' EXIF, HEIF and RAW need too. They link go-media's libraries: FFmpeg 8, libexif, libheif, LibRaw, Chromaprint and libfvad.

* `make go-media` downloads the libraries from the release of the go-media version in `go.mod`, into `build/go-media`. The builds and tests with cgo run it first.
* `GO_MEDIA_PREFIX` uses another build of them instead, such as `build/install` in a clone of go-media, or `/opt/go-media` in its dev image. `GO_MEDIA_LICENSE=gpl` uses the GPL build of FFmpeg, rather than the LGPL one.
* On macOS, the libraries link some Homebrew packages, listed in `build/go-media/Brewfile`: `brew bundle --file build/go-media/Brewfile` installs them.
* `make` builds the web UI first, which the server embeds and serves at `/`. It needs npm, and `GITHUB_TOKEN` with `read:packages`, such as `$(gh auth token)`. See [frontend/README.md](frontend/README.md).
* `make CGO_ENABLED=0` builds without cgo, and so without the libraries. It reads less metadata, and can't watch volumes on macOS, which are polled instead.

## Docker

Each release publishes an image, `ghcr.io/mutablelogic/filer`, for `linux/amd64` and `linux/arm64`, built with cgo, so it reads the metadata of audio, video and photos.

* **Data:** credentials and the index are kept in `/data/filer`, and volumes' caches in `/data/cache`, of a volume at `/data`.
* **Volumes:** mount the directories to index, such as `-v /mnt/media:/media:ro`, and add them as volumes of their paths in the container.
* **User:** the server runs as the `filer` user, whose `UID` and `GID` can be set, such as to the owner of the mounted directories, and which `/data` is made the owner of.
* **Port:** it listens on `8084`. It has no authentication yet, so publish it on localhost only, unless it's behind a proxy which signs people in.

```bash
docker run -d --name filer -p 127.0.0.1:8084:8084 \
  -v filer-data:/data -v /mnt/media:/media:ro \
  ghcr.io/mutablelogic/filer:latest
docker exec filer filer volume-create-fs media /media
```

## Backends

A volume's backend is chosen by its URL's scheme. See [docs/backends.md](docs/backends.md) for each one's layout, options, writing and watching.

| Backend | Scheme | CLI |
| --- | --- | --- |
| [Filesystem](docs/backends.md#filesystem) | `file` | `volume-create-fs` |
| [S3](docs/backends.md#s3) | `s3` | `volume-create-s3` |
| [Google Tasks](docs/backends.md#google-tasks) | `gtasks` | `volume-create-gtasks` |
| [Google Contacts](docs/backends.md#google-contacts) | `gcontacts` | `volume-create-gcontacts` |
| [Google Calendar](docs/backends.md#google-calendar) | `gcalendar` | `volume-create-gcalendar` |
| [Google Drive](docs/backends.md#google-drive) | `gdrive` | `volume-create-gdrive` |
| [RSS feeds](docs/backends.md#rss-feeds) | `rss` | `volume-create-rss` |

Google's backends sign in with an OAuth client, `GOOGLE_CLIENT` (see [Signing in](docs/backends.md#signing-in)).

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

## Actions

An action is something a volume's backend can do beyond reading and writing its objects, such as creating a task list. It maps onto an LLM tool.

* **`Filer.Actions()`** returns a backend's actions, or nil, such as for a filesystem. Each `gofiler.Action` has a `Name` (a tool name, such as `task_list`), `Title`, `Description`, `Input` (a JSON schema) and `Run(ctx, input)`, which returns the object it created or changed.
* **The registry:** `ListActions(ctx, volume)` and `CallAction(ctx, volume, name, input)`. Actions aren't available while a volume is unmounted or offline, which is `ErrServiceUnavailable`.
* **HTTP:** `GET /action` lists every volume's, `GET /action/{name}` one volume's, and `POST /action/{name}/{action}` calls one with its input as the body.
* **CLI:** `filer actions [VOLUME]` lists them, of every volume without one, and `filer action VOLUME NAME '<json>'` calls one, or `-` reads the input from stdin.
* **Google Tasks** has `task_list` on a writable volume: `{"op":"create","title":"Work"}`, `{"op":"update","path":"/<id>","title":"Office"}` or `{"op":"delete","path":"/<id>"}`.
* **Google Calendar** has `event` on a writable volume: `{"op":"create","title":"Lunch","start":"2026-11-25T12:30"}` creates an event in the primary calendar, or another with `"calendar":"<id>"`.
  * `start` and `end` are both days (`YYYY-MM-DD`, of whole days, the end being the day after the last) or both times (RFC 3339, or `YYYY-MM-DDTHH:MM` in `timezone`, or the calendar's zone).
  * Without `end`, it lasts a day, or an hour.
  * `update` of an event's path changes the fields given (`title`, `description`, `location`, `start`, `end`, `timezone`, `attendees`). A new `start` alone keeps its length. The path of one time a repeating event occurs changes that time alone.
  * `delete` deletes it.
  * Those who are invited aren't emailed. A repeating series can't be changed as a whole, only one occurrence at a time.
  * Watches are sent the change as it's made, not at the next sync.
* **Google Tasks** also has `task`: `{"op":"create","title":"Call"}` creates a task in the default list, or in a list with `"path":"/<list>"`. `update` changes the fields given (`title`, `notes`, `due`, `completed`, `parent`) of `/<list>/<id>.ics`, and `delete` deletes it.
* **Google Contacts** has `contact` on a writable volume: `{"op":"create","given_name":"Grace","emails":["grace@example.com"]}`, `update` of `/<id>.vcf` with the fields to change (`given_name`, `family_name`, `emails`, `phones`, `organization`, `job_title`, `birthday`, `notes`, `addresses`), or `delete`. A list replaces the contact's.

## Cache

A `store.CacheStore` keeps what a volume's backend gets from a service, by key, on disk, so it isn't held in memory or fetched again after a restart. Each volume has its own, which the registry passes to its backend's New function. Using it is up to the backend, which must work without any of its entries.

* **`Get(ctx, key)`** returns an entry, or `ErrNotFound` if there's none or it expired.
* **`Put(ctx, key, ttl, meta, r)`** stores the content as it's read, with its metadata, for the TTL (zero keeps it until it's replaced or deleted). The old entry is replaced only once all the new content is read, and kept if it can't be.
* **`GetOrCreate(ctx, key, version, ttl, valuefn)`** returns the entry if it's of the version, or else calls `valuefn`, which writes the content to an `io.Writer` and returns its metadata, and stores it as it's written. Concurrent calls for a key create it once.
* **`Delete(ctx, key)`** deletes a key, or every key under a prefix ending `/`.
* **Entries** can seek and `ReadAt`, have their `Meta()` (a version and small values), `Size()`, `Stored()` and `Expires()`, and are closed by the caller.
* **JSON:** `store.GetCacheJSON` and `store.PutCacheJSON` store a value.
* **SQLite:** `sqlite.NewCacheStore(ctx, path)` opens one volume's cache, of a file, or `sqlite.Memory`.
  * Content is stored in 256 KiB chunks, so only one chunk is in memory as it's written or read.
  * An entry that's replaced or deleted while it's read stays readable until it's closed, and is deleted then.
  * When it's opened, entries left half-written, or expired, are deleted, and their space reclaimed.
* **The registry** keeps each volume's cache in its cache directory (`registry.WithCacheDir`), or a temporary one that's removed when it closes.
  * A cache's file is `<volume>.<hash of its identity>.sqlite`. So it's kept when a volume is mounted again or the server restarts, and starts again when its URL changes.
  * A cache is deleted once its backends are closed, unless it's of the volume as it's stored: the old one when its URL changes, and all of them when it's deleted. A restart deletes those left of volumes, or URLs, that aren't stored.
  * A backend made to check a volume's URL, when it's created or updated, gets a cache in memory.
  * The `filer` server's directory is its folder of the user's cache (`ctx.CacheDir()`), such as `~/Library/Caches/filer`.
  * The directory is made readable only by its owner, and locked while the registry is open (on Unix), so two servers can't share it and delete each other's caches. Only files named as caches are deleted.
  * A cache's identity is its volume's stored URL and when it was created, so a volume that's deleted and created again with the same URL starts again.

## Indexer

`indexer.New(name, backend, store)` returns a filer of a backend whose folders are indexed, in a `Store`, so its folder tree is listed from the index rather than the backend, which can be slow, such as a bucket or a network mount.

* **`Run(ctx, logger)`** indexes it, until the context is cancelled. It watches the backend, and scans it once it's watching, a folder at a time, so a change during the scan isn't missed. Failures are logged to the `*slog.Logger`, or to `slog.Default()` when it's `nil`; the registry passes its own.
* **Task failures:** a task retrying a file is logged at debug, and giving up at warn, with the volume, task, key and error. A task which returns `ErrBadParameter`, such as of a file which can't be read, is given up at once.
* **Following changes:** a rescan event scans that folder again. A folder which is created, or deleted, is indexed, or removed, with what's beneath it.
* **In the background:** changes are queued in the store's `object_state`, and a worker indexes them, so watchers, writes and deletes aren't held up, and work which is queued when it stops is done when it runs again. Changes of a path within the debounce, 250ms by default (`WithDebounce`), are one.
* **Changes, twice:** a change is sent to the watchers when it's queued, before it's indexed, and again once it is, so a watcher which listed what changed in between lists it again. Once it's indexed, an object is updated, or deleted, when the index doesn't have it, and a folder which is scanned is updated. The root isn't, as its change is a rescan of the volume, and files aren't, unless they're indexed.
* **Rescans** are coalesced, a rescan of a folder replacing those beneath it, and a folder is rescanned at most once a minute, by default (`WithRescanMinInterval`). A rescan which is sooner is deferred, not dropped.
* **Backends which can't be watched**, such as a bucket without SeaweedFS, are scanned again at the rescan interval, 15 minutes by default (`WithRescanInterval`).
* **A watch which fails** is watched, and scanned, again after a while, which doubles each time, up to a minute. Why it failed is in the state.
* **`ListFolders`** lists a folder's folders, or every folder beneath it, with `Recursive`, in the order a backend lists them. **`GetObject`** of a folder is the index's, with `folder:object-count` and `folder:subfolder-count`. Until the first scan finishes, both are the backend's.
* **Writes and deletes** through it queue the index of what they change, as the backend's changes do.
* **Everything else**, such as files, is the backend's. Files are indexed later.
* **`State`** is the index's status (never indexed, scanning, or ready), how many folders the scan has indexed, when it started and finished, and why it last failed.
* **Stores:** `store/sqlite`, in a file, or in memory with `sqlite.Memory`. Postgres comes later (#72). Every store passes `store/storetest`.

## Registry

`registry.New(ctx, registry.WithStore(store), ...)` returns the registry of volumes. The store is required: it's the volumes, and their indexes.

* **The store is the source of truth.** `CreateVolume`, `UpdateVolume` and `DeleteVolume` validate a volume, by creating its backend and closing it, and write the store. They return before it's mounted.
* **`Run` mounts what's stored.** It mounts the enabled volumes when it starts, and then follows the store: those created, updated, disabled or deleted, of the registry's calls, and the broadcaster's `store` changes. A store of one process, such as SQLite, publishes its own changes, and Postgres will publish those of other processes too (#72). When the store can't be listed, it's listed again, after a while. A volume is mounted again in place, keeping its name until its old backend is closed, and one which is deleted, and created again, is mounted again.
* **Backends are pluggable.** `registry.WithBackend(scheme, description, open)` adds a backend for a URL scheme, such as one in another package or program, with what it is, such as `RSS feeds`.
  * `open(ctx, url, cache, opts)` creates the volume's backend. The URL has the volume's options except `index`, which the registry handles. `cache` is the volume's `store.CacheStore`, which the registry owns. `opts` are the options every backend gets: the tracer, the credentials provider, and the client options.
  * The built-in backends (`file`, `s3`, `gtasks`, `gcontacts`, `gcalendar`, `gdrive`) are registered the same way. A scheme is lower case, of two characters or more, as one letter is a Windows drive. A scheme that's taken, or isn't valid, is an error.
  * **A stored volume whose backend isn't registered,** such as after a server is built without it, is offline, with why: `enabled: false` and an `unsupported scheme` error. Its URL and index are kept, and it's mounted once its backend is registered again.
  * **Choosing backends:** `registry.WithSchemes(schemes...)` uses only those named. `filer run --backend file,s3,rss` (repeatable, or `FILER_BACKEND`) does this, and runs every backend without it. A volume of a backend that isn't run is offline. A name that isn't a backend's is an error that lists those that are.
  * `Backends()` lists the backends the server runs, each with its scheme and description. `GET /volume` returns them as `backends`, and `filer volume-backends` shows them as a table (`volume-schemes` is an alias). The server logs `added backend` for each when it starts.
  * `filer volume-create <url>` creates a volume of any of them. It and the `volume-create-*` commands check first that the server runs the volume's backend.
  * **Read-only by default:** `volume-create`, and each `volume-create-*` command (other than RSS, which is always read-only), takes `--[no-]writable`, which defaults to false. It sets `readonly=false`, or `readonly=true`. For `volume-create`, without either, `readonly=true` is set unless the URL or an option already sets `readonly`. `volume-update NAME --[no-]writable` changes it, and keeps it as it is without either. For `volume-create` and `volume-update`, setting it with the flag and with `-o readonly=...` is an error. For the `volume-create-*` commands, `-o readonly=...` replaces the flag's value, as `-o` replaces other flags' options, so `volume-create-fs media /mnt/media --no-writable -o readonly=false` creates a writable volume.
  * **Each volume describes itself:** `Filer.Description()` returns what the volume is, such as `Folder /mnt/media` or `RSS feed The Example Podcast`. It's the URL's `description` option when that's set, or the backend's own description otherwise.
* **A volume's scheme isn't changed** by an update, which is `ErrBadParameter`, so a directory stays a directory. Its path, or a bucket's prefix, can be.
* **Creating and updating are atomic in the store:** a volume is created once, of the servers which create it at once, and an update of one which is deleted meanwhile is `ErrNotFound`.
* **Enabled:** a volume which isn't enabled is stored, with its index, but isn't mounted. Mounting or unmounting one, without changing its URL, doesn't create its backend. So a volume that's offline, or whose backend isn't registered, can be unmounted, or mounted, when it's offline until it can be. Updating a mounted volume still opens it again, such as to sign in. `filer volume-unmount NAME` disables one, `filer volume-mount NAME` enables it, and `filer volume-update NAME --[no-]enabled` does either, with its options. Over HTTP, it's `PATCH /volume/{name}?enabled=false`.
* **`GetVolume` and `ListVolumes`** return the stored volumes, which includes those which aren't enabled. A volume which is enabled, but offline, or can't be mounted, is `enabled: false`, with its `error`.
* **Offline volumes** are opened again, after a while, which doubles, up to a minute. One whose user needs to sign in again isn't, until it's updated, such as with `volume-update`, which opens it again even when nothing changed.
* **Descriptions:** every backend's URL has a `description` option, of what the volume is, which the backend sets, when it isn't set: `Folder /mnt/media`, `S3 bucket photos/2026 in eu-west-1`, or `Google Tasks for ada@example.com`. `Volume.Description()` returns it, and the CLI's create commands, and `volume-update`, set it with `--description`.
* **URLs** have each of the backend's options, its `index` option, and the placeholders of its credentials, such as `access-key-id=${ID}`, so it's created again from the store, but not the credentials.
* **`--volume`** creates a volume when the server starts, or updates the stored one of the same name.
* **Indexing:** the `index` option sets what's indexed.
  * `index=none`, or no option, indexes nothing; `index=folders` indexes folders; `index=objects` indexes files too, with their size, modified time, type, entity tag and the backend's metadata.
  * `index=metadata` indexes files as `objects` does, and enriches each new or changed file with a task, such as go-metadata's. The server passes the task with `registry.WithMetadataTask`, from the `metadata` package: `metadata.New(WithTMDB(key), WithAcoustID(key), ...)`. A server embeds `metadata/cmd`'s `Flags`, which are `--tmdb-key` (`TMDB_TOKEN`), `--acoustid-key` (`ACOUSTID_KEY`) and `--enrich-max-size`, and passes `Flags.WithMetadataEnrichment(ctx)` to the registry.
    * Task workers run separately from indexing, so a slow read never holds it up.
    * A task stores what it finds with `SetMetadata`, as its own metadata rows, which the backend's metadata never replaces. Reads merge the two, and the task's value wins for the same key.
    * **Places:** a file whose hints have `geo` has the shapes of its positions, such as a photo's GPS, stored as GeoJSON with their boxes (`object_geo`), and read as the object's `geo`.
    * **Events:** a file whose hints have `event`, such as a calendar's, has the times its events occur stored (`object_event`), and read as the object's `events`: each event, and task, which starts when it's due, and each time one which repeats occurs, when its rule ends (go-metadata's `calendar.Occurrences`). Each has its UID and recurrence ID, which identify it across files, its kind, title, start and end, as dates of an all-day event or times in UTC, and its zone, and up to 1,000 are stored of a file.
    * What a task found of a file is removed when the file changes, until it's enriched again.
  * **Search** (`object_search`, SQLite FTS5): every object, at any index level, is searchable by its name, the main fields of its metadata (`dc:title`, `creator`, `identifier`, `description`, `subject`), its own folder, the folder above it, the folders above those, and the text of its other metadata, ranked by weights of 10, 8, 5, 4, 3 and 4, so other metadata, such as an artist or a location, counts as much as the folder above an object's own. The backend's and tasks' metadata are merged, as they're read. Diacritics are ignored, prefixes are indexed, and folder names of fewer than three characters, links, numbers and `metadata:hint` aren't searched. It keeps an index, not the text, and is refreshed as objects and their metadata change. `Registry.Search(ctx, text, page)` reads the query of its text, of the volumes which are mounted, which is `GET /search?query=...&offset=...&limit=...`, whose limit defaults to, and is capped at, 100, or `filer search arvo '"spiegel im spiegel"' beethov* -symphony`. The store's `Search` takes a `schema.SearchRequest`, a `Query` of terms, any of which match, each a phrase, or the start of a word with `*`, and an `OffsetLimit`, and returns a `schema.SearchResponse` of `SearchObject`s, the objects of every volume, best first, with their volumes' names, and each one's `rank`, of 0 to 1, which is 1 of the best match of the search, of every page, and how many match.
    * A file is enriched again only when its listing's revision changes. A file whose task keeps failing is given up after five attempts, until it changes.
    * Raising a volume to `metadata` queues its indexed files for enrichment straight away, without waiting for a scan. Lowering it below `metadata` deletes what it was enriched with, so a later upgrade enriches every file again.
  * Folders are listed from the index.
  * With `index=objects`, a new or changed file is stored from `GetObject`, not from the listing, which on S3 lacks the metadata. That's one HEAD request per new or changed object.
  * A scan has two phases: **scanning** lists the tree, and **indexing** fetches and stores new or changed files 100 at a time. It then goes **idle**. The Index column shows the phase. On a first scan, the file count rises as files are stored.
  * The listing isn't held in memory. A scan records each key it sees in `object_state`, as rows of kind `seen`. It then deletes indexed objects it didn't see, stores the folders, and takes changed files from the store a page at a time.
  * A scan that stops, such as on a restart, keeps what it stored, and the next scan skips those files.
  * A folder's change event queues a rescan of that folder.
  * A file counts as changed when its listing's ETag, size or modified time differs from when it was stored. The index records these in `object_state`. On S3 a metadata-only overwrite keeps the ETag but changes the modified time, so it's caught.
  * `GetObject` of a file comes from the index when no work is queued for it. Otherwise it comes from the backend.
  * The index follows a backend's changes when it can be watched, and otherwise rescans it at an interval.
  * A rescan of the root is a catch-up for changes that may have been missed: after a restart, when a watcher falls behind, or on each poll. It doesn't count as queued work, so the index keeps serving while it runs. A change event still queues its own work, which sends reads of that path to the backend until it's done.
  * With `index=objects`, `ListObjects` of a folder or a file comes from the index when all of these hold:
    * the index is ready, and its last full scan was an `objects` scan;
    * the index has the folder or file;
    * no work is queued at it, above it, or beneath it.
  * Otherwise a listing comes from the backend, as does one of a path that's both a file and a folder.
  * A listing of a type, such as `type=image`, is a query on the index, which keeps each object's type without its parameters.
  * A page of a listing from the index has `count`, the number of objects the whole query matches, including its type filter but not `start_after` or the limit. When the listing can't be counted, such as from a backend, `count` is `null`.
  * Lowering it to `folders` removes the files from the index after the next scan.
  * The registry runs each indexer while the volume is mounted. Turning indexing off, or pointing the volume at another backend (scheme, host or path), removes the index. Deleting a volume deletes its index.
* **Volume changes:** with `store.WithBroadcaster`, a store publishes a change when a volume is created, updated or deleted, of schema `store`, table `volume`, whose row is the volume's name, and whose data is the volume. The `filer` server serves them on `/changes`, with its objects' changes.
* **Volume state changes:** with a broadcaster, the registry publishes a change when what it knows of a volume changes: it's mounted or unmounted, goes offline, can't be mounted, or is mounted again. They're of schema `registry`, table `volume`, whose row is the volume's name, without data, so the volume is read again. An attempt which fails for the same reason isn't published again.
* **Listing folders:** `ListObjects` with `Type` `inode/directory`, `inode`, or `directory`, which is `GET /object/{name}?type=directory`, or `filer objects --type directory`, lists an indexed volume's folders from its index, once it's ready. A listing of a type is filtered for every volume.
* **The `filer` server** keeps its store in `index.sqlite`, in its folder of the user's data, beside its credentials, with `make INDEXER_BACKEND=sqlite`, the default.
* **Where it keeps its files:**
  * **Data,** which is kept: its credentials and their passphrase (`credentials/`, `credentials.key`), and its index (`index.sqlite`). It's in its folder of the user's data: `~/Library/Application Support/filer` on macOS, `$XDG_DATA_HOME/filer` (or `~/.local/share/filer`) on Linux, and `%AppData%\filer` on Windows.
  * **Caches,** which can be cleared: the volumes' caches, in its folder of the user's cache, such as `~/Library/Caches/filer`.
  * **Earlier versions** kept the credentials and the index in the user's cache folder. They're moved to the data folder when the server starts, unless they're there already.

## Object Model

- An object is a file, identified by its path within the volume, which starts with `/`. The root of the volume is `/`.
- A directory exists when an object exists beneath it. Directories have no size or modification time, and their content type is `inode/directory`.
- The key of a file is its path, and the key of a directory is its path followed by `/`. The root's key is `/`, and every key starts with `/`. Listings are in the byte order of keys, as in S3.
- A bucket can have a file `/a` and a directory `/a/`. They're two objects, with different keys, and the index keeps both.
- Empty parent directories are removed by the filesystem backend when an object is deleted, as an S3 prefix with no objects beneath it does not exist.
- Files which are not regular files, such as sockets, named pipes and devices, are not objects.
- Symbolic links are not objects, as S3 has none, nor are files beneath a linked directory. An object can't be created over a link or beneath one. Deleting a directory deletes the links in it, not what they lead to.
- There are no renames, as in S3. A file renamed on disk is a delete and a create.
- Files and directories whose names are skipped (see [the filesystem's options](docs/backends.md#options)) are not objects, and can't be created.
- A package, such as a macOS sparse disk image, is a directory which is an object, as Finder shows it as one file. It has a modification time, and the content type of its extension, but no size, and can't be read. The files in it are not objects, and are deleted with it.
- Paths which aren't valid `fs.FS` paths, with `//`, `.` or `..` elements, or a trailing `/`, are `fs.ErrInvalid`.
- The content type of an object is from its extension, as sniffing the content would mean reading it.

## Watching

The registry of volumes is watched by its `Run`, which watches each volume when it's added, or updated, until it's removed, and publishes their changes to go-server's broadcaster, as changes of the `filer` schema, whose table is the volume, and whose row is the object's path. A row ending in `/` is a rescan, so a change of a directory has its path as the row and the data `{"folder":true}`. This continues until its context is cancelled, when it closes the registry: it removes and closes the volumes, cancelling the calls to `With` which are using them, and the registry can't be used again. It's closed only by `Run`, so it should be run while the registry is used, as the server does. The broadcaster is set with `registry.WithBroadcaster`, and its owner runs and serves it, such as a server, which can share it with its other modules. The server command creates one, served as `/changes`, unless it's given one. Without a broadcaster, the changes aren't published. `Registry.WatchObjects` watches the broadcaster, and converts the changes back to events. Without it, the volumes aren't watched. It holds a change to an object until the object is quiet for 250ms (`registry.WithDebounce`), or at most two seconds, and merges its changes, so an object which changes many times, such as a file which is written in parts, is one change, one which is created and deleted isn't one, and one which is deleted and created again is updated. A watcher isn't held up by the others, so when it's slow to receive the changes, the broadcaster drops a volume's changes, and sends it a rescan of the volume, once it can receive it, which is an `update` without a `row`. With a meter (`registry.WithMeter`), `Run` counts the changes in `filer.events`, a counter whose attributes are the worker, `filer.worker`, which is the hostname, or `registry.WithWorker`'s name, the volume, `filer.volume`, and the change, `filer.event.type`, such as `created`, which the meter's provider pushes to the metrics server.

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

## Development

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
