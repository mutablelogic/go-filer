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

## Building

`make` builds the server with cgo, which the macOS folder watcher needs, and which go-metadata's handlers of audio, video, and photos' EXIF, HEIF and RAW need too. They link go-media's libraries: FFmpeg 8, libexif, libheif, LibRaw, Chromaprint and libfvad.

* `make go-media` downloads the libraries from the release of the go-media version in `go.mod`, into `build/go-media`. The builds and tests with cgo run it first.
* `GO_MEDIA_PREFIX` uses another build of them instead, such as `build/install` in a clone of go-media, or `/opt/go-media` in its dev image. `GO_MEDIA_LICENSE=gpl` uses the GPL build of FFmpeg, rather than the LGPL one.
* On macOS, the libraries link some Homebrew packages, listed in `build/go-media/Brewfile`: `brew bundle --file build/go-media/Brewfile` installs them.
* `make CGO_ENABLED=0` builds without cgo, and so without the libraries. It reads less metadata, and can't watch volumes on macOS, which are polled instead.

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

### Options

The options of the filesystem backend are passed to `New` and `NewFS` as `url.Values`, and are the query parameters of the filer's URL, `fs://<name>`, which has each of them, defaults too, so it says how the filer is set up. They're followed by the options of a backend, `gofiler.Opts`, which every backend is created with, of the registry's `WithTracer`, `WithCredentials` and `WithClientOpts`, whose `Tracer` traces each operation, such as `fs.ListObjects`, with the volume and path, when it's set. Each option can be repeated, and setting one replaces its defaults, so an empty value, such as `skip=`, sets none.

| Option | Description | Default |
| -------- | ------------- | --------- |
| `skip` | A regular expression of the names of files and directories which aren't objects, nor is anything in a directory with the name | Hidden files, starting with `.`, and those which operating systems and applications leave alongside content, such as `Thumbs.db`, `desktop.ini`, `Icon\r`, Microsoft Office owner files (`~$*`) and backup files (`*~`) |
| `package` | A regular expression of the names of directories which are packages | `(?i)\.bundle$` and `(?i)\.sparsebundle$` |
| `readonly` | Whether objects can only be read, and not created or deleted, even when the filesystem is writable: `true`, `false`, or empty, which is `true`. A filesystem which isn't writable is read-only whatever it's set to | `false` |
| `description` | What the volume is, such as `Family photos`. Every backend has it | `Folder /mnt/media`, of the directory, or `Files of <name>` |

An unknown option, an expression which doesn't compile, or a `readonly` which isn't a boolean, is `filer.ErrBadParameter`.

## Google Tasks

**Caching, of every Google volume:** what's listed is kept for the refresh interval, a minute by default, so a task, contact or event which is read after it's listed, such as by indexing, isn't got from Google again. It's cleared when the volume's watch finds what changed, before it sends it, and when the volume writes. A volume which isn't watched, of a refresh interval of zero, keeps nothing.

`google.NewTaskFiler` returns a filer of a person's Google Tasks:

* **Lists** are directories, named by their ids, such as `/MTA3NjI`, with their titles as metadata, as `task:title` and `dc:title`, and `folder:children` and `folder:children-count`, whether they have tasks, and how many. The API doesn't count them, so they're read: one request a list, for each 100 tasks.
* **Tasks** are iCalendar files, `text/calendar; component=VTODO`, named by their ids, such as `/MTA3NjI/Q2xvdWQ.ics`, so a path stays the same when a task is renamed.
* **Content:** one `VTODO`, which calendar and task apps can import: the task's id as its `UID`, its title, notes, due date, status, completion, parent as `RELATED-TO`, URL, and update time.
* **Metadata:** only what iCalendar can't have, as what it has is go-metadata's, of it, when the volume is `index=metadata`, and `dc:title`, so a task has its title when it isn't: `task:hidden`, of a completed task cleared from its list, and `task:links`, such as to the email a task was created from. The user's default list's folder has `task:default`, of `true`.
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

In the registry, its URL is `gtasks://<name>`, such as `gtasks://tasks?readonly=false`, and its sign-in is of the registry's credentials (`registry.WithCredentials`):

```go
filer, err := google.NewTaskFiler(ctx, "tasks", url.Values{"readonly": {"true"}}, gofiler.Opts{Credentials: credentials, ClientOpts: clientopts})
```

* **`GOOGLE_CLIENT`:** the credential of the OAuth client, a desktop app's, as the JSON of its `client_id` and `client_secret`, or the file Google's console downloads for it.
* **`GOOGLE_TOKEN`:** the credential of the user's sign-in, a go-auth `Grant`, or another, of the `credential` parameter. It must be for `GOOGLE_CLIENT`'s client, and have the `tasks.readonly` or `tasks` scope, or `tasks`, when it isn't read-only. Its token is refreshed, with the client's secret, and stored again, when it expires.
* **Signing in:** when there's no sign-in, or it's for another client, or doesn't have the scopes, or is revoked, it's go-auth's `SignInError`, with how to sign in: Google's server, the client's ID, the scopes, and the credential. The HTTP API returns it as 401, with its `detail`, without the client's secret.
* **Options:** a backend's `gofiler.Opts`: its `Credentials`, of the sign-in, and its `ClientOpts`, go-client's, as they are, such as a user agent, a tracer, of `client.OptTracer`, or a trace.
* **`readonly`:** true by default. With `false`, tasks can be created, updated and deleted, and lists deleted, as under Writing above.
* **`description`:** what the volume is, which is `Google Tasks for <email>` by default, of the user who signed in. A sign-in asks for the `userinfo.email` scope too, but doesn't need it, so one from before it was asked for is just `Google Tasks`, until it's signed in again.
* **`quota`:** the most requests a minute, spaced evenly, which is 120 by default, or 0 for no limit.
* **`refresh_interval`:** how often `WatchObjects` polls for changes, which is a minute by default, or 0 not to watch.
* **`hidden`:** include tasks which are hidden, as they're completed, and cleared from their list, which are left out unless it's `true`.

## Google Contacts

`google.NewContactFiler` returns a filer of a person's Google Contacts, of the People API, with the same Google sign-in as Google Tasks, which asks for the `contacts.readonly` scope, or `contacts` when it's writable, keeping the scopes it has, so one sign-in can have both:

* **Contacts** are vCard files, `text/vcard`, at the root, named by their ids, such as `/c123.vcf`, without a size until they're read, as their photos aren't got for a listing, with `dc:title`, of their names, and `contact:url`, their links in Google Contacts, which a vCard can't have, as its URLs are the person's. The rest is the vCard's, which is go-metadata's.
* **Content:** written with go-metadata's `pkg/vcard`: names, nicknames, emails, phones, URLs, addresses, organisations, titles, birthday, notes, a photo, embedded, at 512 pixels, when the contact is read, or as a link, when it can't be got, but not Google's default one, of initials, and when it was updated, as `REV`. A primary value is preferred.
* **No birthdays as objects**, only contacts: birthdays are events of the Google Calendar volume.
* **Options:** `quota`, `refresh_interval`, `credential` and `description`, as Google Tasks has, whose description is `Google Contacts for <email>` by default. It's read-only by default. With `readonly=false` (`volume-create-gcontacts --writable`), it asks for the `contacts` scope and has the `contact` action.
* **Watching** polls the contacts' entity tags, at the refresh interval.

In the registry, its URL is `gcontacts://<name>`, and the `filer` command creates one with `volume-create-gcontacts`, signing in with Google, in the browser, when it needs to.

### Google Calendar

A volume of the events of a Google account's calendars, of the days before today and after it.

* **Layout:** a folder of each year, and of each month, with every calendar's events in it: `/2026/2026-11/20-<calendar>-<id>.ics`. The day is the one an event starts on, in its own zone (its `timeZone`, or else its time's offset), and `<calendar>` is its calendar's id, with characters such as `#` as `_`.
* **Repeating events** are an event of each time they occur, as Google lists them, so there are no rules to expand, and only those of the window are there.
* **Content:** written with go-metadata's `pkg/ical`, as a VEVENT: title, description, location, link, start and end (a date, or a time in its zone), status, organiser, attendees, its type (such as Birthday) as a category, and whether it's busy. Its UID is Google's iCalendar UID, which an invitation of it has too, and an occurrence has its `RECURRENCE-ID`. Metadata is only what iCalendar can't have, `calendar:calendar`, its calendar's id, and `dc:title`, as the rest, such as when it starts, is go-metadata's, of it.
* **Options:** `window`, the time before today and after it, such as `30d`, `52w` or `720h`, of whole days, which is `365d` by default, and `quota`, `refresh_interval`, `credential` and `description`, as Google Tasks has. It's read-only by default. With `readonly=false` (`volume-create-gcalendar --writable`), it asks for the `calendar` scope and has the `event` action.
* **Watching** syncs each calendar with its sync token, at the refresh interval, and sends what changed: an event, or a time one which repeats occurs, which is created, updated, moved (deleted where it was, and created where it is), or cancelled, of where the events which were last listed are, as a change doesn't say where an event was. The root is rescanned instead when an event which repeats changed, as a sync doesn't say which times it occurs, before anything is listed, when a calendar is added, removed or renamed, when a token is rejected, and each day, as the window moves on. The tokens are stored with the server's credentials, of the sign-in, as `GOOGLE_TOKEN_CALENDAR_SYNC` (of `GOOGLE_TOKEN`), so after a restart only what changed since is synced, and volumes of the same sign-in share them. A token which Google rejects is synced again, at most once an hour.

In the registry, its URL is `gcalendar://<name>`, and the `filer` command creates one with `volume-create-gcalendar`, signing in with Google, in the browser, when it needs to.

### Google Drive

A volume of the files a Google account owns in My Drive, read-only unless it's writable.

* **Layout:** the folders are directories, and each file is at its path, such as `/Work/Reports/report.pdf`.
* **Names:** a `/` in a name is `_`. Drive lets two files in a folder have the same name, so each then gets the start of its id, such as `report (1a2b3c).pdf`. Names are compared ignoring case.
* **Google Docs, Sheets and Slides** are exported as `.docx`, `.xlsx` and `.pptx`, with no size until they're read. Google exports at most 10 MB, and a larger one is `ErrForbidden` when it's read.
* **Not listed:** files others own, such as those shared with you, files in the bin, hidden files (a name starting with `.`), and Google's other types, such as forms and shortcuts. A file in more than one folder is in its first.
* **Content** of a file is read from the revision it was listed with, so it always matches its size and entity tag. It can seek and `ReadAt`, as S3's can, so enrichment reads the ranges it needs, such as a video's headers, rather than all of it. A Doc is exported into memory, and its version is checked before and after. If it changed since it was listed, the read is `ErrPreconditionFailed`, and the list of files is got again.
* **Entity tags:** a file's MD5 checksum, or a Doc's id and version, as each file counts its own versions.
* **Metadata:** `drive:id`, `drive:url` (its link in Drive), `drive:owner`, and `drive:type` of a Doc, Sheet or Slides, such as `document`.
* **Options:** `quota`, `credential` and `description`, as Google Tasks has, and `refresh_interval`, how often changes are polled for, which is a minute, or `0` not to watch it. When it isn't watched, it's how long the list of files is kept. It asks for the `drive.readonly` scope, or `drive` with `readonly=false` (`volume-create-gdrive --writable`).
* **Writing**, of a writable volume:
  * `CreateObject` uploads a file as it's read, in 8 MiB requests, with Google's resumable upload, so only one chunk is in memory.
  * A file that's there gets a new revision, keeping its id, link and sharing. Otherwise a new file is created, along with any missing folders in its path. A new file is created empty first, under a lock only creates share, so two creates of a path don't both make it. Its content is then uploaded as a new revision, alongside other uploads. If the upload fails, the empty file is binned.
  * Its type is the request's, or its extension's, for a new file or one that's there. Its modified time is the request's.
  * It isn't written over a folder, a hidden name, a name that starts or ends with a space, or a name that's another's in its folder ignoring case (its name in Drive, or its path), which would rename both.
  * Docs, Sheets and Slides aren't written (`ErrForbidden`), as their content is an export.
  * `DeleteObjects` moves a file, or a folder with `Recursive`, to the bin, from which it can be restored. Folders left empty stay.
  * While it's watched, a write is sent to the watch as it's made, and isn't sent again when it's polled.
  * Renaming and moving aren't supported yet; they'll be an action.
* **Watching** polls Drive's changes feed, applies the changes to the files it knows, and sends what changed: created, updated, or deleted (such as a file that's moved, renamed, binned or given away, and everything in a folder that's renamed or moved). While it's watched, the filer lists and reads the files the watch knows, rather than listing them again. The first change is a rescan. A token Google doesn't accept lists the files again, with a rescan. When Google is unavailable, it polls again at the next interval. The token isn't stored, so a restart lists the files again (#170 will keep them).

In the registry, its URL is `gdrive://<name>`, and the `filer` command creates one with `volume-create-gdrive`, signing in with Google, in the browser, when it needs to.

## RSS Feeds

A volume of an RSS feed's items, read-only. It's an example of a pluggable backend: it isn't built into the registry, and the `filer` server adds it with `registry.WithBackend(rss.Scheme, rss.Description, rss.Open)`.

* **URL:** `rss://<name>/<host>/<path>`, of the feed `https://<host>/<path>`. `filer volume-create-rss news https://feeds.example.com/news.xml -o index=metadata` creates one, with `--refresh-interval` and `--description`.
  * `insecure=true` fetches it over http instead.
  * Query parameters that aren't the volume's options belong to the feed's URL, such as `?format=rss`.
* **Layout:** a folder of each year and month, of the day each item was published, in UTC: `/2026/2026-11/20-<title>.html`. Items without a date are in `/undated/`. Items of the same title and day are numbered, in the order of their guids.
* **Content:** each item is an HTML page of its title, author, date, link, description and enclosure.
* **Metadata:**
  * `dc:title`, `dc:creator`, `dc:date`, `dc:identifier` (its guid, or its link when it has none), `dc:subject` (its categories), and the feed's `dc:language` and `dc:rights`.
  * `rss:url` (its link), `rss:comments`, `rss:feed` (the feed's title) and `rss:image` (the episode's image, or the feed's).
  * A podcast's item also has `rss:enclosure` (its URL, type and length), `rss:episode`, `rss:season`, `rss:episode_type` and `rss:duration` in seconds.
* **Options:**
  * `refresh_interval`: how long a fetched feed is kept, and how often it's polled. It's never less than the feed's `<ttl>`. Without it, it's the TTL, or `5m` when the feed has none. It's only in the volume's URL when it's set.
  * `description`: by default, the feed's title.
* **Watching:**
  * The feed is polled at that interval.
  * Each poll sends the feed's last ETag and Last-Modified, so an unchanged feed is a 304, not downloaded again.
  * Changes are sent by the items' guids, or their links when they have none:
    * a new item is created;
    * a gone item is deleted;
    * a changed item is updated;
    * an item whose title or date changed is deleted where it was, and created where it is.
  * Folders are created and deleted as items appear in, and leave, their months.
* **Errors:** a feed that isn't found, or isn't RSS, is `ErrBadParameter`. One that can't be reached is `ErrServiceUnavailable`, so the volume is offline and retried.

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

A `store.CacheStore` keeps what a volume's backend gets from a service, by key, on disk, so it isn't held in memory or fetched again after a restart. Each volume has its own, and the registry will pass it to the backend's New function (#170). Using it is up to the backend, which must work without any of its entries.

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

## Indexer

`indexer.New(name, backend, store)` returns a filer of a backend whose folders are indexed, in a `Store`, so its folder tree is listed from the index rather than the backend, which can be slow, such as a bucket or a network mount.

* **`Run(ctx, logger)`** indexes it, until the context is cancelled. It watches the backend, and scans it once it's watching, a folder at a time, so a change during the scan isn't missed. Failures are logged to the `*slog.Logger`, or to `slog.Default()` when it's `nil`; the registry passes its own.
* **Task failures:** a task retrying a file is logged at debug, and giving up at warn, with the volume, task, key and error. A task which returns `ErrBadParameter`, such as of a file which can't be read, is given up at once.
* **Following changes:** a rescan event scans that folder again. A folder which is created, or deleted, is indexed, or removed, with what's beneath it.
* **In the background:** changes are queued in the store's `object_state`, and a worker indexes them, so watchers, writes and deletes aren't held up, and work which is queued when it stops is done when it runs again. Changes of a path within the debounce, 250ms by default (`WithDebounce`), are one.
* **Rescans** are coalesced, a rescan of a folder replacing those beneath it, and a folder is rescanned at most once a minute, by default (`WithRescanMinInterval`). A rescan which is sooner is deferred, not dropped.
* **Backends which can't be watched**, such as a bucket without SeaweedFS, are scanned again at the rescan interval, 15 minutes by default (`WithRescanInterval`).
* **A watch which fails** is watched, and scanned, again after a while, which doubles each time, up to a minute. Why it failed is in the state.
* **`ListFolders`** lists a folder's folders, or every folder beneath it, with `Recursive`, in the order a backend lists them. **`GetObject`** of a folder is the index's, with `folder:children` and `folder:children-count`. Until the first scan finishes, both are the backend's.
* **Writes and deletes** through it queue the index of what they change, as the backend's changes do.
* **Everything else**, such as files, is the backend's. Files are indexed later.
* **`State`** is the index's status (never indexed, scanning, or ready), how many folders the scan has indexed, when it started and finished, and why it last failed.
* **Stores:** `store/sqlite`, in a file, or in memory with `sqlite.Memory`. Postgres comes later (#72). Every store passes `store/storetest`.

## Registry

`registry.New(ctx, registry.WithStore(store), ...)` returns the registry of volumes. The store is required: it's the volumes, and their indexes.

* **The store is the source of truth.** `CreateVolume`, `UpdateVolume` and `DeleteVolume` validate a volume, by creating its backend and closing it, and write the store. They return before it's mounted.
* **`Run` mounts what's stored.** It mounts the enabled volumes when it starts, and then follows the store: those created, updated, disabled or deleted, of the registry's calls, and the broadcaster's `store` changes. A store of one process, such as SQLite, publishes its own changes, and Postgres will publish those of other processes too (#72). When the store can't be listed, it's listed again, after a while. A volume is mounted again in place, keeping its name until its old backend is closed, and one which is deleted, and created again, is mounted again.
* **Backends are pluggable.** `registry.WithBackend(scheme, description, open)` adds a backend for a URL scheme, such as one in another package or program, with what it is, such as `RSS feeds`.
  * `open(ctx, url, opts)` creates the volume's backend. The URL has the volume's options except `index`, which the registry handles. `opts` are the options every backend gets: the tracer, the credentials provider, and the client options.
  * The built-in backends (`file`, `s3`, `gtasks`, `gcontacts`, `gcalendar`, `gdrive`) are registered the same way. A scheme is lower case, of two characters or more, as one letter is a Windows drive. A scheme that's taken, or isn't valid, is an error.
  * **A stored volume whose backend isn't registered,** such as after a server is built without it, is offline, with why: `enabled: false` and an `unsupported scheme` error. Its URL and index are kept, and it's mounted once its backend is registered again.
  * **Choosing backends:** `registry.WithSchemes(schemes...)` uses only those named. `filer run --backend file,s3,rss` (repeatable, or `FILER_BACKEND`) does this, and runs every backend without it. A volume of a backend that isn't run is offline. A name that isn't a backend's is an error that lists those that are.
  * `Backends()` lists the backends the server runs, each with its scheme and description. `GET /volume` returns them as `backends`, and `filer volume-backends` shows them as a table (`volume-schemes` is an alias). The server logs `added backend` for each when it starts.
  * `filer volume-create <url>` creates a volume of any of them. It and the `volume-create-*` commands check first that the server runs the volume's backend.
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
* **Listing folders:** `ListObjects` with `Type` `inode/directory`, `inode`, or `directory`, which is `GET /object/{name}?type=directory`, or `filer objects --type directory`, lists an indexed volume's folders from its index, once it's ready. A listing of a type is filtered for every volume.
* **The `filer` server** keeps its store in `<name>-index.db`, in the user's cache folder, with `make INDEXER_BACKEND=sqlite`, the default.

## Object Model

- An object is a file, identified by its path within the volume, which starts with `/`. The root of the volume is `/`.
- A directory exists when an object exists beneath it. Directories have no size or modification time, and their content type is `inode/directory`.
- The key of a file is its path, and the key of a directory is its path followed by `/`. The root's key is `/`, and every key starts with `/`. Listings are in the byte order of keys, as in S3.
- A bucket can have a file `/a` and a directory `/a/`. They're two objects, with different keys, and the index keeps both.
- Empty parent directories are removed by the filesystem backend when an object is deleted, as an S3 prefix with no objects beneath it does not exist.
- Files which are not regular files, such as sockets, named pipes and devices, are not objects.
- Symbolic links are not objects, as S3 has none, nor are files beneath a linked directory. An object can't be created over a link or beneath one. Deleting a directory deletes the links in it, not what they lead to.
- There are no renames, as in S3. A file renamed on disk is a delete and a create.
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

`WatchObjects` sends each change to an object to a channel, until the context is cancelled or the filer is closed. The first change is a rescan of the root, once the objects are watched, so a caller which lists them then misses no change. A change is an object which is created, updated or deleted, or a directory which should be rescanned, as its changes may have been missed. A change of a directory names it by its key, ending in `/`, when the backend knows it's a directory. The filesystem knows on create and update, but not on delete, and SeaweedFS always knows. Changes to files which aren't objects aren't sent, and a change within a package is an update to the package.

Directories are watched with the `fsevents` package, using the efficient mechanism of each platform:

| Platform | Mechanism |
| ---------- | ----------- |
| macOS | FSEvents, which needs cgo |
| Linux | inotify |
| Windows | `ReadDirectoryChangesW` |

On other platforms, and on macOS without cgo, and for a filer of a filesystem which isn't a directory, `WatchObjects` returns `filer.ErrNotImplemented`.

The registry of volumes is watched by its `Run`, which watches each volume when it's added, or updated, until it's removed, and publishes their changes to go-server's broadcaster, as changes of the `filer` schema, whose table is the volume, and whose row is the object's path. A row ending in `/` is a rescan, so a change of a directory has its path as the row and the data `{"folder":true}`. This continues until its context is cancelled, when it closes the registry: it removes and closes the volumes, cancelling the calls to `With` which are using them, and the registry can't be used again. It's closed only by `Run`, so it should be run while the registry is used, as the server does. The broadcaster is set with `registry.WithBroadcaster`, and its owner runs and serves it, such as a server, which can share it with its other modules. The server command creates one, served as `/changes`, unless it's given one. Without a broadcaster, the changes aren't published. `Registry.WatchObjects` watches the broadcaster, and converts the changes back to events. Without it, the volumes aren't watched. It holds a change to an object until the object is quiet for 250ms (`registry.WithDebounce`), or at most two seconds, and merges its changes, so an object which changes many times, such as a file which is written in parts, is one change, one which is created and deleted isn't one, and one which is deleted and created again is updated. A watcher isn't held up by the others, so when it's slow to receive the changes, the broadcaster drops a volume's changes, and sends it a rescan of the volume, once it can receive it, which is an `update` without a `row`. With a meter (`registry.WithMeter`), `Run` counts the changes in `filer.events`, a counter whose attributes are the worker, `filer.worker`, which is the hostname, or `registry.WithWorker`'s name, the volume, `filer.volume`, and the change, `filer.event.type`, such as `created`, which the meter's provider pushes to the metrics server.

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
| `make clean` | Remove `build/` |

The test workflow vets the packages, and runs the race tests on Linux, macOS and Windows, for pull requests and pushes to `main`.

`make` builds the `filer` server with the store of its indexes chosen by `INDEXER_BACKEND`, as the build tag `indexer_<backend>`:

* **`sqlite`**, the default, is a SQLite database, `<name>-index.db`, such as `filer-index.db`, in the user's cache folder.
* **`memory`** is a SQLite database in memory, so volumes aren't kept, and are scanned again, when the server starts.
* **`pg`** is a Postgres database, of go-pg's flags, `--pg.url` and `--pg.password`, or `PG_URL` and `PG_PASSWORD`.

The Postgres store isn't implemented yet (#72), so its volumes' indexes fail with `ErrNotImplemented`, until it is.
