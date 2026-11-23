# Backends

A volume's backend is chosen by its URL's scheme, such as `file://media/mnt/media`. Each implements `filer.Filer`, and behaves the same way, so a client sees no difference between them. The semantics follow S3, and the filesystem backend emulates S3 where they differ.

| Backend | Scheme | Writable | Watched | CLI |
| --- | --- | --- | --- | --- |
| [Filesystem](#filesystem) | `file` | Yes | FSEvents, inotify or `ReadDirectoryChangesW` | `volume-create-fs` |
| [S3](#s3) | `s3` | Yes | Through a SeaweedFS filer, or rescanned | `volume-create-s3` |
| [Google Tasks](#google-tasks) | `gtasks` | With `readonly=false` | Polled | `volume-create-gtasks` |
| [Google Contacts](#google-contacts) | `gcontacts` | With `readonly=false` | Synced | `volume-create-gcontacts` |
| [Google Calendar](#google-calendar) | `gcalendar` | With `readonly=false` | Synced | `volume-create-gcalendar` |
| [Google Drive](#google-drive) | `gdrive` | With `readonly=false` | Its changes feed | `volume-create-gdrive` |
| [RSS feeds](#rss-feeds) | `rss` | No | Polled | `volume-create-rss` |
| [Calendar subscriptions](#calendar-subscriptions) | `webcal` | No | Polled | `volume-create-webcal` |

## Filesystem

`fs.New` returns a writable filer for a directory, and `fs.NewFS` a filer for any `fs.FS` (see [Usage](development.md#usage)).

### Options

The options of the filesystem backend are passed to `New` and `NewFS` as `url.Values`, and are the query parameters of the filer's URL, `fs://<name>`, which has each of them, defaults too, so it says how the filer is set up. They're followed by the options of a backend, `gofiler.Opts`, which every backend is created with, of the registry's `WithTracer`, `WithCredentials` and `WithClientOpts`, whose `Tracer` traces each operation, such as `fs.ListObjects`, with the volume and path, when it's set. Each option can be repeated, and setting one replaces its defaults, so an empty value, such as `skip=`, sets none.

| Option | Description | Default |
| -------- | ------------- | --------- |
| `skip` | A regular expression of the names of files and directories which aren't objects, nor is anything in a directory with the name | Hidden files, starting with `.`, and those which operating systems and applications leave alongside content, such as `Thumbs.db`, `desktop.ini`, `Icon\r`, Microsoft Office owner files (`~$*`) and backup files (`*~`) |
| `package` | A regular expression of the names of directories which are packages | `(?i)\.bundle$` and `(?i)\.sparsebundle$` |
| `readonly` | Whether objects can only be read, and not created or deleted, even when the filesystem is writable: `true`, `false`, or empty, which is `true`. A filesystem which isn't writable is read-only whatever it's set to | `false` |
| `description` | What the volume is, such as `Family photos`. Every backend has it | `Folder /mnt/media`, of the directory, or `Files of <name>` |

An unknown option, an expression which doesn't compile, or a `readonly` which isn't a boolean, is `filer.ErrBadParameter`.

### Watching

`WatchObjects` sends each change to an object to a channel, until the context is cancelled or the filer is closed. The first change is a rescan of the root, once the objects are watched, so a caller which lists them then misses no change. A change is an object which is created, updated or deleted, or a directory which should be rescanned, as its changes may have been missed. A change of a directory names it by its key, ending in `/`, when the backend knows it's a directory. The filesystem knows on create and update, but not on delete, and SeaweedFS always knows. Changes to files which aren't objects aren't sent, and a change within a package is an update to the package.

Directories are watched with the `fsevents` package, using the efficient mechanism of each platform:

| Platform | Mechanism |
| ---------- | ----------- |
| macOS | FSEvents, which needs cgo |
| Linux | inotify |
| Windows | `ReadDirectoryChangesW` |

On other platforms, and on macOS without cgo, and for a filer of a filesystem which isn't a directory, `WatchObjects` returns `filer.ErrNotImplemented`.

## S3

### Modification Time

The modification time of an object is stored in the `x-amz-meta-mtime` metadata of an S3 object, which is used in preference to `LastModified`. `LastModified` is the time the object was uploaded, and cannot be set, so it is only used when the metadata is not set. Its value is seconds since the Unix epoch, with an optional fraction of a second, such as `1764569708.5`, as rclone and s3fs write it, so the times they write are read too. It isn't part of an object's user metadata, so `mtime` can't be a key of it, outside a namespace.

`ListObjectsV2` does not return metadata, so a listing from S3 has the `LastModified` time, which can differ from the modification time returned by `GetObject`.

### Differences

These cannot be made identical, and are known differences between the backends:

- **Keys.** S3 allows keys which cannot be represented as paths, such as those containing `//`, `.` or `..` elements, `\` or `:`, or a trailing `/`, and a file and a directory with the same name, such as `a` and `a/b`. The S3 backend skips these keys in listings, so that they are not objects, in the same way as hidden files.
- **Directory markers.** Zero-byte objects whose keys end in `/` are created by the S3 console and some tools to represent directories. They are not objects, and are skipped in listings.
- **Atomicity.** Deleting a directory in S3 takes several requests, and can fail part of the way through. Deleting a directory on the filesystem is not atomic either.
- **ETag.** S3 returns an ETag for each object. The filesystem backend does not set it, so only `IfNoneMatch: *` matches.
- **Packages.** S3 has no packages, so a package's files are objects in S3.

## Filesystem and S3

How each operation is done, of the filesystem, and of S3:

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
| `WatchObjects` | Watch the directory (see [Watching](#watching)) | Subscribe to the changes to a SeaweedFS filer's metadata, with the `seaweedfs-filer-grpc` option, and otherwise `filer.ErrNotImplemented`, as S3's event notifications need a queue |

Object metadata isn't stored yet, so `CreateObject` with metadata is `filer.ErrNotImplemented`.

## Google

Google's backends sign in with the same OAuth client, and keep what they get from Google in the volume's cache.

### Signing in

Store the OAuth client, a web application's, which signs in with the web UI and the CLI, or a desktop app's, which signs in with the CLI only, as the server's credential, `GOOGLE_CLIENT`, of the file Google's console downloads. A volume then signs in with Google, in the browser, when it's created, or mounted, and needs to:

```bash
filer credential-set GOOGLE_CLIENT --json < client_secret.json
filer volume-create-gtasks tasks
```

* **`GOOGLE_CLIENT`:** the credential of the OAuth client, as the JSON of its `client_id` and `client_secret`, or the file Google's console downloads for it. A desktop app's client signs in with the CLI, at any loopback address. A **web application's** client signs in with the CLI, and the web UI too: its authorized redirect URIs are the CLI's loopback address, `http://localhost:8085/callback` (of `--signin-addr`), and the server's callback, `<api>/credential-signin/callback`, as each browser reaches it, such as `http://localhost:8084/api/credential-signin/callback`, or `https://filer.example.com/api/credential-signin/callback` (Google only allows `http` of `localhost`). It needs no JavaScript origins.
* **`GOOGLE_TOKEN`:** the credential of the user's sign-in, a go-auth `Grant`, or another, of the `credential` parameter. It must be for `GOOGLE_CLIENT`'s client, and have the scopes its volumes ask for, such as `tasks.readonly`, or `tasks` when the volume is writable, which one sign-in can have of each of Google's backends. Its token is refreshed, with the client's secret, and stored again, when it expires.
* **Signing in:** when there's no sign-in, or it's for another client, or doesn't have the scopes, or is revoked, it's go-auth's `SignInError`, with how to sign in: Google's server, the client's ID, the scopes, and the credential. The HTTP API returns it as 401, with its `detail`, without the client's secret.

### Caching

What's listed is kept for the refresh interval, a minute by default, so a task, contact or event which is read after it's listed, such as by indexing, isn't got from Google again. It's cleared when the volume's watch finds what changed, before it sends it, and when the volume writes. A volume which isn't watched, of a refresh interval of zero, keeps nothing.

### Google Tasks

`google.NewTaskFiler` returns a filer of a person's Google Tasks:

* **Lists** are directories, named by their ids, such as `/MTA3NjI`, with their titles as metadata, as `task:title` and `dc:title`, and `folder:object-count`, how many tasks they have, with `folder:subfolder-count` of 0. The API doesn't count them, so they're read: one request a list, for each 100 tasks.
* **Tasks** are iCalendar files, `text/calendar; component=VTODO`, named by their ids, such as `/MTA3NjI/Q2xvdWQ.ics`, so a path stays the same when a task is renamed.
* **Content:** one `VTODO`, which calendar and task apps can import: the task's id as its `UID`, its title, notes, due date, status, completion, parent as `RELATED-TO`, URL, and update time.
* **Metadata:** only what iCalendar can't have, as what it has is go-metadata's, of it, when the volume is `index=metadata`, and `dc:title`, so a task has its title when it isn't: `task:hidden`, of a completed task cleared from its list, and `task:links`, such as to the email a task was created from. The user's default list's folder has `task:default`, of `true`.
* **Writing:** a volume with `readonly=false`, which asks for write access when its user signs in, writes a task as an iCalendar of one `VTODO`:
  * **Creating:** `PUT /LIST/~new.ics` creates a task, beneath its `RELATED-TO`, when it has one, and returns it at its path, of the id Google gives it, as `~` is never in one.
  * **Updating:** `PUT` of a task's path replaces its title, notes, due date and status, clearing those the `VTODO` doesn't have, and moves it, when its `RELATED-TO` changes. Its other fields, such as its links, are kept. Its `UID`, when it has one, must be the task's id, and a task which doesn't exist isn't found, as its id can't be chosen.
  * **Deleting:** a task, with its subtasks, as Google does, or a list, with its tasks, with `recursive`, which returns the list too, after its tasks, as it's an object of its own.
  * **Watching:** what the volume writes, or deletes, is sent to its watches straight away, rather than when they refresh.

* **Options:** a backend's `gofiler.Opts`: its `Credentials`, of the sign-in, and its `ClientOpts`, go-client's, as they are, such as a user agent, a tracer, of `client.OptTracer`, or a trace.
* **`readonly`:** true by default. With `false`, tasks can be created, updated and deleted, and lists deleted, as under Writing above.
* **`description`:** what the volume is, which is `Google Tasks for <email>` by default, of the user who signed in. A sign-in asks for the `userinfo.email` scope too, but doesn't need it, so one from before it was asked for is just `Google Tasks`, until it's signed in again.
* **`quota`:** the most requests a minute, spaced evenly, which is 120 by default, or 0 for no limit.
* **`refresh_interval`:** how often `WatchObjects` polls for changes, which is a minute by default, or 0 not to watch.
* **`hidden`:** include tasks which are hidden, as they're completed, and cleared from their list, which are left out unless it's `true`.

In the registry, its URL is `gtasks://<name>`, such as `gtasks://tasks?readonly=false`, and its sign-in is of the registry's credentials (`registry.WithCredentials`):

```go
filer, err := google.NewTaskFiler(ctx, "tasks", url.Values{"readonly": {"true"}}, gofiler.Opts{Credentials: credentials, ClientOpts: clientopts})
```

### Google Contacts

`google.NewContactFiler` returns a filer of a person's Google Contacts, of the People API, with the same Google sign-in as Google Tasks, which asks for the `contacts.readonly` scope, or `contacts` when it's writable, keeping the scopes it has, so one sign-in can have both:

* **Contacts** are vCard files, `text/vcard`, at the root, named by their ids, such as `/c123.vcf`, without a size until they're read, as their photos aren't got for a listing, with `dc:title`, of their names, and `contact:url`, their links in Google Contacts, which a vCard can't have, as its URLs are the person's. The rest is the vCard's, which is go-metadata's.
* **Content:** written with go-metadata's `pkg/vcard`: names, nicknames, emails, phones, URLs, addresses, organisations, titles, birthday, notes, a photo, embedded, at 512 pixels, when the contact is read, or as a link, when it can't be got, but not Google's default one, of initials, and when it was updated, as `REV`. A primary value is preferred.
* **No birthdays as objects**, only contacts: birthdays are events of the Google Calendar volume.
* **Options:** `quota`, `refresh_interval`, `credential` and `description`, as Google Tasks has, whose description is `Google Contacts for <email>` by default. It's read-only by default. With `readonly=false` (`volume-create-gcontacts --writable`), it asks for the `contacts` scope and has the `contact` action.
* **Watching** syncs the contacts at the refresh interval, with the People API's sync token, so only what changed is fetched, and sends a contact that's created, changed (its ETag) or deleted.
  * While it's watched, reads use the contacts the watch knows. The `contact` action's changes are applied and sent as they're made, and not sent again when they're synced.
  * The contacts and their token are saved in the volume's cache, after the first sync, at most every five minutes while they change, and when it stops. So a restart syncs what changed since, rather than fetching every contact, unless Google doesn't accept the token (it expires after seven days).
* **Photos** are kept in the volume's cache for a day, so they aren't downloaded each time a contact is read.

In the registry, its URL is `gcontacts://<name>`, and the `filer` command creates one with `volume-create-gcontacts`, signing in with Google, in the browser, when it needs to.

### Google Calendar

A volume of the events of a Google account's calendars, of the days before today and after it.

* **Layout:** a folder of each year, and of each month, with every calendar's events in it: `/2026/2026-11/20-<calendar>-<id>.ics`. The day is the one an event starts on, in its own zone (its `timeZone`, or else its time's offset), and `<calendar>` is its calendar's id, with characters such as `#` as `_`.
* **Repeating events** are an event of each time they occur, as Google lists them, so there are no rules to expand, and only those of the window are there.
* **Content:** written with go-metadata's `pkg/ical`, as a VEVENT: title, description, location, link, start and end (a date, or a time in its zone), status, organiser, attendees, its type (such as Birthday) as a category, and whether it's busy. Its UID is Google's iCalendar UID, which an invitation of it has too, and an occurrence has its `RECURRENCE-ID`. Metadata is only what iCalendar can't have, `calendar:calendar`, its calendar's id, and `dc:title`, as the rest, such as when it starts, is go-metadata's, of it.
* **Options:** `window`, the time before today and after it, such as `30d`, `52w` or `720h`, of whole days, which is `365d` by default, and `quota`, `refresh_interval`, `credential` and `description`, as Google Tasks has. It's read-only by default. With `readonly=false` (`volume-create-gcalendar --writable`), it asks for the `calendar` scope and has the `event` action.
* **Watching** syncs each calendar with its sync token, at the refresh interval, and sends what changed: an event, or a time one which repeats occurs, which is created, updated, moved (deleted where it was, and created where it is), or cancelled, of where the events which were last listed are, as a change doesn't say where an event was. The root is rescanned instead when an event which repeats changed, as a sync doesn't say which times it occurs, before anything is listed, when a calendar is added, removed or renamed, when a token is rejected, and each day, as the window moves on. The tokens are stored with the server's credentials, of the sign-in, as `GOOGLE_TOKEN_CALENDAR_SYNC` (of `GOOGLE_TOKEN`), so after a restart only what changed since is synced, and volumes of the same sign-in share them. A token which Google rejects is synced again, at most once an hour.
* **Cache:** while it's watched, each directory's listing is saved in the volume's cache, with the days it's of.
  * An event that changes deletes the listings of its month, its year and the root, and a rescan deletes them all. A listing fetched before they're deleted isn't saved.
  * A checkpoint of the calendars' sync tokens and ETags says which state the listings are up to date with. It's deleted before listings are, and saved after, so listings that couldn't be deleted aren't trusted.
  * A restart uses the saved listings only when the checkpoint's tokens are the ones it starts from (another volume of the sign-in may have synced them since), its first sync finds nothing changed, and a listing is of the same days. Otherwise it lists them again.

In the registry, its URL is `gcalendar://<name>`, and the `filer` command creates one with `volume-create-gcalendar`, signing in with Google, in the browser, when it needs to.

### Google Drive

A volume of the files a Google account owns in My Drive, read-only unless it's writable.

* **Layout:** the folders are directories, and each file is at its path, such as `/Work/Reports/report.pdf`.
* **Names:** a `/` in a name is `_`. Drive lets two files in a folder have the same name, so each then gets the start of its id, such as `report (1a2b3c).pdf`. Names are compared ignoring case.
* **Google Docs, Sheets and Slides** are exported as `.docx`, `.xlsx` and `.pptx`, with no size until they're read. Google exports at most 10 MB, and a larger one is `ErrForbidden` when it's read.
* **Not listed:** files others own, such as those shared with you, files in the bin, hidden files (a name starting with `.`), and Google's other types, such as forms and shortcuts. A file in more than one folder is in its first.
* **Content** of a file is read from the revision it was listed with, so it always matches its size and entity tag. It can seek and `ReadAt`, as S3's can, so enrichment reads the ranges it needs, such as a video's headers, rather than all of it. A Doc is exported into the volume's cache, where it's kept for a week, and its version is checked before and after. If it changed since it was listed, nothing is kept, the read is `ErrPreconditionFailed`, and the list of files is got again. A Doc that hasn't changed is read from the cache rather than exported again, and can seek and `ReadAt`. A Doc that's removed while it's watched has its export deleted.
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
* **Watching** polls Drive's changes feed, applies the changes to the files it knows, and sends what changed: created, updated, or deleted (such as a file that's moved, renamed, binned or given away, and everything in a folder that's renamed or moved). While it's watched, the filer lists and reads the files the watch knows, rather than listing them again. The first change is a rescan. A token Google doesn't accept lists the files again, with a rescan. When Google is unavailable, it polls again at the next interval. The files it knows, and the token of the changes after them, are saved in the volume's cache, after the first listing, at most every five minutes while they change, and when it stops. So a restart applies what changed since, rather than listing the drive again, unless Google doesn't accept the token.

In the registry, its URL is `gdrive://<name>`, and the `filer` command creates one with `volume-create-gdrive`, signing in with Google, in the browser, when it needs to.

## RSS Feeds

A volume of an RSS feed's items, read-only. It's an example of a pluggable backend: it isn't built into the registry, and the `filer` server adds it with `registry.WithBackend(rss.Backend())`.

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
* **Cache:** the feed's body is saved in the volume's cache as it's fetched, with its ETag and Last-Modified. A restart reads it from there, as old as when it was saved, so it isn't fetched again until it would have been, and then with its ETag, so an unchanged feed is a 304. A feed that can't be read isn't saved, and one that can't be saved is still read. The items' pages are kept in memory, as before.
* **Errors:** a feed that isn't found, or isn't RSS, is `ErrBadParameter`. One that can't be reached is `ErrServiceUnavailable`, so the volume is offline and retried.

## Calendar Subscriptions

A volume of an iCalendar feed's events, read-only, such as a calendar's "Subscribe" link.

* **URL:** `webcal://<name>/<host>/<path>`, of the feed `https://<host>/<path>`. `filer volume-create-webcal holidays webcal://www.gov.uk/bank-holidays/england-and-wales.ics` creates one, with `--refresh-interval` and `--description`.
  * The CLI and the web UI take a `webcal://`, `https://` or `http://` link. `webcal://` is fetched as https.
  * `insecure=true` fetches it over http instead.
  * Query parameters that aren't the volume's options belong to the feed's URL, such as a private link's `?key=...`.
* **Layout:** like Google Calendar's: a folder of each year and month, of the day each event starts, in its own zone: `/2026/2026-12/25-<title>.ics`.
  * A repeating event is one file, at its first start, with its changed occurrences (their `RECURRENCE-ID`s).
  * Events without a start, such as a task that isn't due, are in `/undated/`.
  * Events of the same title and day are numbered, in the order of their UIDs.
* **Content:** each event is an iCalendar of the feed's own lines: the calendar's properties, the `VTIMEZONE`s its times use, and the event's lines, with its changed occurrences and alarms. Nothing is re-encoded, so properties go-metadata doesn't decode, such as `RDATE`, are kept.
  * Its `DTSTAMP` is its `LAST-MODIFIED` or `CREATED`, or else the feed's, and isn't part of its ETag. Many feeds stamp every event with the time they're fetched, so this keeps unchanged events from being updated.
  * go-metadata decodes the events only for their paths and metadata.
* **Metadata:** `dc:title`, `dc:identifier` (its UID) and `webcal:calendar` (the calendar's name). The rest is go-metadata's, from the iCalendar.
* **Options:**
  * `refresh_interval`: how long a fetched feed is kept, and how often it's polled. `5m` by default.
  * `description`: by default, the calendar's name (`X-WR-CALNAME`), or its URL.
* **Watching:**
  * The feed is polled at that interval, with its last ETag and Last-Modified, so an unchanged feed is a 304, not downloaded again.
  * Changes are sent by the events' UIDs: new, gone and changed events, and events whose title or day changed, as for RSS. They're of the events the watch last sent, so a feed that a read fetched between polls is still sent.
  * Folders are created and deleted as events appear in, and leave, their months.
* **Cache:** the feed is kept in the volume's cache, not in memory. Memory holds each event's path, metadata, and where its lines are in the cached feed, and a read renders the event from there.
  * A fetched feed is saved to whichever of two cache slots isn't current, then read from there. It replaces the current feed only once it's a whole calendar.
  * A restart reads it from the cache, with its ETag and Last-Modified, so it isn't fetched again until it would have been.
  * A feed that's gone from the cache, such as when the cache is cleared, is fetched again when it's read.
* **Errors:** a feed that isn't found, or isn't an iCalendar (such as a sign-in page), is `ErrBadParameter`, as is one that's cut off, or whose components don't end as they began, so it doesn't replace the saved feed. One that can't be reached is `ErrServiceUnavailable`, so the volume is offline and retried.
