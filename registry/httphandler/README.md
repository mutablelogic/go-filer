# Filer

## Volume Operations

A volume is a backend which stores objects, which is registered with a `name`, and created from a URL, such as `file://media/mnt/folder`, for a directory, whose scheme is one which the server supports.

### GET /volume

List the volumes which are stored, in order of their names, including those which aren't `enabled`, with when they were `created` and `modified`.

* **`url`** is the backend's, with each of its options, defaults too, such as `readonly=false`, its `index` option, and the names of its credentials, but not the credentials. A volume is created again from it when the server starts.
* **`index`** is that of a volume which is mounted and indexed, with `index=folders`, or `index=objects`, of its files too: its `mode`, its `status`, `scanning` or `ready`, the `folders` its scan has indexed, when it `started` and was `scanned`, and its last `error`.
* **`mounted`** is true once the server has mounted the volume, so its objects can be used, which an enabled volume is shortly after it's created, or updated.
* **`enabled: false` with an `error`** is a volume which is enabled, but offline, or can't be mounted. One whose user needs to sign in again stays so until `PATCH /volume/{name}`. Others are opened again by the server, after a while.
* **`schemes`** is the schemes of the URLs which volumes can be created from, such as `file` and `s3`.
* **`backends`** is what each is: its `scheme`, its `description`, such as `Server filesystem`, the media type of its `icon`, such as `image/svg+xml`, when it has one, which is `GET /backend/{scheme}/icon`, and its `defaults`, the options of a volume which aren't set, such as `readonly=true`.

### POST /volume

Create a volume with a `name`, which is an identifier, the `url` of its backend, and whether it's `enabled`, which it is by default. The volume is stored, and the server mounts it, when it's enabled, shortly after this returns. Its `url` whose `name` option, or otherwise its host, is the name, and whose scheme is one of the `schemes`. A `file` URL is a directory on the server, such as `file://media/mnt/folder`, whose path is absolute, and which exists, or `file://media/C:/folder` on Windows. An `s3` URL is a bucket, and the prefix of its objects, such as `s3://photos/2026`, whose name is the volume's, or its name option, which is needed when the bucket's name isn't an identifier, such as `s3://my.bucket/photos?name=photos`, and whose objects can be listed, got, read, written and deleted. A bucket's directories are the prefixes of its keys, before a `/`, and keys which aren't paths, such as `a//b` or `../c`, aren't listed. Its query is the options, which are dependent on the scheme:

| Scheme | Option | Value | Default |
| ------ | ------ | ----- | ------- |
| `file` | `readonly` | `true` or `false`, so objects can only be read, even when the directory can be written | `false` |
| `file` | `skip` | A regular expression of the names of files and directories which aren't listed, which can be repeated, in place of the defaults | Hidden files, and files which aren't content |
| `file` | `package` | A regular expression of the names of directories which are listed as files, which can be repeated, in place of the defaults | macOS bundles and sparse disk images |
| `s3` | `name` | The volume's name, which is an identifier | The bucket's name |
| `s3` | `region` | The bucket's region, such as `eu-west-1`, which it must be in, and which is in the volume's `url` | The bucket's region |
| `s3` | `profile` | The profile of the credentials and configuration, in `~/.aws/config` and `~/.aws/credentials` | `AWS_PROFILE`, or `default` |
| `s3` | `access-key-credential` | The name of the server's credential of the access key ID, such as `S3_KEY`, of its store, or its environment, which it looks up | The `profile`'s, or the default |
| `s3` | `secret-key-credential` | The name of the server's credential of the secret access key, such as `S3_SECRET` | |
| `s3` | `session-token-credential` | The name of the server's credential of the session token, such as `S3_TOKEN`, with the other two | |
| `s3` | `endpoint` | The `http` or `https` URL of a service which is compatible with S3, such as `http://localhost:9000` for MinIO | AWS |
| `s3` | `anonymous` | `true` or `false`, so requests aren't signed, without credentials, which is read-only | `false` |
| `s3` | `readonly` | `true` or `false`, so objects can only be read | `false` |
| any | `description` | What the volume is, such as `Family photos` | Of the backend, such as `Folder /mnt/media`, `S3 bucket photos/2026 in eu-west-1`, or `Google Tasks for ada@example.com` |
| `s3` | `seaweedfs-filer-grpc` | The `host:port` of the gRPC service of the SeaweedFS filer whose S3 gateway the `endpoint` is, such as `192.168.86.21:18888`, which is the filer's port plus 10000, so the bucket's objects are watched, by subscribing to the changes to the filer's metadata | Not watched |

A bucket is checked when its volume is created, with its credentials, which are the server's, of the names in its options, `access-key-credential`, `secret-key-credential` and `session-token-credential`, such as `S3_KEY` and `S3_SECRET`, which can't be used with `profile` or `anonymous`. The server's provider of credentials looks them up each time they're used: those it stores, such as with `filer credential-set S3_SECRET`, which the `filer` server keeps in files, encrypted, in `credentials`, in its folder of the user's data, with a passphrase of `--passphrase`, or one it makes, and keeps beside them, in `credentials.key`, or else its environment's. So a volume's `url` has their names, and never the credentials. A name can be of any credential of the server, such as a variable of its environment, which is sent to the volume's `endpoint`, so creating a volume is only for trusted users. A volume of an earlier version, whose options are placeholders of the names, such as `access-key-id=${S3_KEY}`, is read as them, and a credential itself in an option is an error. Otherwise its credentials are those of the `profile`, or the default ones, such as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the server's environment, a role, or none, when it's `anonymous`. When the bucket is in another region, and `region` isn't set, it's the bucket's region. The volume's `url` has the region, so it isn't redirected again.

Returns 400 if the name or URL is invalid, or an option is unknown or invalid, or the bucket isn't in its `region`, 401 when its user needs to sign in, with how, 403 if the bucket can't be accessed, with the credentials, or there are none, 404 if the bucket doesn't exist, or 409 if a volume with the name exists.

### GET /volume/{name}

Get the volume `name`, as `GET /volume` lists it. Returns 404 if the volume isn't stored.

### PATCH /volume/{name}

Update the options of the volume `name`, with an object of the options to change, by their keys, whose values are JSON: `null` removes the option, so it's the default, a list replaces its values. For example, `{"readonly": true, "skip": null}` makes the volume read-only, and skips the default names. The options which aren't in the object are kept, and so are its credentials' names, and its scheme, which can't be changed. A backend is created with the options, which checks them, so the volume isn't changed when they're invalid. The volume is stored, and the server mounts it again, shortly after this returns.

With `enabled=false` in the query, the volume is disabled, so the server unmounts it, keeping it, and its index, and with `enabled=true` it's enabled, so it's mounted. Without it, whether it's enabled is kept.

Without any options, `{}`, the volume is opened again, as it is. That brings an offline volume back, such as one whose user signed in again. Returns 401, with how to sign in as its `detail`, as `POST /volume` does, when its user needs to sign in, such as when their sign-in is revoked.

### DELETE /volume/{name}

Delete the volume `name`, and its index, from the store, and return it. The server then stops watching it, and closes its backend, once the requests which are using it, which are cancelled, have finished, and releases what it used, such as its directory, whose files aren't deleted. A volume of the same name isn't mounted until it's closed. Returns 404 if the volume isn't stored.

### GET /backend/{scheme}/icon

Get the icon of the backend of `scheme`, such as `file`, which represents it, such as in the web UI's Add Volume menu, as an image of its media type, such as `image/svg+xml`. Its `ETag` is of the image, which a browser keeps for a day, so it's 304 when it matches `If-None-Match`. Returns 404 if there's no backend of the scheme, such as one which isn't run, or it has no icon.

## Object Operations

An object is a file in a volume, or a directory under which the file is located.

### GET /object/{name}

List the objects in a directory of the volume `name`, a page at a time. The `path` query parameter is the directory to list, which defaults to the root of the volume, and can be a file, when only it is returned. A path which ends with `/` is a directory, even when a file has its name, as it can in an S3 bucket, and isn't found when it's a file, so the links to directories, and the redirects to their listings, end with `/`. Only the objects immediately within the directory are returned, including directories, unless `recursive=true`, when every object beneath the directory is returned, including directories.

With `type`, only the objects of a type are listed: a media type, such as `inode/directory`, or its major type, `inode`, or its subtype, `directory`. Parameters, such as `charset`, and case are ignored. A directory without a type of its own is `inode/directory`. The folders of an indexed volume, with `type=directory`, `inode`, or `inode/directory`, are listed from its index, once it's ready, rather than the backend.

Objects are returned in the same order as S3, by the byte order of their keys. A page has at most `limit` objects, which defaults to and is capped at 100. When there are more objects, `next` is the key of the last object, and the next page is returned by passing it as `start_after`. The total number of objects isn't returned. Returns 400 if the path is outside the volume, or the query is invalid, or 404 if the volume isn't mounted, or the path doesn't exist.

### GET /object/{name}/{path...}

Get the content of the file at `path` in the volume `name`, with the headers `HEAD` has, and its metadata as JSON in the `X-Meta` header. The file is shown in the browser, or with `download=true`, downloaded, as an attachment, with its name. When `Accept` has `text/markdown`, a file which isn't Markdown is read as it, such as a PDF, an Office document or a web page, of go-metadata, and the start of it, up to 256 KiB, is returned, as `text/markdown`, with `X-Markdown-Next`, where the rest starts, when there's more. Its entity tag is the file's, so it's 304 when it isn't modified, without reading it, and a file which can't be read as Markdown is 406. Its content isn't sniffed as another type, and HTML, SVG and other XML is sandboxed, so it can't run scripts with access to the API. A file which can seek, such as a file on disk, or an object in a bucket, which is requested again from the offset it seeks to, can be requested in ranges, with `Range`, which is 206, or 416 when the range isn't satisfiable, so a browser can seek in audio and video. A directory, or the root of the volume, redirects to its listing, with 307. Returns 304 when the file isn't modified since `If-Modified-Since`, or `If-None-Match` is `*`, without reading it, and 412 when `If-Match` isn't `*`, or `If-Unmodified-Since` fails, or the file changed while it was opened. Returns 400 if the path is outside the volume, or has `%2F`, or the query is invalid, 404 if the volume isn't mounted, or the file doesn't exist, such as a hidden file, or a file in a package, or 501 if the file is a package, such as a macOS sparse disk image, which can't be read.

### HEAD /object/{name}/{path...}

Get the metadata of the object at `path` in the volume `name`, which is a file, a directory, or the root of the volume, when it's empty, with its headers, and no body. Its metadata is JSON in the `X-Meta` header, and its type, size and modification time are its `Content-Type`, `Content-Length` and `Last-Modified`. A directory's type is `inode/directory`, and it has no size. A path is resolved, so `.` and `..` elements are removed, but its elements are separated with `/`, not `%2F`. Returns 304 when the object isn't modified since `If-Modified-Since`, or `If-None-Match` is `*`, and 412 when `If-Unmodified-Since` fails, as for a file. Returns 400 if the path is outside the volume, or has `%2F`, or 404 if the volume isn't mounted, or the object doesn't exist, such as a hidden file, or a file in a package.

### PUT /object/{name}/{path...}

Upload the request's body to the file at `path` in the volume `name`, which is created, with 201, or replaced, with 200, with the directories it's in. The file is replaced atomically, so it's either the file which was, or the whole of the body, and when the body ends before its `Content-Length`, it's 400, and the file isn't changed. With `If-None-Match: *`, a file which exists isn't replaced, and with `If-Match: *`, a file which doesn't exist isn't created, which are 412. The file's metadata is the request's `X-Meta-Data`, when it's set, which is a JSON array, such as `[{"namespace":"photo","key":"camera","value":"X100V"},{"key":"rating","value":5}]`, whose values are JSON, and which `GET` and `HEAD` return in `meta`, in `X-Meta`. Metadata which the index found has a `text`, for people, when it says more than the value, such as `5s` of a duration whose value is `5000000000`. A bucket stores it, as `x-amz-meta-<namespace>.<key>`, whose namespaces and keys are lowercase letters, numbers and `-`, up to 2 KB, but a directory can't, which is 501. The file's modification time is the request's `Last-Modified`, when it's set, which a bucket stores in the file's `x-amz-meta-mtime`, as S3 sets the time a file is written, which it is otherwise. A file in a bucket is written in one request, or in parts of 8 MiB, when it's larger, so it can be up to 78 GiB. Its type is from its extension, so a `Content-Type`, other than `application/octet-stream`, which isn't the type of its extension, is 501. Returns the file. Returns 400 if the path is outside the volume, has `%2F`, or is the root of the volume, or `If-None-Match` or `If-Match` isn't `*`, or both are set, 403 if the volume is read-only, 404 if the volume isn't mounted, 409 if a directory is at the path, 412 as above, or 422 if the file's name is skipped, such as a hidden file, or it's in a package.

### POST /object/{name}/{path...}

Upload files into the directory at `path` in the volume `name`, or its root, when it's empty, with a `multipart/form-data` body of `file` parts, whose file names are their paths in the directory, which can have directories, as a browser sends a directory's files, but not be outside it. The directory, and those of the files, are created. A file's modification time is its part's `Last-Modified` header, when it's set, which a browser doesn't set, and its metadata is its part's `X-Meta-Data`, or otherwise the request's, as for `PUT`, which applies to every file, as a browser can't set a part's. A file whose metadata isn't valid, or can't be stored, is skipped, but the request's which isn't valid is 400. Each file is read from the form as it's written, so they aren't buffered, and replaced atomically. A file which exists is skipped, unless `overwrite=true`, when it's replaced, as is a file which can't be stored, such as a hidden file, or one in a package, or one at a directory's path. Returns the files which were written, in `body`, and those which were skipped, and why, in `skipped`, where a file which exists has `exists`, with 201 when a file was written, and otherwise 200. An error which isn't a file's stops the upload, when the files written are kept. Returns 400 if the path is outside the volume, or has `%2F`, or the query is invalid, or the body isn't a multipart form, or ends before it should, 403 if the volume is read-only, 404 if the volume isn't mounted, or 409 if the path is a file.

### DELETE /object/{name}/{path...}

Delete the file at `path` in the volume `name`, or a directory, with `recursive=true`, and every object beneath it, including hidden files, which aren't listed. A package, such as a macOS sparse disk image, is deleted as a file, with the files in it, without `recursive`. The directories which are left empty are removed, as a directory exists while an object is beneath it. Returns the objects deleted, which are files, not directories. Returns 400 if the path is outside the volume, has `%2F`, is hidden, or is the root of the volume, or the query is invalid, 403 if the volume is read-only, 404 if the volume isn't mounted, or the object doesn't exist, or 409 if the object is a directory, and the request isn't recursive. A path which ends with `/` is a directory, even when a file has its name, as it can in a bucket, and isn't found when it's a file. In a bucket, a directory's keys are deleted a page of up to 1,000 at a time, so when one can't be, those before it, and the others of its page, are deleted, and a directory which is a key of its own, ending with `/`, isn't removed when the last file beneath it is.

### GET /artwork/{hash}

Get the image of the artwork of `hash`, which an object links to, as its `artwork` lists it, such as an album's cover, or a thumbnail, of its media type, such as `image/jpeg`. Its `ETag` is its hash, and as it never changes, a browser keeps it for a year, without checking it, so it's 304 when it matches `If-None-Match`. Returns 400 if the hash isn't a SHA-256 hash, in hex, and 404 if there's no artwork of it, such as once no object links to it.

### HEAD /artwork/{hash}

Get the headers of the artwork of `hash`, as `GET /artwork/{hash}` has them, without its image.

## Search

### GET /search

Returns the objects of the mounted volumes which match the `query`, best first, as `body`, with each one's `rank`, from 0 to 1, which is 1 of the best match, and `count`, how many match, of every page. A page is of `offset` and `limit`, which defaults to, and is capped at, 100.

The query is words, any of which match an object's name, its folders, or its metadata, such as its title: `arvo beethoven`. A quoted phrase is one term, `"spiegel im spiegel"`, a word which ends with `*` matches the start of a word, `beethov*`, and a word which starts with `-` excludes what matches it, `-symphony`. A quote which doesn't end is a bad request.

## Actions

An action is something a volume's backend can do, beyond reading and writing its objects, such as creating a task list. Each has a name, a title, a description, and a JSON schema of its input, and returns the object it created or changed. A filesystem has none. Actions aren't available while a volume is unmounted, or offline.

### GET /action

List the actions of every volume which is mounted, and online, in order of their volumes, and their names, each with its `volume`, and the schema of its input.

### GET /action/{name}

List the actions of the volume `name`, in order of their names. Returns 404 if the volume doesn't exist, and 503 when it's unmounted, or offline.

### POST /action/{name}/{action}

Call the action `action` of the volume `name`, with its input as the body, which is JSON of its schema, or `{}` when it's empty, and return the object it created or changed. Returns 400 when the input isn't valid, 404 when the volume, or the action, doesn't exist, 503 when the volume is unmounted, or offline, and 401, with how to sign in, when the volume's user needs to sign in again.

## Signing In

A volume whose backend needs its user to sign in, such as one of Google Tasks, which they haven't, or whose sign-in is revoked, is 401 on `POST /volume`, with how to sign in as its `detail`: the authorization `server`, the `client_id`, the `scopes`, and the `credential` which is stored once they've signed in. A client, such as the `filer` command, authorizes at a loopback address with PKCE, and sends the code to go-auth's `POST /credential/{key}/signin`, which exchanges it with the secret of the `GOOGLE_CLIENT` credential, and stores the sign-in as the `GOOGLE_*` credential.

## Changes

The changes to the objects of the volumes are streamed as they happen, as server-sent events, by go-server's `GET /changes`, as changes of the `filer` schema. The server serves it, or, when it shares its broadcaster with other modules, the module which owns it does.

- **A change** is `{"action":"create","schema":"filer","table":"media","row":"/music/a.mp3"}`: its action, which is `create`, `update` or `delete`, the volume, as the table, and the object's path, as the row.
- **Filtering:** `?schema=filer&table=media` streams the changes of one volume.
- **A rescan** is an `update` without a `row`, of a volume, or whose `row` ends with `/`, of a directory, whose changes may have been missed, such as when the client fell behind, or the volume was updated, so everything beneath it should be compared with what's known about it.
- **Buckets:** a volume of a bucket is only watched when its endpoint is a SeaweedFS filer's S3 gateway, with the `seaweedfs-filer-grpc` option, as otherwise S3 would need the bucket's event notifications, and a queue.
