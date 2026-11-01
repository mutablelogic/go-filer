# Filer

## Volume Operations

A volume is a backend which stores objects, which is registered with a `name`, and created from a URL, such as `file://media/mnt/folder`, for a directory, whose scheme is one which the server supports.

### GET /volume

List the volumes which are registered, in order of their names, with the `url` of each one's backend, which includes the options which aren't the defaults, such as `readonly=true`, and which a volume can be added from again. A volume which is being added or removed isn't listed. `schemes` is the schemes of the URLs which volumes can be added from, such as `file` and `s3`.

### POST /volume

Create a volume with a `name`, which is an identifier, and the `url` of its backend, whose `name` option, or otherwise its host, is the name, and whose scheme is one of the `schemes`. A `file` URL is a directory on the server, such as `file://media/mnt/folder`, whose path is absolute, and which exists, or `file://media/C:/folder` on Windows. An `s3` URL is a bucket, and the prefix of its objects, such as `s3://photos/2026`, whose name is the volume's, or its name option, which is needed when the bucket's name isn't an identifier, such as `s3://my.bucket/photos?name=photos`, and whose objects can be listed, got, read and written, but not deleted yet. A bucket's directories are the prefixes of its keys, before a `/`, and keys which aren't paths, such as `a//b` or `../c`, aren't listed. Its query is the options, which are dependent on the scheme:

| Scheme | Option | Value | Default |
| ------ | ------ | ----- | ------- |
| `file` | `readonly` | `true` or `false`, so objects can only be read, even when the directory can be written | `false` |
| `file` | `skip` | A regular expression of the names of files and directories which aren't listed, which can be repeated, in place of the defaults | Hidden files, and files which aren't content |
| `file` | `package` | A regular expression of the names of directories which are listed as files, which can be repeated, in place of the defaults | macOS bundles and sparse disk images |
| `s3` | `name` | The volume's name, which is an identifier | The bucket's name |
| `s3` | `region` | The bucket's region, such as `eu-west-1`, which it must be in, and which is in the volume's `url` | The bucket's region |
| `s3` | `profile` | The profile of the credentials and configuration, in `~/.aws/config` and `~/.aws/credentials` | `AWS_PROFILE`, or `default` |
| `s3` | `access-key-id` | The access key ID, or a placeholder of it, such as `${S3_KEY}`, which the server's provider of credentials expands | The `profile`'s, or the default |
| `s3` | `secret-access-key` | The secret access key, or a placeholder of it | |
| `s3` | `session-token` | The session token, or a placeholder of it, with the access key ID and secret access key | |
| `s3` | `endpoint` | The `http` or `https` URL of a service which is compatible with S3, such as `http://localhost:9000` for MinIO | AWS |
| `s3` | `anonymous` | `true` or `false`, so requests aren't signed, without credentials, which is read-only | `false` |
| `s3` | `readonly` | `true` or `false`, so objects can only be read | `false` |

A bucket is checked when its volume is created, with its credentials, which are those of the options, whose placeholders the server's provider of credentials expands, and which can't be used with `profile` or `anonymous`. A credential which isn't a placeholder is stored by the provider, once the bucket is checked with it, by a key of the volume's name and the option, with a random suffix, such as `S3_PHOTOS_SECRET_ACCESS_KEY_3F9A…`, so it doesn't replace another credential, and the volume uses its placeholder, so its `url` never has the credentials. A credential which starts with `$` is given with `$$`, such as `$$word` for `$word`, as otherwise it's a placeholder. A placeholder can be of any credential of the server, such as a variable of its environment, which is sent to the volume's `endpoint`, so creating a volume is only for trusted users. Otherwise its credentials are those of the `profile`, or the default ones, such as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the server's environment, a role, or none, when it's `anonymous`. When the bucket is in another region, and `region` isn't set, it's the bucket's region. The volume's `url` has the region, so it isn't redirected again.

Returns 400 if the name or URL is invalid, or an option is unknown or invalid, or the bucket isn't in its `region`, 403 if the bucket can't be accessed, with the credentials, or there are none, 404 if the bucket doesn't exist, or 409 if a volume with the name exists, or is being added.

### GET /volume/{name}

Get the volume `name`, with the `url` of its backend, which includes the options which aren't the defaults, and which the volume can be created again from. Returns 404 if the volume isn't mounted, or is being added or deleted.

### PATCH /volume/{name}

Update the options of the volume `name`, with an object of the options to change, by their keys, whose values are JSON: `null` removes the option, so it's the default, a list replaces its values. For example, `{"readonly": true, "skip": null}` makes the volume read-only, and skips the default names. The options which aren't in the object are kept. The volume's backend is cloned with the options, which are checked, so the volume isn't changed when they're invalid, and its credentials, which its `url` doesn't have, are kept.

### DELETE /volume/{name}

Delete the volume `name`, which stops watching it, and closes its backend, once the requests which are using it, which are cancelled, have finished, which it waits for, and releases what it used, such as its directory, whose files aren't deleted. The volume can't be used once it's being deleted, and its name can't be used for another volume until it's deleted.

## Object Operations

An object is a file in a volume, or a directory under which the file is located.

### GET /object/{name}

List the objects in a directory of the volume `name`, a page at a time. The `path` query parameter is the directory to list, which defaults to the root of the volume, and can be a file, when only it is returned. A path which ends with `/` is a directory, even when a file has its name, as it can in an S3 bucket, and isn't found when it's a file, so the links to directories, and the redirects to their listings, end with `/`. Only the objects immediately within the directory are returned, including directories, unless `recursive=true`, when every object beneath the directory is returned, including directories.

Objects are returned in the same order as S3, by the byte order of their keys. A page has at most `limit` objects, which defaults to and is capped at 100. When there are more objects, `next` is the key of the last object, and the next page is returned by passing it as `start_after`. The total number of objects isn't returned. When `text/html` is accepted before JSON, as a browser does, the page is HTML: a table of the objects, whose names are relative to the directory, with links to the contents of the files, the listings of the directories, and of the directories of the path, and to the next page, which keep the query. Unless the volume is read-only, an object can be deleted, and files uploaded into the directory, from the page, which asks to confirm deleting, and replacing files which exist. Returns 400 if the path is outside the volume, or the query is invalid, or 404 if the volume isn't mounted, or the path doesn't exist.

### GET /object/{name}/{path...}

Get the content of the file at `path` in the volume `name`, with the headers `HEAD` has, and its metadata as JSON in the `X-Meta` header. The file is shown in the browser, or with `download=true`, downloaded, as an attachment, with its name. Its content isn't sniffed as another type, and HTML, SVG and other XML is sandboxed, so it can't run scripts with access to the API. A file which can seek, such as a file on disk, or an object in a bucket, which is requested again from the offset it seeks to, can be requested in ranges, with `Range`, which is 206, or 416 when the range isn't satisfiable, so a browser can seek in audio and video. A directory, or the root of the volume, redirects to its listing, with 307. Returns 304 when the file isn't modified since `If-Modified-Since`, or `If-None-Match` is `*`, without reading it, and 412 when `If-Match` isn't `*`, or `If-Unmodified-Since` fails, or the file changed while it was opened. Returns 400 if the path is outside the volume, or has `%2F`, or the query is invalid, 404 if the volume isn't mounted, or the file doesn't exist, such as a hidden file, or a file in a package, or 501 if the file is a package, such as a macOS sparse disk image, which can't be read.

### HEAD /object/{name}/{path...}

Get the metadata of the object at `path` in the volume `name`, which is a file, a directory, or the root of the volume, when it's empty, with its headers, and no body. Its metadata is JSON in the `X-Meta` header, and its type, size and modification time are its `Content-Type`, `Content-Length` and `Last-Modified`. A directory's type is `inode/directory`, and it has no size. A path is resolved, so `.` and `..` elements are removed, but its elements are separated with `/`, not `%2F`. Returns 304 when the object isn't modified since `If-Modified-Since`, or `If-None-Match` is `*`, and 412 when `If-Unmodified-Since` fails, as for a file. Returns 400 if the path is outside the volume, or has `%2F`, or 404 if the volume isn't mounted, or the object doesn't exist, such as a hidden file, or a file in a package.

### PUT /object/{name}/{path...}

Upload the request's body to the file at `path` in the volume `name`, which is created, with 201, or replaced, with 200, with the directories it's in. The file is replaced atomically, so it's either the file which was, or the whole of the body, and when the body ends before its `Content-Length`, it's 400, and the file isn't changed. With `If-None-Match: *`, a file which exists isn't replaced, and with `If-Match: *`, a file which doesn't exist isn't created, which are 412. The file's modification time is the request's `Last-Modified`, when it's set, except in a bucket, where it's when it's written, as S3 sets it. A file in a bucket is written in one request, or in parts of 8 MiB, when it's larger, so it can be up to 78 GiB. Its type is from its extension, so a `Content-Type`, other than `application/octet-stream`, which isn't the type of its extension, is 501. Returns the file. Returns 400 if the path is outside the volume, has `%2F`, or is the root of the volume, or `If-None-Match` or `If-Match` isn't `*`, or both are set, 403 if the volume is read-only, 404 if the volume isn't mounted, 409 if a directory is at the path, 412 as above, or 422 if the file's name is skipped, such as a hidden file, or it's in a package.

### POST /object/{name}/{path...}

Upload files into the directory at `path` in the volume `name`, or its root, when it's empty, with a `multipart/form-data` body of `file` parts, whose file names are their paths in the directory, which can have directories, as a browser sends a directory's files, but not be outside it. The directory, and those of the files, are created. A file's modification time is its part's `Last-Modified` header, when it's set, which a browser doesn't set. Each file is read from the form as it's written, so they aren't buffered, and replaced atomically. A file which exists is skipped, unless `overwrite=true`, when it's replaced, as is a file which can't be stored, such as a hidden file, or one in a package, or one at a directory's path. Returns the files which were written, in `body`, and those which were skipped, and why, in `skipped`, where a file which exists has `exists`, with 201 when a file was written, and otherwise 200. An error which isn't a file's stops the upload, when the files written are kept. Returns 400 if the path is outside the volume, or has `%2F`, or the query is invalid, or the body isn't a multipart form, or ends before it should, 403 if the volume is read-only, 404 if the volume isn't mounted, or 409 if the path is a file.

### DELETE /object/{name}/{path...}

Delete the file at `path` in the volume `name`, or a directory, with `recursive=true`, and every object beneath it, including hidden files, which aren't listed. A package, such as a macOS sparse disk image, is deleted as a file, with the files in it, without `recursive`. The directories which are left empty are removed, as a directory exists while an object is beneath it. Returns the objects deleted, which are files, not directories. Returns 400 if the path is outside the volume, has `%2F`, is hidden, or is the root of the volume, or the query is invalid, 403 if the volume is read-only, 404 if the volume isn't mounted, or the object doesn't exist, or 409 if the object is a directory, and the request isn't recursive.

## Watch Operations

The changes to the objects of the volumes are streamed as they happen, as server-sent events.

### GET /watch

Stream the changes to the objects of every volume, or of the volumes of `name`, which can be repeated, as `text/event-stream`, including the volumes which are created while it's streamed. An event's name is the change, which is `created`, `updated` or `deleted`, of a file or directory, or `rescan`, of a directory whose changes may have been missed, such as when the client fell behind, or the volume was updated, so everything beneath it should be compared with what's known about it. Its data is the object's key, as JSON, such as `{"name":"media","path":"/music/a.mp3"}`. A `ping` event is sent when there are no changes, every ten seconds, so the connection is kept open. When the server is stopping, a `shutdown` event is sent, and the stream ends, within a second, even when the client stopped reading, when the event isn't sent. Returns 400 if the query is invalid, or 406 if the response can't be any of the accepted types.
