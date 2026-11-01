# Filer

## Volume Operations

A volume is a backend which stores objects, which is registered with a `name`, and created from a URL, such as `file://media/mnt/media`, for a directory, whose scheme is one which the server supports.

### GET /volume

List the volumes which are registered, in order of their names, with the `url` of each one's backend, which includes the options which aren't the defaults, such as `readonly=true`, and which a volume can be added from again. A volume which is being added or removed isn't listed. `schemes` is the schemes of the URLs which volumes can be added from, such as `file`.

### POST /volume

Create a volume with a `name`, which is an identifier, and the `url` of its backend, whose host is the name, and whose scheme is one of the `schemes`. A `file` URL is a directory on the server, such as `file://media/mnt/media`, whose path is absolute, and which exists, or `file://media/C:/media` on Windows. Its query is the options of the volume:

- `readonly`, which is `true` or `false`, so objects can only be read, even when the directory can be written
- `skip`, which can be repeated, the regular expressions of the names of files and directories which aren't listed, in place of the defaults, which skip hidden files, and files which aren't content
- `package`, which can be repeated, the regular expressions of the names of directories which are listed as files, in place of the defaults, which are macOS bundles and sparse disk images

The volume is watched for changes from when it's created. Returns the volume, with the URL of its backend, which has the options which aren't the defaults, with 201. Returns 400 if the request body, name or URL is invalid, such as when the scheme isn't supported, the host isn't the name, the path isn't absolute, or isn't a directory, or an option is unknown or invalid, 409 if a volume with the name exists, or is being added, or 503 if the registry is closed.

### GET /volume/{name}

Get the volume `name`, with the `url` of its backend, which includes the options which aren't the defaults, and which the volume can be created again from. Returns 404 if the volume isn't mounted, or is being added or deleted.

### PATCH /volume/{name}

Update the options of the volume `name`, with an object of the options to change, by their keys, whose values are JSON: `null` removes the option, so it's the default, a list replaces its values, where an empty list has none, which for `skip` and `package` is none of them, and a string, number or boolean replaces its values with the one. For example, `{"readonly": true, "skip": null}` makes the volume read-only, and skips the default names. The options which aren't in the object are kept. The volume's URL, with the options, is checked first, so the volume isn't changed when they're invalid. Then the volume is closed, waiting for the requests which are using it, which are cancelled, and opened again with the options, which watches it again. Its name can't be used for another volume meanwhile. When it can't be opened with the options, it's opened as it was. Returns the volume, with the URL of its backend. Returns 400 if the request body is invalid, has no options, or an option is unknown or invalid, 404 if the volume isn't mounted, or is being added or deleted, or 503 if the registry is closed.

### DELETE /volume/{name}

Delete the volume `name`, which stops watching it, and closes its backend, once the requests which are using it, which are cancelled, have finished, which it waits for, and releases what it used, such as its directory, whose files aren't deleted. The volume can't be used once it's being deleted, and its name can't be used for another volume until it's deleted. Returns the deleted volume, with the URL of its backend, which it can be created again from. Returns 404 if the volume isn't mounted, or is being deleted.

## Object Operations

An object is a file or a directory in a volume, which is identified by the volume's `name` and its `path`, which starts with `/`, the root of the volume. The key of a file is its path, and the key of a directory is its path followed by `/`.

### GET /object/{name}

List the objects in a directory of the volume `name`, a page at a time. The `path` query parameter is the directory to list, which defaults to the root of the volume, and can be a file, when only it is returned. Only the objects immediately within the directory are returned, including directories, unless `recursive=true`, when every object beneath the directory is returned, including directories. Hidden files, and files which aren't content, such as Microsoft Office owner files (`~$*`), editor backups (`*~`), `Thumbs.db` and `desktop.ini`, aren't listed, and a package, such as a macOS sparse disk image, is listed as a file, without the files in it.

Objects are returned in the same order as S3, by the byte order of their keys. A page has at most `limit` objects, which defaults to and is capped at 100. When there are more objects, `next` is the key of the last object, and the next page is returned by passing it as `start_after`. The total number of objects isn't returned. Returns 400 if the path is outside the volume, or the query is invalid, or 404 if the volume isn't mounted, or the path doesn't exist.
