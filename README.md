# go-filer

A backend stores the objects in a volume. Each backend implements `backend.Backend`, and must behave the same way, so that a volume can be stored on the local filesystem (`file://`) or in S3 without clients seeing a difference. The semantics follow S3, and the filesystem backend emulates S3 where they differ.

Only the filesystem backend is implemented. This document records how an S3 backend maps onto the interface, and where the backends cannot behave identically.

## Object Model

- An object is a file, identified by its path within the volume, which starts with `/`. The root of the volume is `/`.
- A directory exists when an object exists beneath it. Directories have no size or modification time.
- The key of a file is its path, and the key of a directory is its path followed by `/`. Listings are in the byte order of keys, as in S3.
- Empty directories are pruned by the filesystem backend, as an S3 prefix with no objects beneath it does not exist.
- Files which are not regular files, such as sockets, named pipes and devices, are not objects.
- Hidden files and directories, whose names start with `.`, are not objects.

## Operations

| Operation | Filesystem | S3 |
| ----------- | ------------ | ---- |
| `GetObject` of a file | `stat` | `HeadObject` |
| `GetObject` of a directory | `stat` | `HeadObject` returns 404, then `ListObjectsV2` with `Prefix="dir/"` and `MaxKeys=1` to check the directory exists |
| `ListObjects` | Walk the directory, in key order | `ListObjectsV2` with `Delimiter="/"`, or no delimiter when recursive. Directories are the `CommonPrefixes` |
| `ListObjects` with `StartAfter` | Seek within each directory | `StartAfter`, dropping a `CommonPrefix` equal to it |
| `ListObjects` of a missing path | 404 | An empty listing, so an extra request is needed to return 404 |
| `DeleteObjects` of a file | `remove` | `HeadObject` for the metadata, and to return 404 as `DeleteObject` succeeds for missing keys, then `DeleteObject` |
| `DeleteObjects` of a directory | `RemoveAll` | List the prefix, then `DeleteObjects` in batches of 1000. The count is the number of keys deleted |
| Remove empty parent directories | After each delete | Not needed, as empty prefixes do not exist |

## Modification Time

The modification time of an object is stored in the `x-amz-meta-mtime` metadata of an S3 object, which is used in preference to `LastModified`. `LastModified` is the time the object was uploaded, and cannot be set, so it is only used when the metadata is not set.

`ListObjectsV2` does not return metadata, so a listing from S3 has the `LastModified` time, which can differ from the modification time returned by `GetObject`.

## Differences

These cannot be made identical, and are known differences between the backends:

- **Keys.** S3 allows keys which cannot be represented as paths, such as those containing `//`, `.` or `..` elements, `\` or `:`, or a trailing `/`, and a file and a directory with the same name, such as `a` and `a/b`. The S3 backend skips these keys in listings, so that they are not objects, in the same way as hidden files.
- **Directory markers.** Zero-byte objects whose keys end in `/` are created by the S3 console and some tools to represent directories. They are not objects, and are skipped in listings.
- **Atomicity.** Deleting a directory in S3 takes several requests, and can fail part of the way through. Deleting a directory on the filesystem is not atomic either.
- **ETag.** S3 returns an ETag for each object. The filesystem backend does not set it.

## Errors

Backend errors are converted to HTTP errors by `omodie.HTTPErr`. The filesystem backend returns `fs` errors, such as `fs.ErrNotExist`. The S3 backend converts S3 errors in the same way:

| S3 error | Error | HTTP status |
| ---------- | ------- | ------------- |
| `NoSuchKey`, `NoSuchBucket` | `fs.ErrNotExist` | 404 |
| `AccessDenied` | `omodie.ErrForbidden` | 403 |
| `SlowDown`, `ServiceUnavailable` | `omodie.ErrServiceUnavailable` | 503 |
