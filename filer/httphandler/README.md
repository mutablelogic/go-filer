# Filer

## Volume Operations

Volumes represent storage locations that can be used by the filer system to store and retrieve data. Each volume is identified by a name and can be managed through various operations such as create, get, list, delete, and update.

A volume URL identifies the storage backend for a volume. The scheme selects
the backend type, the host is the volume's name (must be a valid identifier:
starting with a letter, followed by letters, numbers, underscores or
hyphens), and the path and query string are backend-specific.

* `file://name/path` Mounts a local directory on disk. There are no query parameters.
* `s3://name/prefix?params` Mounts an Amazon S3 (or S3-compatible) bucket. The volume name must match the bucket name.

**File parameters are as follows:**

| Part | Description |
|------|-------------|
| `name` | Volume name |
| `path` | Absolute path to an existing local directory |

Example: `file://mydata/mnt/data`

**S3 parameters are as follows:**

| Part | Description |
|------|-------------|
| `name` | Volume name, and the S3 bucket name |
| `prefix` | Optional key prefix within the bucket under which objects are stored |
| params `region` | AWS region of the bucket (optional) |
| params `endpoint` | Custom S3-compatible endpoint URL (optional; enables path-style addressing) |
| params `anonymous` | Set to `true` to use unauthenticated access; ignored if credentials are set |
| params `access-key` | Key of a stored [credential](#tag/Credentials) holding the AWS access key ID |
| params `secret-key` | Key of a stored [credential](#tag/Credentials) holding the AWS secret access key |

If `access-key`/`secret-key` are not set and `anonymous` is not `true`, the default AWS credential chain is used (environment, shared config, instance role, etc).

Example: `s3://mybucket/myprefix?region=us-east-1&access-key=aws_access_key&secret-key=aws_secret_key`

### GET /volume

List volumes, optionally filtered and paginated.

### POST /volume

Create a new volume from a URL identifying its backend, along with volume metadata. Returns the created volume. Returns 400 if the URL is invalid or the backend scheme is unsupported, or 409 if a volume with that name already exists.

### GET /volume/{volume}

Get the status of a volume by name. Returns 404 if the volume does not exist.

### DELETE /volume/{volume}

Delete a volume by name. The volume must be unmounted before it can be deleted. Returns 404 if the volume does not exist, or 409 if it is still mounted.

### PATCH /volume/{volume}

Mount or unmount a volume, or change its parameters. Returns the updated volume. Returns 400 if the request body is invalid, or 404 if the volume does not exist.

### POST /volume/{volume}/reindex

Reindex objects in a volume matching the given filters. Returns 404 if the volume does not exist, or 503 if the volume is not mounted or has not yet been indexed.

## Object Operations

Objects are the fundamental units of data stored within volumes. The filer system provides operations to manage these objects, including listing, retrieving, and reindexing them based on specific filters.

### GET /object

List objects within a volume, optionally filtered by path prefix or type, and paginated. Returns 400 if the query is invalid, 404 if the volume does not exist, or 503 if the volume is not mounted.

The `type` filter can be set to:

| Value | Matches |
| ----- | ------- |
| `text/directory` | Directories, rather than objects |
| A full mimetype, eg. `image/jpeg` | Objects with that exact content type |
| A partial type, eg. `image` or `jpeg` | Objects whose content type has that major or minor type |

### HEAD /object/{volume}/{path...}

Get the metadata of an object as HTTP headers, without returning its content. Returns 400 if the volume or path is invalid, 404 if the volume or object does not exist, or 503 if the volume is not mounted.

Supports conditional requests via `If-Match`, `If-None-Match`, `If-Modified-Since` and `If-Unmodified-Since` headers: returns 304 if the object matches the conditions for a conditional GET, or 412 if it fails the conditions for a conditional update.

## Search operations

The filer system also provides search capabilities, allowing users to query objects based on various criteria such as metadata, content, and other attributes. This enables efficient retrieval of relevant data across multiple volumes.

### GET /search

Search indexed objects using a full-text query, optionally restricted to a set of volumes, and paginated. Returns 400 if the query is missing or invalid.

The `query` parameter is parsed as a PostgreSQL [`websearch_to_tsquery`](https://www.postgresql.org/docs/current/textsearch-controls.html#TEXTSEARCH-PARSING-QUERIES) expression, similar to the syntax used by most web search engines:

| Syntax | Meaning |
| ------ | ------- |
| `foo bar` | Matches objects containing both `foo` and `bar` |
| `foo or bar` | Matches objects containing `foo` or `bar` |
| `"foo bar"` | Matches objects containing the exact phrase `foo bar` |
| `-foo` | Excludes objects containing `foo` |

Unlike `to_tsquery`, punctuation such as unmatched quotes is tolerated rather than raising a syntax error, and parentheses/explicit `AND`/`NOT` operators are not supported as grouping constructs.

## Metadata Operations

The filer system allows users to manage metadata associated with objects. This includes operations to add, update, retrieve, and delete metadata, enabling better organization and searchability of stored data.

### POST /metadata

Extract metadata from an uploaded file, without storing it. The file is submitted as a `file` field in a `multipart/form-data` request, and the content type is sniffed from its content and filename extension. Returns the detected content type along with any format-specific metadata (eg. EXIF, ID3 tags). Returns 400 if the `file` field is missing or unreadable, or if the content could not be parsed.

## Artwork Operations

Artwork operations in the filer system refer to the management of thumbnails and other artwork representations of objects. Users can generate, retrieve, and manage artwork for objects to enhance the visual representation of stored data.

Artwork is content-addressed: its key is a SHA-256 hash of the image data, so uploading the same image twice returns the same artwork rather than creating a duplicate.

### POST /artwork

Upload an image and generate artwork (eg. a thumbnail) from it. The image is submitted as a `data` field in a `multipart/form-data` request. Returns the created artwork, or the existing artwork if the same image data was previously uploaded. Returns 400 if the image data is missing, unreadable, or in an unsupported format.

### GET /artwork/{etag}

Get artwork by its ETag, returning the raw image data with the appropriate content type. The response is cacheable indefinitely since artwork is immutable. Supports conditional requests via `If-None-Match` and `If-Modified-Since` headers, returning 304 if the artwork matches. Returns 404 if no artwork exists with that ETag.

## Large Language Model (LLM) Operations

LLM operations in the filer system involve the integration and management of large language models for various tasks such as natural language processing, text generation, and other AI-driven functionalities. Users can interact with LLMs to perform advanced data analysis and processing within the filer environment.

### POST /llmprovider

Create a new LLM provider, used for extracting metadata whilst indexing objects. The `provider` field must be one of `anthropic`, `gemini`, `mistral`, `ollama` or `openai`. The `url` field is only used for the `ollama` provider; every other provider is authenticated instead via the `credential` field, which references a stored [credential](#tag/Credentials) holding the API key. The provider is validated by connecting to it before it is saved. Returns 400 if the request is invalid, the provider type is unsupported, or the provider could not be reached, or 409 if a provider with that name already exists.
