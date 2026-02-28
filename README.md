# go-filer

[![Go Reference](https://pkg.go.dev/badge/github.com/mutablelogic/go-filer.svg)](https://pkg.go.dev/github.com/mutablelogic/go-filer)
[![License](https://img.shields.io/badge/license-Apache-blue.svg)](LICENSE)

A file storage server and Go SDK with a unified HTTP API across local disk, S3-compatible object stores, and in-memory storage.

## Features

- **Multiple backends**: `file://` (local disk), `s3://` (AWS S3 and S3-compatible), `mem://` (in-memory)
- **HTTP API server**: REST endpoints for listing, uploading, downloading, and deleting objects with SSE streaming for uploads
- **CLI**: List, upload, download, head, and delete objects against a running server
- **Go SDK**: Typed client library (`pkg/httpclient`) for embedding filer into Go applications
- **Conditional create**: `If-None-Match: *` support to prevent overwriting existing objects
- **User metadata**: Arbitrary key-value metadata on objects, preserved across backends
- **Pagination**: Offset/limit pagination with count-only mode
- **Concurrent uploads**: Parallel multi-file uploads with per-file progress callbacks
- **Docker image**: Multi-arch (`linux/amd64`, `linux/arm64`) image published to `ghcr.io/mutablelogic/filer`

## Quick Start

### Server

Run the server with a local data directory mounted at `/data`:

```bash
docker volume create filer-data
docker run -d --name filer \
  -v filer-data:/data -p 8087:8087 \
  ghcr.io/mutablelogic/filer
```

The server defaults to `run file://data/data`, which creates a backend named `data` rooted at `/data`. Override the listen address or UID/GID as needed:

```bash
docker run -d --name filer \
  -v /host/path:/data -p 8087:8087 \
  -e FILER_UID=1234 -e FILER_GID=1234 \
  ghcr.io/mutablelogic/filer
```

### Build from source

```bash
git clone https://github.com/mutablelogic/go-filer
cd go-filer

# Full binary (server + client)
make build

# Client-only binary (no server component)
make filer-client
```

Binaries are placed in `build/filer`. Pre-built client-only binaries for Linux, macOS, and Windows are also available on the [releases page](https://github.com/mutablelogic/go-filer/releases).

## Server Configuration

### Backends

One or more backend URLs are passed as positional arguments to `filer run`. Multiple backends can be registered simultaneously.

| Scheme | Example | Description |
|--------|---------|-------------|
| `file://` | `file://mydata/var/data` | Local filesystem. Name is `mydata`, path is `/var/data` |
| `s3://` | `s3://my-bucket/pre/fix` | AWS S3 or S3-compatible rooted at a specific prefix |
| `mem://` | `mem://cache` | In-memory (data lost on restart) |

```bash
# Two backends: a local disk backend and an S3 backend
filer run file://local/var/data s3://my-bucket/
```

### AWS / S3 credentials

| Flag | Env Variable | Description |
|------|-------------|-------------|
| `--aws.profile` | `AWS_PROFILE` | AWS credentials profile (SSO, assume-role) |
| `--aws.access-key` | `AWS_ACCESS_KEY_ID` | Static access key |
| `--aws.secret-key` | `AWS_SECRET_ACCESS_KEY` | Static secret key |
| `--aws.session-token` | `AWS_SESSION_TOKEN` | Session token for temporary credentials |
| `--aws.region` | `AWS_REGION` | AWS region |
| `--aws.endpoint` | — | Custom S3-compatible endpoint, e.g. `http://localhost:9000` |

Credential priority: `--aws.profile` → `--aws.access-key` → anonymous.

### HTTP

| Flag | Env Variable | Default | Description |
|------|-------------|---------|-------------|
| `--http.addr` | `FILER_ADDR` | `:8087` | HTTP listen address |
| `--http.prefix` | — | `/api/filer` | HTTP path prefix |
| `--http.timeout` | — | `30s` | Request timeout |
| `--http.origin` | — | (same-origin) | CORS origin (`*` to allow all) |

## CLI Commands

Released binaries are client-only. The full binary (built with `make build`) includes the `run` server command. All client commands point at a running server via `--http.addr`.

### Server

| Command | Description | Example |
|---------|-------------|---------|
| `run [backend...]` | Start the HTTP server | `filer run file://data/var/data` |

### Client

| Command | Description | Example |
|---------|-------------|---------|
| `backends` | List registered backends | `filer backends` |
| `list <backend>` | List objects in a backend | `filer list data --path /docs --recursive` |
| `head <backend> <path>` | Print object metadata | `filer head data /docs/report.pdf` |
| `get <backend> <path>` | Download an object to stdout | `filer get data /docs/report.pdf > report.pdf` |
| `upload <backend> <path>` | Upload a file or directory | `filer upload data /docs ./local-dir` |
| `download <backend> <path>` | Download objects to a local directory | `filer download data /docs ./output` |
| `delete <backend> <path>` | Delete an object or path prefix | `filer delete data /docs/old.txt` |

## REST API

All endpoints are prefixed with `--http.prefix` (default `/api/filer`).

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/filer` | List registered backends |
| `GET` | `/api/filer/{backend}` | List objects (query: `path`, `recursive`, `offset`, `limit`) |
| `PUT` | `/api/filer/{backend}/{path}` | Upload an object |
| `GET` | `/api/filer/{backend}/{path}` | Download an object |
| `HEAD` | `/api/filer/{backend}/{path}` | Get object metadata |
| `DELETE` | `/api/filer/{backend}/{path}` | Delete an object or path prefix |

Object metadata is returned in the `X-Object-Meta` response header as a JSON blob. Multi-file uploads use SSE (Server-Sent Events) to stream per-file progress events.

## Go SDK

```go
import httpclient "github.com/mutablelogic/go-filer/pkg/httpclient"

c, err := httpclient.New("http://localhost:8087/api/filer")

// List objects
resp, err := c.ListObjects(ctx, "data", schema.ListObjectsRequest{
    Path:      "/docs",
    Recursive: true,
    Limit:     100,
})

// Upload
obj, err := c.CreateObject(ctx, "data", schema.CreateObjectRequest{
    Path:        "/docs/hello.txt",
    Body:        strings.NewReader("hello world"),
    ContentType: "text/plain",
})

// Download
obj, err := c.ReadObject(ctx, "data", schema.ReadObjectRequest{Path: "/docs/hello.txt"},
    func(chunk []byte) error {
        _, err := os.Stdout.Write(chunk)
        return err
    })

// Delete
obj, err := c.DeleteObject(ctx, "data", schema.DeleteObjectRequest{Path: "/docs/hello.txt"})
```

## Architecture

```
cmd/filer/          CLI + HTTP server binary
pkg/backend/        Storage backends (file, s3, mem) via gocloud.dev/blob
pkg/httpclient/     Typed Go HTTP client
pkg/httphandler/    HTTP handler registration + request/response encoding
pkg/manager/        Backend registry and lifecycle management
pkg/schema/         Shared request/response types
pkg/version/        Build metadata
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
