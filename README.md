# go-filer

[![Go Reference](https://pkg.go.dev/badge/github.com/mutablelogic/go-filer.svg)](https://pkg.go.dev/github.com/mutablelogic/go-filer)
[![Test](https://github.com/mutablelogic/go-filer/actions/workflows/test.yaml/badge.svg)](https://github.com/mutablelogic/go-filer/actions/workflows/test.yaml)

Filer puts object-oriented data sources behind one file system.

- **Sources:** local file systems, S3 buckets, Google Workspace (Drive, Calendar, Contacts and Tasks) and RSS feeds are each a volume of objects and folders.
- **Indexed:** each volume is indexed, and its objects' metadata enriched, such as a photo's camera and place, a song's artist, or a document's title.
- **Search:** search files, tasks, events, and media using rich metadata queries from the index.
- **Interfaces:** a web application, a command line, and a REST API.

## Quick Start

> [!WARNING]
> **Don't run filer on the internet.** It has no authentication yet, so anyone who can reach it can read, and change, your volumes and credentials. Publish its port on `localhost` only, as below, or put it behind a proxy that signs people in, or a VPN.

Run the server, with a directory of yours mounted, such as `/mnt/media`, as a volume, `media`, whose files are indexed, and their metadata read:

```bash
docker volume create filer

docker run -d --name filer -p 127.0.0.1:8084:8084 \
  -v filer:/data -v /mnt/media:/media:ro \
  ghcr.io/mutablelogic/filer run --volume "file://media/media?index=metadata"
```

Then open [http://localhost:8084/](http://localhost:8084/).

- **`--volume`:** the URL of a volume, whose host is its name, and whose path is in the container, which is created, or updated, each time the server starts. It can be repeated.
- **Add Volume** adds more, in the web UI, such as a **Server filesystem** of another directory which is mounted.

![The Volumes page: a tile of each volume, of its backend, and how many objects it has](docs/volumes.png)

Choose a volume to browse its folders and files, and a file to see its metadata, preview it, play it, or download it:

![A volume's folders and files, and a film's metadata, of its file and TMDB](docs/objects.png)

- **Data:** credentials and the index are kept in `/data/filer`, and volumes' caches in `/data/cache`.
- **User:** the server runs as the `filer` user. Set `UID` and `GID`, such as to the owner of the mounted directories, which `/data` is made the owner of.
- **Docker Compose:** [docker/compose.yaml](docker/compose.yaml) runs it with its variables, such as `TMDB_TOKEN` and `ACOUSTID_KEY`, which look up films and recordings, and `FILER_TLS_NAME`, behind a proxy which serves it over TLS: `docker compose -f docker/compose.yaml up -d`.
- **Image:** `ghcr.io/mutablelogic/filer`, for `linux/amd64` and `linux/arm64`, is published with each release, built with cgo, so it reads the metadata of audio, video and photos.

For the command line:

```bash
CGO_ENABLED=0 go install -tags client github.com/mutablelogic/go-filer/cmd/filer@latest
filer volume-create-fs media /media -o index=metadata
filer volumes
filer search beethoven
```

It talks to `localhost:8084`, or `FILER_ADDR`. Inside the container, `docker exec filer filer volumes` does the same.

## Adding Google Workspace

Google Drive, Calendar, Contacts and Tasks are volumes too, which sign in with Google, as you, once you've an OAuth client of your own:

1. **Create a project** in the [Google Cloud console](https://console.cloud.google.com/), and enable the APIs of the volumes you want: the Drive API, the Google Calendar API, the People API (Contacts) and the Google Tasks API.
2. **Set up the consent screen** (Google Auth Platform), as an external app in testing, with yourself as a test user.
3. **Create an OAuth client**, of the type **Web application**, whose authorized redirect URIs are:
   - `http://localhost:8084/api/credential-signin/callback`, of the web UI, or `https://<host>/api/credential-signin/callback`, as it's reached (Google only allows `http` of `localhost`);
   - `http://localhost:8085/callback`, of the command line.

   Download its JSON.
4. **Store it** as the server's `GOOGLE_CLIENT` credential:

   ```bash
   docker exec -i filer filer credential-set GOOGLE_CLIENT --json < client_secret.json
   ```

5. **Add a volume**, with **Add Volume**, such as **Google Drive**, and **Sign in with Google**, in the window which opens. Or, with the command line, `filer volume-create-gdrive drive`, which signs in, in the browser.

One sign-in, `GOOGLE_TOKEN`, is shared by the Google volumes, and asks for each one's access as it's added. See [Google](docs/backends.md#google) for each volume's layout, options and writing.

## Architecture

```mermaid
flowchart LR
    ui[Web UI] --> api
    cli[Command line] --> api
    rest[REST API clients] --> api
    api[HTTP API] --> registry[Registry]
    registry --> volumes
    subgraph volumes[Volumes]
        fs[Filesystem]
        s3[S3]
        google[Google Workspace]
        rss[RSS feeds]
    end
    registry --> indexer[Indexer]
    indexer --> volumes
    indexer --> store[(Store: objects, metadata, search)]
    indexer --> enrich[Metadata task, of go-metadata]
    enrich --> volumes
    registry --> changes[Changes] --> ui
```

- **[Backends](docs/backends.md):** each source, of a URL's scheme, such as `file`, `s3`, `gdrive` or `rss`: its layout, options, writing and watching.
- **[Architecture](docs/architecture.md):** the registry of volumes, the indexer and its store, metadata enrichment and search, the volumes' caches, actions, and watching changes.
- **[Development](docs/development.md):** the Go packages, building with go-media's libraries, testing, using them as a library, the object model, and errors.
- **[Web UI](frontend/README.md):** the frontend, which the server embeds.

## Roadmap

TODO: milestones of these themes, from the [issue list](https://github.com/mutablelogic/go-filer/issues):

- **File browser:** upload and delete ([#216](https://github.com/mutablelogic/go-filer/issues/216)), sorting ([#218](https://github.com/mutablelogic/go-filer/issues/218)), more of a preview ([#219](https://github.com/mutablelogic/go-filer/issues/219)), and its packages ([#148](https://github.com/mutablelogic/go-filer/issues/148), [#149](https://github.com/mutablelogic/go-filer/issues/149), [#220](https://github.com/mutablelogic/go-filer/issues/220)).
- **Indexing:** failed files ([#217](https://github.com/mutablelogic/go-filer/issues/217)), artwork ([#146](https://github.com/mutablelogic/go-filer/issues/146)) and content ([#147](https://github.com/mutablelogic/go-filer/issues/147)).
- **Search:** its UI ([#150](https://github.com/mutablelogic/go-filer/issues/150)), filters, places and events ([#138](https://github.com/mutablelogic/go-filer/issues/138)–[#142](https://github.com/mutablelogic/go-filer/issues/142)), and saved searches ([#151](https://github.com/mutablelogic/go-filer/issues/151)).
- **Backends:** Gmail ([#144](https://github.com/mutablelogic/go-filer/issues/144)), MCP servers ([#145](https://github.com/mutablelogic/go-filer/issues/145)), Drive's moves ([#174](https://github.com/mutablelogic/go-filer/issues/174)), and a Postgres store ([#72](https://github.com/mutablelogic/go-filer/issues/72)).
- **Authentication,** so it can be run beyond localhost.

## License

go-filer is licensed under the [Apache License, Version 2.0](LICENSE).

### Third-party libraries

Built with cgo, as `make` and the Docker image are, the `filer` command links go-media's libraries, of the LGPL build of its release, which read the metadata of audio, video and photos. A build of `CGO_ENABLED=0` has none of them. See go-media's [NOTICE](https://github.com/mutablelogic/go-media/blob/v1.11.5/NOTICE) for their versions, copyrights and sources.

| Library | License |
| --- | --- |
| [FFmpeg](https://ffmpeg.org/) | LGPL version 3 or later, of its LGPL build |
| [libexif](https://libexif.github.io/) | LGPL version 2.1 or later |
| [libheif](https://github.com/strukturag/libheif) | LGPL version 3 |
| [LibRaw](https://www.libraw.org/) | LGPL version 2.1, or CDDL version 1.0 |
| [Chromaprint](https://acoustid.org/chromaprint) | LGPL version 2.1, as a whole (its own code is MIT) |
| [libfvad](https://github.com/dpirch/libfvad) | BSD 3-clause, with Google's patent grant |

They're linked statically, so under the LGPL, the source of this program, and its `Makefile`, which links go-media's libraries, or a build of them of your own, of `GO_MEDIA_PREFIX`, let you relink it with a modified version of them.

The libraries they link with, in turn, are the system's, which the Docker image installs from Debian, each with its copyright in `/usr/share/doc/<package>/copyright`:

| Library | License |
| --- | --- |
| [libde265](https://github.com/strukturag/libde265) | LGPL version 3 |
| [libaom](https://aomedia.googlesource.com/aom/) | BSD 2-clause, with the AOMedia patent license |
| [dav1d](https://code.videolan.org/videolan/dav1d) | BSD 2-clause |
| [libxml2](https://gitlab.gnome.org/GNOME/libxml2) | MIT |
| [OpenSSL](https://www.openssl.org/) | Apache License 2.0 |
| [zstd](https://github.com/facebook/zstd) | BSD 3-clause, or GPL version 2 |
| [XZ Utils](https://tukaani.org/xz/) (liblzma) | 0BSD |
| [zlib](https://zlib.net/) | zlib License |
| GCC's runtime (libstdc++, libgcc, libgomp) | GPL version 3, with the GCC Runtime Library Exception |
| [glibc](https://www.gnu.org/software/libc/) | LGPL version 2.1 or later |

The web UI, which the server embeds, includes:

| Package | License |
| --- | --- |
| [Carbon Design System](https://carbondesignsystem.com/): web components, charts, icons and styles | Apache License 2.0 |
| [Lit](https://lit.dev/) | BSD 3-clause |
| [marked](https://marked.js.org/) | MIT |
| [DOMPurify](https://github.com/cure53/DOMPurify) | Apache License 2.0, or MPL 2.0 |
| [IBM Plex](https://www.ibm.com/plex/) Sans and Mono | SIL Open Font License 1.1 |
| [go-server](https://github.com/mutablelogic/go-server)'s frontend | Apache License 2.0 |
