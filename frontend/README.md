# Web UI

go-filer's web UI modules, which any app can include, and the filer server's app, which is built from them.

- It's built on [`@mutablelogic/go-server`](https://github.com/mutablelogic/go-server/tree/main/frontend), from GitHub Packages, with Lit and Carbon's web components.
- An app is composed of **modules**. Each owns an item in the menu bar, and the sidebar while it's active.
- So far there's one module, Files, which lists the volumes. A file tree and a file view of a volume's objects come next, and other views, such as a calendar, a map and a gallery, later.

## Modules

| Import | What it has |
|---|---|
| `@mutablelogic/go-filer/filer` | The filer API's types and client: `listVolumes`, and the schemas and tables of the volumes' and objects' changes |
| `@mutablelogic/go-filer/files/view` | `gf-files`, the Files page, and `filesModule`, which adds it to an app |

Including Files in another app:

```ts
import { filesModule } from '@mutablelogic/go-filer/files/view';

filesModule({ nav, router, client, root: document.querySelector('main')!, notifications, changes });
```

- **The module:** adds the Files section to the app's `NavController`, its page to `root`, and its route to the `Router`.
- **The page:** lists the volumes, with their URLs, and why each one which isn't mounted isn't. They're loaded again when the stored volumes change.

## Layout

| Folder | What it has |
|---|---|
| `src/filer` | The filer API's types and client |
| `src/files` | The Files module |
| `app` | The server's app: `index.html`, the shell, and `index.ts`, which adds the modules, which `npm run build-app` builds into `app/dist` |
| `embed.go` | Embeds `app/dist`, with the `frontend` tag |
| `cmd` | Serves the app, and `/config.json`, behind the `frontend` tag |

## Developing

| Command | Description |
|---|---|
| `make frontend` | Installs from `package-lock.json`, type checks, and builds the modules and the app |
| `make frontend-test` | Type checks, and runs the tests |
| `make` | Builds the UI, and then the commands, which embed it |

- **Token:** installing needs a token which can read GitHub Packages, in `GITHUB_TOKEN`, such as `$(gh auth token)`.
- **Node:** 22.18 or later.
- **Without the UI:** a server built without the `frontend` tag, such as with `go install`, serves its API and `/config.json`, but not the UI. `--no-ui` stops a server which has it from serving it.
