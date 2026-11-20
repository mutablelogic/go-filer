# Web UI

go-filer's web UI modules, which any app can include, and the filer server's app, which is built from them.

- It's built on [`@mutablelogic/go-server`](https://github.com/mutablelogic/go-server/tree/main/frontend), from GitHub Packages, with Lit and Carbon's web components.
- An app is composed of **modules**. Each owns an item in the menu bar, and the sidebar while it's active.
- So far there's one module, Files: a tree of the volumes and their folders. A file view of a folder's objects comes next, and other views, such as a calendar, a map and a gallery, later.

## Modules

| Import | What it has |
|---|---|
| `@mutablelogic/go-filer/filer` | The filer API's types and client: `listVolumes`, `listObjects`, the schemas and tables of the volumes' and objects' changes, and helpers of objects, such as `objectTitle` and `subfolderCount` |
| `@mutablelogic/go-filer/tree` | `TreeController`, which keeps the volumes and the folders which are expanded, and loads what changes again |
| `@mutablelogic/go-filer/tree/view` | `gf-file-tree`, the tree, which fires `gf-file-tree-selected` with the volume and path of the folder which is chosen |
| `@mutablelogic/go-filer/files/view` | `gf-files`, the Files page, and `filesModule`, which adds it to an app |

Including Files in another app:

```ts
import { filesModule } from '@mutablelogic/go-filer/files/view';

filesModule({ nav, router, client, root: document.querySelector('main')!, notifications, changes });
```

- **The module:** adds the Files section to the app's `NavController`, its page to `root`, and its route to the `Router`.
- **The page:** a tree of the folders on the left, and the folder which is chosen on the right, whose file view comes next. The folder is in the location, `#files/<volume>/<path>`, so it can be linked to, and it's revealed in the tree when the page is opened at it.
- **The tree:** the volumes are its roots. One which isn't mounted can't be expanded, with its error as its title. A folder's subfolders are loaded when it's first expanded, 100 at a time, up to 1,000. A folder whose `folder:subfolder-count` is 0 has no caret. A folder's title is its `dc:title`, such as a task list's, or its name.
- **Changes:** the volumes are loaded again when they, or the registry's state of them, change. A folder which is created, updated or deleted loads its folder's subfolders again, and its folder's folder's, whose count of it changed. A rescan loads what it's of again. The subfolders of a folder which is collapsed are forgotten, and loaded again when it's expanded.

## Layout

| Folder | What it has |
|---|---|
| `src/filer` | The filer API's types and client |
| `src/tree` | The tree of folders, and its controller |
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
