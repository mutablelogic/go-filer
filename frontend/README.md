# Web UI

go-filer's web UI modules, which any app can include, and the filer server's app, which is built from them.

- It's built on [`@mutablelogic/go-server`](https://github.com/mutablelogic/go-server/tree/main/frontend), from GitHub Packages, with Lit and Carbon's web components.
- An app is composed of **modules**. Each owns an item in the menu bar, and the sidebar while it's active.
- So far there's one module, Volumes, with a tree of a volume's folders, and a file view of a folder's objects. Other views, such as a calendar, a map and a gallery, come later.

## Modules

| Import | What it has |
|---|---|
| `@mutablelogic/go-filer/filer` | The filer API's types and client: `listVolumes`, `listObjects`, the schemas and tables of the volumes' and objects' changes, and helpers of objects, such as `objectTitle` and `subfolderCount` |
| `@mutablelogic/go-filer/volumes` | `VolumesController`, which keeps the volumes, and loads them again when they, or the registry's state of them, change |
| `@mutablelogic/go-filer/volumes/view` | `gf-volumes`, the Volumes page, `gf-volume`, a volume's page, and `volumesModule`, which adds them, and the sidebar, to an app |
| `@mutablelogic/go-filer/tree` | `TreeController`, which keeps the folders which are expanded, and loads what changes again |
| `@mutablelogic/go-filer/folder` | `FolderController`, which keeps a folder's objects, without its folders, a page at a time, of the listing's cursor, and loads them again when they change |
| `@mutablelogic/go-filer/folder/view` | `gf-file-view`, a table of a folder's objects, without its folders, which loads more as it's scrolled |
| `@mutablelogic/go-filer/tree/view` | `gf-file-tree`, a tree of a volume's folders, which fires `gf-file-tree-selected` with the volume and path of the folder which is chosen |

Including Volumes in another app:

```ts
import { navPage, volumesModule } from '@mutablelogic/go-filer/volumes/view';

volumesModule({ nav, router, client, root: document.querySelector('main')!, notifications, changes });
router.onRoute((route) => nav.setActive(navPage(route))).start();
```

- **The sidebar:** Volumes, with each volume indented beneath it, as go-server's `NavPage.parent`. A volume's label is its description, the `description` of its URL, or its name, which is cut short, when it's long, and its title, shown when it's pointed at, is the whole description, with its name and backend, such as `drive · Google Drive`. Its page has the same label. Volumes are added and removed as they're created and deleted. `navPage` returns the sidebar's page of a route, which is a volume's for any of its folders.
- **The Volumes page:** `#volumes` lists the volumes, with their URLs, and the error of each one which is offline, or can't be mounted, or that it isn't mounted. What's indexing will be shown here.
- **A volume's page:** `#volume/<name>/<path>` fills the window, with a tree of the volume's folders on the left, and the file view of the folder which is chosen on the right, each of which scrolls on its own, rather than the window. The folder is in the location, so it can be linked to, and it's revealed in the tree, also once the volume is mounted, such as after the server starts. A volume which isn't mounted shows why.
- **The tree:** its roots are the folders of the volume's root, which is the volume's page, without a folder. A volume without folders has no tree. A folder's subfolders are loaded when it's first expanded, 100 at a time, up to 1,000, which is at most how many are loaded. A folder whose `folder:subfolder-count` is 0 has no caret. A folder's title is its `dc:title`, such as a task list's, or its name.
- **The file view:** a table of the folder's objects, without its folders, which the tree has, with their types, sizes, and when they were changed, in the order the listing has them, 100 at a time. An object's type is its kind, `metadata:kind`, such as Book, or else the subtype of its type, such as mpeg, and its whole type when it's pointed at, of its cursor, as it's scrolled to its end. The listing can't leave out folders, so pages of only folders are skipped. Its header stays at the top. How many objects the folder has is beneath, once they're all loaded. More aren't loaded while they're loaded again. When they can't be loaded, it says so, with Try again, which loads them from where they couldn't be. A folder without objects has an icon, and Select a folder from the left. They're loaded again, as many as are shown, when an object in the folder changes, or the folder does, such as once it's indexed, or what it's in is rescanned.
- **Changes:** the volumes are loaded again when they, or the registry's state of them, change. A folder which is created, updated or deleted loads its folder's subfolders again, and its folder's folder's, whose count of it changed, and one which is updated, such as once it's indexed, its own. A deletion which doesn't say it's of a folder, as the filesystem's don't, is one when the tree has it. A rescan loads what it's of again. The subfolders of a folder which is collapsed are forgotten, and loaded again when it's expanded.

## Layout

| Folder | What it has |
|---|---|
| `src/filer` | The filer API's types and client |
| `src/volumes` | The Volumes module, and the volumes' controller |
| `src/tree` | The tree of folders, and its controller |
| `src/folder` | The file view, and its controller |
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
