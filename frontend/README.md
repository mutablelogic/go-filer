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
| `@mutablelogic/go-filer/chart/meter` | `gf-meter`, a proportional meter chart of Carbon Charts, of groups of a total, in units, which is changed in place as they change |
| `@mutablelogic/go-filer/tree/view` | `gf-file-tree`, a tree of a volume's folders, which fires `gf-file-tree-selected` with the volume and path of the folder which is chosen |

Including Volumes in another app:

```ts
import '@carbon/charts/styles.min.css';
import { navPage, volumesModule } from '@mutablelogic/go-filer/volumes/view';

volumesModule({ nav, router, client, root: document.querySelector('main')!, notifications, changes });
router.onRoute((route) => nav.setActive(navPage(route))).start();
```

- **The sidebar:** Volumes, with each volume indented beneath it, as go-server's `NavPage.parent`. A volume's label is its description, the `description` of its URL, or its name, which is cut short, when it's long, and its title, shown when it's pointed at, is the whole description, with its name and backend, such as `drive · Google Drive`. Its page has the same label. Volumes are added and removed as they're created and deleted. `navPage` returns the sidebar's page of a route, which is a volume's for any of its folders.
- **Adding a volume:** the Volumes page's header has **Add Volume**, a menu of the server's backends, each of which opens a panel (`gf-volume-add`) of a volume of it: its name, the backend's fields (a directory's path; a bucket, and its region, endpoint, credentials' names and SeaweedFS filer; a feed's URL; a Google account's sign-in, and a calendar's window), its access, unless it's a feed, and its search, Read only and On by default. Its description is the backend's, such as a feed's title, which can be edited once it's added. **Add** makes its URL, as the CLI's `volume-create-…` commands do, and creates it. A Google volume which needs a new sign-in asks for one, in the panel: **Sign in with Google** opens a window at Google, of the server's client (`GOOGLE_CLIENT`), a web application's, which redirects to the server's callback, which stores the sign-in, and tells the panel, which adds the volume. When it's a desktop app's client, it says to add it with the CLI.
- **The Volumes page:** `#volumes` has an alert of each volume whose index failed, with why, above its tiles, while its status is Indexing error, which goes once it's indexed again. It has a tile of each volume, which goes to its page: its description, its name, its backend as a tag, and how many objects it has indexed, in the backend's words, such as events, files, tasks or items, or "–" when that isn't known, such as before it's scanned, with why. A tile says when its volume is offline, or isn't mounted. Its pencil, at its bottom right, opens the volume's panel (`gf-volume-panel`), of four tabs, View, Edit, Options and Remove, on View, whenever it's opened:
  - **View:** its details (description, name, backend, status, access, search, folders, objects and URL, and its error), with a note that a volume can be unmounted to exclude it from search and browsing, and **Unmount** and **Mount**, which leave the panel open, so it's seen to change, each disabled when it would do nothing.
  - **Edit:** its description, its access (Read only or Writable, unless its backend is read-only, such as a feed), and its search, how it's indexed (Off, On (`objects`), Metadata, and Artwork and Content, which are disabled), each with what the one which is chosen does beneath it, with **Save**.
  - **Options:** its other options, of its URL, of a text input each, or a text area of one value a line, with **Save**, which is disabled when it has none. Edit's and Options' changes are kept while the other tabs are shown, and Save, of either, patches all of those which are changed, as they are, where one which is emptied is removed.
  - **Remove:** **Remove** is enabled once the volume's name is typed, and removes it from the filer, with its index and cache. What's its backend's, such as its events in Google Calendar, or its files, isn't changed.
  - An action which fails leaves the panel open, and says why. The panel can't be closed while one saves, or removes, and the volumes are loaded again once it's done.

  Beneath the tiles, tabs of the volumes' stats:
  - Each tab has a section of each volume:
    - Its label, as a heading, which goes to its page.
    - Tags of its status, when it isn't Ready (Scanning, Indexing, Indexing error, Offline, Unmounted, Not mounted or Not indexed, with why when it's pointed at, such as that it isn't searched, when it isn't indexed), and of how many folders and objects it has.
  - **Objects:** chosen first. A meter of the volume's objects of each kind, of `index.kinds`, such as Recording or Event, or of their media type, without one, such as `text/plain`, or Unknown, of the six which have the most, the rest of which are Other.
  - **Indexing:** a meter of its objects which are Indexed, Failed and Unindexed, in that order, whose legend says what each is, when it's pointed at, of its metadata task, when it has one, of its total.
- **A volume's page:** `#volume/<name>/<path>` has **Edit Volume** in its header, which opens the volume's panel, on View, as its tile's pencil does, and a tag of the volume's status beside its title, when it isn't Ready, as the Volumes page's stats do, whose title says why. It fills the window, with a tree of the volume's folders on the left, and the file view of the folder which is chosen on the right, each of which scrolls on its own, rather than the window. A volume whose root has no folders, such as contacts, or a feed, has no tree and its file view is as wide as the page. The folder is in the location, so it can be linked to, and it's revealed in the tree, and scrolled to, also once the volume is mounted, such as after the server starts, loading more of the folders it's in until it's there. A volume which isn't mounted shows why.
- **The tree:** its roots are the folders of the volume's root, which is the volume's page, without a folder. A volume without folders has no tree. A folder is selected when it's clicked, and expanded, or collapsed, unless it has no folders; its caret only toggles it. A folder's subfolders are loaded when it's first expanded, 100 at a time, and more as they're scrolled to. A row's selection is as wide as the tree. A folder whose `folder:subfolder-count` is 0 has no caret. A folder's title is its `dc:title`, such as a task list's, or its name.
- **The file view:** a table of the folder's objects, Carbon's `cds-table`, whose header stays at the top as it's scrolled, without its folders, which the tree has, with their names (their `dc:title`, or file name), and the columns chosen of its toolbar's **Columns** menu, which are remembered per volume, in the browser: File and Format, of volumes whose objects have file names (a folder's, a bucket's or Drive's, not contacts', events' or tasks'), and Kind, Size and Modified, which are shown by default. Kind is `metadata:kind`, or else of the media type, such as Image; Format is the file's extension, or else the media subtype, such as HEIC. Cells are one line, cut short with an ellipsis; columns are of fixed widths, and Name has what's left, at least 12rem, beyond which the table scrolls sideways. A file with artwork shows its best image in place of its icon (`objectArtwork`: a cover, then a thumbnail, then a photo, else its first), loaded as it's scrolled to, in the order the listing has them, 100 at a time. The listing can't leave out folders, so pages of only folders are skipped. How many objects the folder has is beneath, once they're all loaded. More aren't loaded while they're loaded again. When they can't be loaded, it says so, with Try again, which loads them from where they couldn't be. A folder without objects has an icon, and Select a folder from the left, or No objects, without the tree. They're loaded again, as many as are shown, when an object in the folder changes, or the folder does, such as once it's indexed, or what it's in is rescanned.
- **The file panel:** a file which is clicked opens its panel, with Download, and Play of audio or video. Its tabs:
  - **Metadata:** its type, size, and when it changed, and its metadata, by namespace.
  - **Artwork**, **Map** and **Events:** only shown when it has them. Artwork is a grid of its images (`GET /artwork/{hash}`), each with what it is, such as Cover or Page 3, its size in pixels, and its type, which opens on its own when it's clicked. Map and Events only count them, for now.
  - **Preview:** its content, as Markdown, of a file which can be read as it.
- **Changes:** the volumes are loaded again when they, or the registry's state of them, change. A folder which is created, updated or deleted loads its folder's subfolders again, and its folder's folder's, whose count of it changed, and one which is updated, such as once it's indexed, its own. A deletion which doesn't say it's of a folder, as the filesystem's don't, is one when the tree has it. A bucket's folders are the prefixes of its objects' keys, which are created, and deleted, with them, without changes of their own, so an object which is created in a folder the tree doesn't have loads the folders it's in again, and one which is deleted loads its folder's folder again. A rescan loads what it's of again. The subfolders of a folder which is collapsed are forgotten, and loaded again when it's expanded.

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
