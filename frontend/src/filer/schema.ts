// The filer's schema, which mirrors go-filer's schema, of what the UI shows

// Volume is a volume: its name, the URL of its backend, without secrets,
// whether it's enabled and mounted, and why it isn't, when it isn't
export interface Volume {
    name: string;
    url: string;
    enabled: boolean;
    mounted: boolean;
    created?: string;
    modified?: string;
    error?: string;
    index?: VolumeIndex;
}

// VolumeIndex is a volume's index, when it's indexed: what's indexed, such
// as folders, or objects, or metadata, how far, how many folders and objects
// it has, which are left out when there are none, and how many objects are
// of each kind, such as book, or else of their media type, such as
// text/plain, or of "", of neither, when its objects are indexed
export interface VolumeIndex {
    mode: string;
    status?: string;
    phase?: string;
    folders?: number;
    objects?: number;
    error?: string;
    metadata?: TaskState;
    kinds?: Record<string, number>;
    sortable?: boolean;
}

// TaskState is the work of a task of a volume's objects, such as indexing
// their metadata: how many are queued, or being done, how many it's done,
// and how many it failed of too often, and why it last failed
export interface TaskState {
    queued?: number;
    indexed?: number;
    failed?: number;
    error?: string;
}

// VolumeIndexing is how many of a volume's objects aren't indexed, are,
// and which it failed to index too often, which are given up on
export interface VolumeIndexing {
    unindexed: number;
    indexed: number;
    failed: number;
}

// Backend is a type of backend, of the scheme of its volumes' URLs
export interface Backend {
    scheme: string;
    description?: string;
    // icon is the media type of its icon, such as image/svg+xml, when it has
    // one, of backendIconURL, and defaults the options of a volume which
    // aren't set, such as readonly=true
    icon?: string;
    defaults?: Record<string, string[]>;
}

// VolumeList is the volumes, in order of their names, and the backends
// volumes can be added of
export interface VolumeList {
    body: Volume[];
    backends: Backend[];
}

// The schema and table of the changes of the stored volumes, the schema of
// the changes of what the registry knows of them, such as whether they're
// mounted, or offline, whose table is the same, and the schema of the
// changes of their objects, whose table is the volume's name
export const StoreChangeSchema = 'store';
export const RegistryChangeSchema = 'registry';
export const VolumeChangeTable = 'volume';
export const ObjectChangeSchema = 'filer';

// Meta is an object's metadata, of a namespace and key, whose value is JSON,
// and its text, for people, such as 5s of a duration, when it says more than
// the value
export interface Meta {
    namespace?: string;
    key?: string;
    value?: unknown;
    text?: string;
}

// Artwork is an image an object links to, such as an album's cover, or a
// video's thumbnail, of its context, its type, its size, in pixels, and in
// bytes, whose image is of its hash (see artworkURL)
export interface Artwork {
    hash: string;
    context: string;
    type: string;
    width: number;
    height: number;
    size?: number;
    created_at?: string;
}

// Occurrence is an event an object has, such as a calendar's, of its
// times, which are a date, of an all-day event, or a time, in UTC, and the
// zone of its times
export interface Occurrence {
    uid: string;
    recurrence_id?: string;
    kind: string;
    title?: string;
    start: string;
    end?: string;
    tz?: string;
}

// FilerObject is a file or a folder in a volume, of its path, which is
// "/" for the root, and a folder's has no "/" after it, and what its index
// found of it: where it is, as GeoJSON geometries, its events, and the
// artwork it links to, in the order of their contexts
export interface FilerObject {
    name: string;
    path: string;
    type?: string;
    meta?: Meta[];
    size?: number;
    etag?: string;
    mtime?: string;
    dir?: boolean;
    geo?: unknown[];
    events?: Occurrence[];
    artwork?: Artwork[];
}

// ObjectListQuery is the query of a listing: the folder, which is the root
// by default, whether it's of everything beneath it, the key it starts
// after, how many objects it has, at most, the type of its objects, and
// its order, of a volume whose files are indexed, such as size, or -size,
// which is descending
export interface ObjectListQuery {
    path?: string;
    recursive?: boolean;
    start_after?: string;
    limit?: number;
    type?: string;
    sort?: string;
}

// ObjectList is a page of a listing, and the key the next page starts
// after, when there's one, and how many objects the listing has, of every
// page, or null, when its volume can't count them
export interface ObjectList extends ObjectListQuery {
    name: string;
    next?: string;
    count: number | null;
    body: FilerObject[];
}

// DirectoryType is the type of a folder
export const DirectoryType = 'inode/directory';

// The namespace of a folder's metadata, and its keys: how many objects
// which aren't folders, and how many subfolders, it lists
export const FolderNamespace = 'folder';
export const FolderObjectCount = 'object-count';
export const FolderSubfolderCount = 'subfolder-count';

// metaValue returns the value of an object's metadata, of its namespace and
// key, or undefined, when it has none
export function metaValue(object: FilerObject, namespace: string, key: string): unknown {
    return object.meta?.find((m) => m.namespace === namespace && m.key === key)?.value;
}

// The contexts of artwork which show an object, best first, such as a
// file's icon, before those of other contexts, such as a document's pages
const ArtworkContexts = ['cover', 'thumbnail', 'photo'];

// objectArtwork returns the artwork which shows an object best, of its
// context, such as an album's cover, or a video's thumbnail, or else its
// first, or undefined, when it has none
export function objectArtwork(object: FilerObject): Artwork | undefined {
    const rank = (a: Artwork) => {
        const i = ArtworkContexts.indexOf(a.context);
        return i < 0 ? ArtworkContexts.length : i;
    };
    let best: Artwork | undefined;
    for (const a of object.artwork ?? []) {
        if (!best || rank(a) < rank(best)) {
            best = a;
        }
    }
    return best;
}

// artworkLabel returns what artwork is of an object, of its context, such
// as Cover, Thumbnail 2, or Page 3, of page0003
export function artworkLabel(artwork: Artwork): string {
    const page = /^page0*(\d+)$/.exec(artwork.context);
    if (page) {
        return `Page ${page[1]}`;
    }
    const words = artwork.context.split(/[-_]+/).filter(Boolean).join(' ');
    return words.charAt(0).toUpperCase() + words.slice(1);
}

// MajorKinds are the kinds of objects of the major types of their media
// types, without a metadata:kind, MaxFormat the longest format which is
// shown, and FileNameSchemes the backends whose objects have file names
const MajorKinds: Record<string, string> = { image: 'Image', audio: 'Audio', video: 'Video', text: 'Text', font: 'Font' };
const MaxFormat = 8;
const FileNameSchemes = ['file', 's3', 'gdrive'];

// subfolderCount returns how many subfolders a folder lists, or undefined,
// when it isn't known
export function subfolderCount(object: FilerObject): number | undefined {
    const value = metaValue(object, FolderNamespace, FolderSubfolderCount);
    return typeof value === 'number' ? value : undefined;
}

// objectTitle returns the title of an object: its dc:title, such as a task
// list's, whose path is its id, or else the last element of its path, or
// its volume's name, of the root
export function objectTitle(object: FilerObject): string {
    const title = metaValue(object, 'dc', 'title');
    if (typeof title === 'string' && title) {
        return title;
    }
    return baseName(object.path) || object.name;
}

// baseName returns the last element of a path, or "" of the root
export function baseName(path: string): string {
    const trimmed = path.replace(/\/+$/, '');
    return trimmed.slice(trimmed.lastIndexOf('/') + 1);
}

// parentPath returns the path of the folder of a path, which is "/" of the
// root, and of what's in it
export function parentPath(path: string): string {
    const trimmed = path.replace(/\/+$/, '');
    const i = trimmed.lastIndexOf('/');
    return i <= 0 ? '/' : trimmed.slice(0, i);
}

// volumeDescription returns what a volume is, of its URL's description, such
// as the folder it's of, or the account of a Google volume, or ""
export function volumeDescription(volume: Volume): string {
    try {
        return new URL(volume.url).searchParams.get('description')?.trim() ?? '';
    } catch {
        return '';
    }
}

// volumeLabel returns a volume's label: its description, or its name
export function volumeLabel(volume: Volume): string {
    return volumeDescription(volume) || volume.name;
}

// volumeBackend returns what a volume's backend is, such as Google Drive,
// of the backends, or its scheme
export function volumeBackend(volume: Volume, backends: readonly Backend[] = []): string {
    const scheme = volumeScheme(volume);
    if (!scheme) {
        return '';
    }
    return backends.find((backend) => backend.scheme === scheme)?.description || scheme;
}

// volumeScheme returns the scheme of a volume's backend, such as file, or ""
export function volumeScheme(volume: Volume): string {
    try {
        return new URL(volume.url).protocol.replace(/:$/, '');
    } catch {
        return '';
    }
}

// volumeObjects returns how many objects a volume's index has, without its
// folders, or undefined, when it isn't known (see volumeObjectsUnknown).
// The count is left out when it's zero, once the index is ready, and when
// it isn't counted yet, such as before it's scanned.
export function volumeObjects(volume: Volume): number | undefined {
    const index = volume.index;
    if (!index || index.mode === 'folders') {
        return undefined;
    } else if (index.objects !== undefined) {
        return index.objects;
    }
    return index.status === 'ready' ? 0 : undefined;
}

// volumeObjectsUnknown returns why how many objects a volume has isn't
// known, or "" when it is
export function volumeObjectsUnknown(volume: Volume): string {
    const index = volume.index;
    if (volumeObjects(volume) !== undefined) {
        return '';
    } else if (!index) {
        return "It isn't indexed";
    } else if (index.mode === 'folders') {
        return 'Only its folders are indexed';
    } else if (index.error) {
        return `Its count isn't known: ${index.error}`;
    } else if (index.status === 'scanning') {
        return "It's being indexed";
    }
    return "It hasn't been indexed yet";
}

// volumeIndexing returns how many of a volume's objects aren't indexed,
// are, and failed, of the task which indexes their metadata, whose count
// of those it's done has those which failed, when it has one, or all of
// them indexed, when it hasn't one, or undefined, when how many objects it
// has isn't known
export function volumeIndexing(volume: Volume): VolumeIndexing | undefined {
    const objects = volumeObjects(volume);
    if (objects === undefined) {
        return undefined;
    }
    const task = volume.index?.metadata;
    if (!task) {
        return { unindexed: 0, indexed: objects, failed: 0 };
    }
    const done = Math.min(task.indexed ?? 0, objects);
    const failed = Math.min(task.failed ?? 0, done);
    return { unindexed: objects - done, indexed: done - failed, failed };
}

// volumeStatus returns how a volume is: Offline, when its backend failed,
// whether it's mounted or not, Unmounted, when it's unmounted, Not mounted,
// when it's to be, but isn't yet, Not indexed, Scanning, or Indexing, while
// its files, or their metadata, are, Indexing error, when its last scan, or
// its watch, failed, which clears its phase, but not its status, or Ready,
// or Not indexed yet, when it hasn't been scanned
export function volumeStatus(volume: Volume): string {
    const index = volume.index;
    if (volume.error) {
        return 'Offline';
    } else if (!volume.mounted) {
        return volume.enabled ? 'Not mounted' : 'Unmounted';
    } else if (!index) {
        return 'Not indexed';
    } else if (index.phase === 'scanning') {
        return 'Scanning';
    } else if (index.phase === 'indexing') {
        return 'Indexing';
    } else if (index.error) {
        return 'Indexing error';
    } else if (index.status === 'scanning') {
        return 'Scanning';
    } else if ((index.metadata?.queued ?? 0) > 0) {
        return 'Indexing';
    } else if (index.status === 'ready') {
        return 'Ready';
    }
    return 'Not indexed yet';
}

// volumeIndexFailure returns why a volume's index failed, while its status
// is Indexing error, such as when its last scan, or its watch, failed, and it isn't
// scanned, or indexed, again, or "", otherwise, such as once it's indexed
export function volumeIndexFailure(volume: Volume): string {
    return volumeStatus(volume) === 'Indexing error' ? volume.index?.error ?? '' : '';
}

// VolumeKind is a kind of a volume's objects, as it's shown, and how many
// objects are of it
export interface VolumeKind {
    group: string;
    value: number;
}

// kindLabel returns how a kind is shown: a kind, such as book, capitalised,
// such as Book, a media type, such as text/plain, as it is, or Unknown, of
// "", or of the unknown kind
export function kindLabel(kind: string): string {
    if (kind === '' || kind === 'unknown') {
        return 'Unknown';
    } else if (kind.includes('/')) {
        return kind;
    }
    return kind.charAt(0).toUpperCase() + kind.slice(1);
}

// OtherKind is the kinds of a volume's objects which aren't shown, of their
// own, as there are too many, and of a kind which is other
const OtherKind = 'Other';

// volumeKinds returns how many of a volume's objects are of each kind, as
// they're shown, of those which have the most first, of at most max kinds,
// the rest of which are Other, or undefined, when its kinds aren't known,
// as its objects aren't indexed
export function volumeKinds(volume: Volume, max = 6): VolumeKind[] | undefined {
    // A volume of none has no kinds, which are left out
    const objects = volumeObjects(volume);
    const kinds = volume.index?.kinds ?? (objects === 0 ? {} : undefined);
    if (!kinds || objects === undefined) {
        return undefined;
    }
    const labels = new Map<string, number>();
    for (const [kind, count] of Object.entries(kinds)) {
        if (count > 0) {
            const label = kindLabel(kind);
            labels.set(label, (labels.get(label) ?? 0) + count);
        }
    }
    // A kind which is Other, of its own, is of the rest, so there's one
    const other = labels.get(OtherKind) ?? 0;
    labels.delete(OtherKind);
    const sorted = [...labels].map(([group, value]) => ({ group, value }))
        .sort((a, b) => b.value - a.value || a.group.localeCompare(b.group));
    if (sorted.length + (other > 0 ? 1 : 0) <= max) {
        return other > 0 ? [...sorted, { group: OtherKind, value: other }] : sorted;
    }
    const rest = sorted.slice(max - 1).reduce((sum, kind) => sum + kind.value, other);
    return [...sorted.slice(0, max - 1), { group: OtherKind, value: rest }];
}

// ObjectNouns are what a backend's objects are called, of its scheme, in
// the singular and the plural, such as the events of a calendar, rather
// than objects
const ObjectNouns = new Map<string, [string, string]>([
    ['file', ['file', 'files']],
    ['gcalendar', ['event', 'events']],
    ['gcontacts', ['contact', 'contacts']],
    ['gdrive', ['file', 'files']],
    ['gtasks', ['task', 'tasks']],
    ['rss', ['item', 'items']],
    ['webcal', ['event', 'events']],
]);

// volumeNoun returns what a volume's objects are called, of its backend, of
// a count of them, such as "event", or "events", or else "object", or
// "objects"
export function volumeNoun(volume: Volume, count: number): string {
    const [one, many] = ObjectNouns.get(volumeScheme(volume)) ?? ['object', 'objects'];
    return count === 1 ? one : many;
}

// volumeTitle returns a volume's title, which is shown when it's pointed at:
// its whole label, and its name and backend
export function volumeTitle(volume: Volume, backends: readonly Backend[] = []): string {
    const backend = volumeBackend(volume, backends);
    return `${volumeLabel(volume)}\n${volume.name}${backend ? ` · ${backend}` : ''}`;
}

// The namespace of the metadata about an object itself, of go-metadata, and
// the key of its kind, such as book
export const MetadataNamespace = 'metadata';
export const MetadataKind = 'kind';

// objectKind returns what an object is: its metadata:kind, such as Book,
// or Recording, or else of its media type's major type, such as Image, or
// Audio, or "", when that isn't one which says
export function objectKind(object: FilerObject): string {
    const kind = metaValue(object, MetadataNamespace, MetadataKind);
    if (typeof kind === 'string' && kind) {
        return kind.charAt(0).toUpperCase() + kind.slice(1);
    }
    const major = (object.type ?? '').split('/')[0].trim().toLowerCase();
    return MajorKinds[major] ?? '';
}

// objectFileName returns an object's file name, the last element of its
// path, with its extension
export function objectFileName(object: FilerObject): string {
    return baseName(object.path);
}

// objectFormat returns an object's format, for people: its file name's
// extension, such as HEIC, or PDF, or else its media type's subtype, without
// x- or vnd., such as MPEG, or "", of neither, or one which is too long to
// say much
export function objectFormat(object: FilerObject): string {
    const name = objectFileName(object);
    const dot = name.lastIndexOf('.');
    if (dot > 0 && name.length - dot - 1 <= MaxFormat && /^[a-z0-9]+$/i.test(name.slice(dot + 1))) {
        return name.slice(dot + 1).toUpperCase();
    }
    const type = (object.type ?? '').split(';')[0].trim();
    const subtype = type.slice(type.indexOf('/') + 1).replace(/^(x-|vnd\.)/, '');
    return type.includes('/') && subtype.length <= MaxFormat ? subtype.toUpperCase() : '';
}

// volumeSortable returns whether a volume's listings can be sorted, by the
// server, of its index, which says so once its files are indexed, of its
// last scan which finished
export function volumeSortable(volume: Volume): boolean {
    return volume.index?.sortable === true;
}

// volumeFileNames returns whether a volume's objects have file names, such
// as a folder's, or a bucket's, whose paths are their names, rather than
// ids, such as a contact's, or an event's
export function volumeFileNames(volume: Volume): boolean {
    return FileNameSchemes.includes(volumeScheme(volume));
}
