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
}

// Backend is a type of backend, of the scheme of its volumes' URLs
export interface Backend {
    scheme: string;
    description?: string;
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

// Meta is an object's metadata, of a namespace and key, whose value is JSON
export interface Meta {
    namespace?: string;
    key?: string;
    value?: unknown;
}

// FilerObject is a file or a folder in a volume, of its path, which is
// "/" for the root, and a folder's has no "/" after it
export interface FilerObject {
    name: string;
    path: string;
    type?: string;
    meta?: Meta[];
    size?: number;
    etag?: string;
    mtime?: string;
    dir?: boolean;
}

// ObjectListQuery is the query of a listing: the folder, which is the root
// by default, whether it's of everything beneath it, the key it starts
// after, how many objects it has, at most, and the type of its objects
export interface ObjectListQuery {
    path?: string;
    recursive?: boolean;
    start_after?: string;
    limit?: number;
    type?: string;
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
    let scheme = '';
    try {
        scheme = new URL(volume.url).protocol.replace(/:$/, '');
    } catch {
        return '';
    }
    return backends.find((backend) => backend.scheme === scheme)?.description || scheme;
}

// volumeTitle returns a volume's title, which is shown when it's pointed at:
// its whole label, and its name and backend
export function volumeTitle(volume: Volume, backends: readonly Backend[] = []): string {
    const backend = volumeBackend(volume, backends);
    return `${volumeLabel(volume)}\n${volume.name}${backend ? ` · ${backend}` : ''}`;
}
