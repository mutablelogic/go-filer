// The tree controller keeps the folders of the volumes which are shown in
// a tree: the volumes, and the subfolders of each folder which is
// expanded, which are loaded when it's first expanded, and loaded again
// when they change, of the changes which are given to it.

import type { Client } from '@mutablelogic/go-server/api';
import type { Change } from '@mutablelogic/go-server/broadcaster';
import { listObjects, listVolumes } from '../filer/httpclient.ts';
import {
    DirectoryType, ObjectChangeSchema, RegistryChangeSchema, StoreChangeSchema, VolumeChangeTable, parentPath,
    type FilerObject, type Volume,
} from '../filer/schema.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Folders are a folder's subfolders: loading, loaded, with whether there are
// more than are loaded, or not, when they couldn't be
export type Folders =
    | { state: 'loading' }
    | { state: 'loaded'; folders: FilerObject[]; more: boolean }
    | { state: 'error'; error: unknown };

// TreeOptions are how many subfolders a page has, and how many of a folder
// are loaded, at most
export interface TreeOptions {
    pageLimit?: number;
    maxFolders?: number;
}

// TreeListener is called when what the tree shows changes
export type TreeListener = () => void;

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const defaultPageLimit = 100;
const defaultMaxFolders = 1000;

///////////////////////////////////////////////////////////////////////////////
// CONTROLLER

export class TreeController {
    readonly client: Client;
    private readonly pageLimit: number;
    private readonly maxFolders: number;

    // volumes are the volumes, once they're loaded
    private volumeList?: Volume[];
    private volumesLoad = 0;

    // The subfolders which are loaded, which folders are expanded, and the
    // latest load of each folder's, by their keys, so an older response
    // isn't kept
    private readonly loaded = new Map<string, Folders>();
    private readonly expanded = new Set<string>();
    private readonly loads = new Map<string, number>();
    private generation = 0;

    private readonly listeners = new Set<TreeListener>();
    private errorFn?: (title: string, error: unknown) => void;

    constructor(client: Client, options: TreeOptions = {}) {
        this.client = client;
        this.pageLimit = options.pageLimit ?? defaultPageLimit;
        this.maxFolders = options.maxFolders ?? defaultMaxFolders;
    }

    ///////////////////////////////////////////////////////////////////////////
    // PROPERTIES

    // volumes are the volumes, in order of their names, once they're loaded
    get volumes(): readonly Volume[] | undefined {
        return this.volumeList;
    }

    // folders returns a folder's subfolders, when they're loading, or loaded
    folders(volume: string, path: string): Folders | undefined {
        return this.loaded.get(treeKey(volume, path));
    }

    // folder returns a folder, of its folder's subfolders, when they're
    // loaded
    folder(volume: string, path: string): FilerObject | undefined {
        const folders = this.loaded.get(treeKey(volume, parentPath(path)));
        return folders?.state === 'loaded' ? folders.folders.find((folder) => folder.path === path) : undefined;
    }

    // isExpanded returns whether a folder is expanded
    isExpanded(volume: string, path: string): boolean {
        return this.expanded.has(treeKey(volume, path));
    }

    ///////////////////////////////////////////////////////////////////////////
    // PUBLIC METHODS

    // subscribe calls the function when what the tree shows changes, until
    // the function it returns is called
    subscribe(fn: TreeListener): () => void {
        this.listeners.add(fn);
        return () => this.listeners.delete(fn);
    }

    // onError calls the function with what couldn't be loaded
    onError(fn: (title: string, error: unknown) => void): this {
        this.errorFn = fn;
        return this;
    }

    // expand expands a folder, whose subfolders are loaded, unless they are
    expand(volume: string, path: string): Promise<void> {
        const key = treeKey(volume, path);
        this.expanded.add(key);
        if (this.loaded.get(key)?.state === 'loaded' || this.loads.has(key)) {
            this.notify();
            return Promise.resolve();
        }
        return this.load(volume, path);
    }

    // reveal expands the folders a folder is in, from its volume's root, so
    // it's shown, such as when it's chosen by its location
    async reveal(volume: string, path: string): Promise<void> {
        const elements = path.split('/').filter((e) => e);
        let dir = '/';
        for (const element of elements) {
            await this.expand(volume, dir);
            dir = dir === '/' ? '/' + element : dir + '/' + element;
        }
    }

    // collapse collapses a folder, whose subfolders are kept
    collapse(volume: string, path: string) {
        this.expanded.delete(treeKey(volume, path));
        this.notify();
    }

    // loadVolumes loads the volumes, and forgets the folders of those which
    // aren't any more, or aren't mounted
    async loadVolumes(): Promise<void> {
        const load = ++this.volumesLoad;
        try {
            const list = await listVolumes(this.client);
            if (load !== this.volumesLoad) {
                return;
            }
            this.volumeList = list.body;
            const mounted = new Set(list.body.filter((volume) => volume.mounted).map((volume) => volume.name));
            this.forget((volume) => !mounted.has(volume));
            this.notify();
        } catch (error) {
            if (load === this.volumesLoad) {
                this.errorFn?.("The volumes couldn't be loaded", error);
            }
        }
    }

    // load loads a folder's subfolders, a page at a time, up to the most
    // which are loaded, keeping those which are loaded, while they're loaded
    // again
    load(volume: string, path: string): Promise<void> {
        const key = treeKey(volume, path);
        if (this.loaded.get(key)?.state !== 'loaded') {
            this.loaded.set(key, { state: 'loading' });
        }
        const generation = ++this.generation;
        this.loads.set(key, generation);
        this.notify();
        const latest = () => this.loads.get(key) === generation;
        return (async () => {
            try {
                const folders: FilerObject[] = [];
                let more = false;
                let startAfter: string | undefined;
                for (;;) {
                    const list = await listObjects(this.client, volume, { path, type: DirectoryType, start_after: startAfter, limit: this.pageLimit });
                    folders.push(...list.body);
                    if (!list.next || !latest()) {
                        break;
                    } else if (folders.length >= this.maxFolders) {
                        more = true;
                        break;
                    }
                    startAfter = list.next;
                }
                if (latest()) {
                    this.loaded.set(key, { state: 'loaded', folders, more });
                }
            } catch (error) {
                if (latest()) {
                    this.loaded.set(key, { state: 'error', error });
                    this.errorFn?.(`The folders of ${volume}${path === '/' ? '' : path} couldn't be loaded`, error);
                }
            }
            if (latest()) {
                this.loads.delete(key);
                this.notify();
            }
        })();
    }

    // change loads what changed again: the volumes, when they, or what the
    // registry knows of them, change, and the subfolders of the folders
    // which a change of a folder is in, or a rescan is of, or beneath. Those
    // of a folder which isn't expanded are forgotten, so they're loaded when
    // it's expanded. Without a change, as changes may have been missed, such
    // as when they're first watched, everything is loaded again.
    change(change?: Change) {
        if (!change) {
            void this.loadVolumes();
            this.reload(() => true);
            return;
        }
        if ((change.schema === StoreChangeSchema || change.schema === RegistryChangeSchema) && change.table === VolumeChangeTable) {
            void this.loadVolumes();
            return;
        }
        if (change.schema !== ObjectChangeSchema || !change.table) {
            return;
        }
        const volume = change.table;
        const row = change.row ?? '';
        if (row === '') {
            // A rescan of the volume
            this.reload((v) => v === volume);
        } else if (row.endsWith('/')) {
            // A rescan of a folder, of it, what's beneath it, and its folder,
            // whose count of it may have changed
            const dir = row.replace(/\/+$/, '') || '/';
            const parent = parentPath(dir);
            this.reload((v, p) => v === volume && (p === parent || within(p, dir)));
        } else if (isFolderChange(change)) {
            // A folder which is created, updated or deleted, which changes
            // its folder, and the count of its folder's folder
            const parent = parentPath(row);
            const grandparent = parentPath(parent);
            if (change.action === 'delete') {
                this.forget((v, p) => v === volume && within(p, row));
            }
            this.reload((v, p) => v === volume && (p === parent || p === grandparent));
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // PRIVATE METHODS

    // reload loads the subfolders of the folders it's true of again, which
    // are expanded, and forgets those which aren't
    private reload(of: (volume: string, path: string) => boolean) {
        for (const key of [...this.loaded.keys()]) {
            const [volume, path] = splitKey(key);
            if (!of(volume, path)) {
                continue;
            } else if (this.expanded.has(key)) {
                void this.load(volume, path);
            } else {
                this.loaded.delete(key);
                this.loads.delete(key);
            }
        }
        this.notify();
    }

    // forget forgets the subfolders, and whether they're expanded, of the
    // folders it's true of
    private forget(of: (volume: string, path: string) => boolean) {
        for (const set of [this.loaded, this.loads, this.expanded]) {
            for (const key of [...set.keys()]) {
                const [volume, path] = splitKey(key);
                if (of(volume, path)) {
                    set.delete(key);
                }
            }
        }
    }

    private notify() {
        for (const fn of this.listeners) {
            fn();
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// treeKey returns the key of a folder, of its volume and path
function treeKey(volume: string, path: string): string {
    return volume + '\u0000' + (path || '/');
}

// splitKey returns the volume and path of a folder's key
function splitKey(key: string): [string, string] {
    const i = key.indexOf('\u0000');
    return [key.slice(0, i), key.slice(i + 1)];
}

// within returns whether a path is the folder's, or beneath it
function within(path: string, dir: string): boolean {
    return dir === '/' || path === dir || path.startsWith(dir + '/');
}

// isFolderChange returns whether a change of an object is of a folder, of
// its data, {"folder":true}
function isFolderChange(change: Change): boolean {
    const data = change.data;
    return typeof data === 'object' && data !== null && (data as { folder?: unknown }).folder === true;
}
