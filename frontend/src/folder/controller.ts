// The folder controller keeps the objects of a volume's folder, which aren't
// folders, as the tree has those, which it loads a page at a time, as
// they're scrolled to, following the listing's cursor, and loads again when
// they change, of the changes which are given to it, keeping what's shown
// until they're loaded. The listing can't leave out folders, so they're
// left out of its pages, of which more are loaded, until one has an object,
// or there are no more.

import type { Client } from '@mutablelogic/go-server/api';
import type { Change } from '@mutablelogic/go-server/broadcaster';
import { listObjects } from '../filer/httpclient.ts';
import { ObjectChangeSchema, parentPath, type FilerObject } from '../filer/schema.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// FolderState is the folder which is shown: its volume and path, its
// objects, which are undefined while they're first loading, how many it
// has, which is null until they're all loaded, as the listing's count has
// its folders too, whether it has more than are loaded, and whether more
// are loading
export interface FolderState {
    volume?: string;
    path?: string;
    objects?: readonly FilerObject[];
    count: number | null;
    more: boolean;
    loading: boolean;
}

// FolderOptions are how many objects a page has
export interface FolderOptions {
    pageLimit?: number;
}

///////////////////////////////////////////////////////////////////////////////
// CONTROLLER

export class FolderController {
    readonly client: Client;
    private readonly pageLimit: number;
    private state: FolderState = { count: null, more: false, loading: false };

    // The key the next page starts after, and the generation of what's
    // listed, which changes when it's another folder, or it's loaded again,
    // so a response to an earlier one isn't kept
    private next?: string;
    private generation = 0;
    private reloading?: Promise<void>;
    private again = false;

    private readonly listeners = new Set<(state: FolderState) => void>();
    private errorFn?: (title: string, error: unknown) => void;

    constructor(client: Client, options: FolderOptions = {}) {
        this.client = client;
        this.pageLimit = options.pageLimit ?? 100;
    }

    // current is the folder which is shown
    get current(): FolderState {
        return this.state;
    }

    // subscribe calls the function with the state, now, and whenever it
    // changes, until the function it returns is called
    subscribe(fn: (state: FolderState) => void): () => void {
        this.listeners.add(fn);
        fn(this.state);
        return () => this.listeners.delete(fn);
    }

    // onError calls the function when the objects can't be loaded
    onError(fn: (title: string, error: unknown) => void): this {
        this.errorFn = fn;
        return this;
    }

    // open shows a folder, from its first page, unless it's shown
    open(volume: string, path: string): Promise<void> {
        if (volume === this.state.volume && path === this.state.path) {
            return Promise.resolve();
        }
        this.generation++;
        this.next = undefined;
        this.update({ volume, path, objects: undefined, count: null, more: false, loading: false });
        return this.loadMore();
    }

    // close shows no folder
    close() {
        this.generation++;
        this.next = undefined;
        this.update({ volume: undefined, path: undefined, objects: undefined, count: null, more: false, loading: false });
    }

    // loadMore loads the next page of objects, when there's one, and more
    // aren't loading
    async loadMore(): Promise<void> {
        const { volume, path, objects, loading } = this.state;
        if (!volume || !path || loading || (objects && !this.state.more)) {
            return;
        }
        const generation = this.generation;
        this.update({ loading: true });
        try {
            const { added, next } = await this.list(volume, path, this.next, 1, () => generation === this.generation);
            if (generation === this.generation) {
                this.next = next;
                const all = [...(objects ?? []), ...added];
                this.update({ objects: all, count: next ? null : all.length, more: !!next, loading: false });
            }
        } catch (error) {
            if (generation === this.generation) {
                this.update({ objects: objects ?? [], loading: false, more: false });
                this.errorFn?.(`The objects of ${volume}${path === '/' ? '' : path} couldn't be loaded`, error);
            }
        }
    }

    // reload loads the objects which are shown again, at least a page,
    // keeping them until they're loaded. While it's loading, it's loaded
    // once more after.
    reload(): Promise<void> {
        if (this.reloading) {
            this.again = true;
            return this.reloading;
        }
        this.reloading = (async () => {
            do {
                this.again = false;
                const { volume, path } = this.state;
                if (!volume || !path) {
                    break;
                }
                const generation = ++this.generation;
                const want = Math.max(this.state.objects?.length ?? 0, 1);
                try {
                    const { added, next } = await this.list(volume, path, undefined, want, () => generation === this.generation);
                    if (generation === this.generation) {
                        this.next = next;
                        this.update({ objects: added, count: next ? null : added.length, more: !!next, loading: false });
                    }
                } catch (error) {
                    if (generation === this.generation) {
                        this.update({ loading: false });
                        this.errorFn?.(`The objects of ${volume}${path === '/' ? '' : path} couldn't be loaded`, error);
                    }
                }
            } while (this.again);
            this.reloading = undefined;
        })();
        return this.reloading;
    }

    // list returns the objects of a folder, which aren't folders, after the
    // key, a page at a time, until there are at least as many as are wanted,
    // or no more, or it isn't latest, and the key the next page starts after
    private async list(volume: string, path: string, after: string | undefined, want: number, latest: () => boolean): Promise<{ added: FilerObject[]; next?: string }> {
        const added: FilerObject[] = [];
        let next = after;
        do {
            const list = await listObjects(this.client, volume, { path, start_after: next, limit: this.pageLimit });
            added.push(...list.body.filter((object) => !object.dir));
            next = list.next;
        } while (next && added.length < want && latest());
        return { added, next };
    }

    // change loads the objects again, when a change is of the folder's: an
    // object in it, the folder itself, such as once it's indexed, or a rescan
    // of it, of what it's in, or of the volume, or when changes may have been
    // missed, without one
    change(change?: Change) {
        const { volume, path } = this.state;
        if (!volume || !path) {
            return;
        } else if (!change) {
            void this.reload();
            return;
        } else if (change.schema !== ObjectChangeSchema || change.table !== volume) {
            return;
        }
        const row = change.row ?? '';
        if (row === '') {
            void this.reload();
        } else if (row.endsWith('/')) {
            const dir = row.replace(/\/+$/, '') || '/';
            if (within(path, dir) || parentPath(dir) === path) {
                void this.reload();
            }
        } else if (row === path || parentPath(row) === path) {
            void this.reload();
        }
    }

    private update(state: Partial<FolderState>) {
        this.state = { ...this.state, ...state };
        this.listeners.forEach((fn) => fn(this.state));
    }
}

// within returns whether a path is the folder's, or beneath it
function within(path: string, dir: string): boolean {
    return dir === '/' || path === dir || path.startsWith(dir + '/');
}
