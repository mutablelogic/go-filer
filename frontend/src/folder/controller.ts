// The folder controller keeps the objects of a volume's folder, which aren't
// folders, as the tree has those, as go-server's list controller, which
// scrolls, so gs-table shows them, and loads more as they're scrolled to,
// following the listing's cursor, after the last object which is loaded. It
// loads them again when they change, of the changes which are given to it,
// keeping what's shown until they're loaded. The listing can't leave out
// folders, so they're left out of its pages, of which more are loaded,
// until one has an object, or there are no more.

import type { Client } from '@mutablelogic/go-server/api';
import type { Change } from '@mutablelogic/go-server/broadcaster';
import { ListController, type ListState, type Page } from '@mutablelogic/go-server/list';
import type { NotificationController } from '@mutablelogic/go-server/notification';
import { listObjects } from '../filer/httpclient.ts';
import { ObjectChangeSchema, parentPath, type FilerObject } from '../filer/schema.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// FolderState is the folder which is shown: its volume and path, its
// objects, which are undefined while they're first loading, how many it
// has, which is null until they're all loaded, as the listing's count has
// its folders too, whether it has more than are loaded, whether they're
// loading, and whether they couldn't be, the last time, when loading more
// can be tried again, and the object which is opened, if any
export interface FolderState {
    volume?: string;
    path?: string;
    objects?: readonly FilerObject[];
    count: number | null;
    more: boolean;
    loading: boolean;
    failed: boolean;
    selected?: FilerObject;
}

// FolderOptions are how many objects a page has
export interface FolderOptions {
    pageLimit?: number;
}

///////////////////////////////////////////////////////////////////////////////
// CONTROLLER

export class FolderController {
    readonly client: Client;

    // list is the list controller of the folder's objects, which gs-table
    // shows
    readonly list: ListController<FilerObject>;

    private volume?: string;
    private path?: string;
    private state: FolderState = { count: null, more: false, loading: false, failed: false };
    private readonly listeners = new Set<(state: FolderState) => void>();
    private errorFn?: (title: string, error: unknown) => void;

    constructor(client: Client, options: FolderOptions = {}) {
        this.client = client;
        const limit = options.pageLimit ?? 100;
        this.list = new ListController<FilerObject>({
            name: 'The objects',
            scroll: true,
            pageSize: limit,
            same: (a, b) => a.name === b.name && a.path === b.path,
            list: (_offset, _limit, _sort, after) => this.page(after, limit),
            notifications: { error: (title: string, error: unknown) => this.errorFn?.(this.title(title), error) } as unknown as NotificationController,
        });
        this.list.subscribe((state) => this.follow(state));
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
        if (volume === this.volume && path === this.path) {
            return Promise.resolve();
        }
        this.volume = volume;
        this.path = path;
        return this.list.reset();
    }

    // close shows no folder
    close() {
        if (this.volume === undefined && this.path === undefined) {
            return;
        }
        this.volume = undefined;
        this.path = undefined;
        void this.list.reset();
    }

    // loadMore loads the next page of objects, when there's one
    loadMore(): Promise<void> {
        return this.list.loadMore();
    }

    // reload loads the objects which are shown again, at least a page,
    // keeping them until they're loaded
    reload(): Promise<void> {
        return this.list.refresh();
    }

    // change loads the objects again, when a change is of the folder's: an
    // object in it, the folder itself, such as once it's indexed, or a rescan
    // of it, of what it's in, or of the volume, or when changes may have been
    // missed, without one
    change(change?: Change) {
        const { volume, path } = this;
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

    // page returns the objects of the folder, which aren't folders, after
    // the object, a page of the listing at a time, until there's one, or no
    // more, and whether there are more. Without a folder, there are none.
    private async page(after: FilerObject | undefined, limit: number): Promise<Page<FilerObject>> {
        const { volume, path } = this;
        if (!volume || !path) {
            return { body: [], more: false };
        }
        const body: FilerObject[] = [];
        let next = after?.path;
        do {
            const list = await listObjects(this.client, volume, { path, start_after: next, limit });
            body.push(...list.body.filter((object) => !object.dir));
            next = list.next;
        } while (next && body.length === 0 && volume === this.volume && path === this.path);
        return { body, more: !!next };
    }

    // follow keeps the folder's state, of the list's
    private follow(list: ListState<FilerObject>) {
        const more = list.more ?? false;
        this.state = {
            volume: this.volume,
            path: this.path,
            objects: this.volume ? list.items : undefined,
            count: list.items && !more ? list.items.length : null,
            more,
            loading: list.loading ?? false,
            failed: list.failed ?? false,
            selected: list.selected,
        };
        this.listeners.forEach((fn) => fn(this.state));
    }

    // title says which folder's objects couldn't be loaded
    private title(title: string): string {
        return this.volume ? `The objects of ${this.volume}${this.path === '/' ? '' : this.path} couldn't be loaded` : title;
    }
}

// within returns whether a path is the folder's, or beneath it
function within(path: string, dir: string): boolean {
    return dir === '/' || path === dir || path.startsWith(dir + '/');
}
