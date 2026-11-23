// The folder controller keeps the objects of a volume's folder, which aren't
// folders, as the tree has those, as go-server's list controller, which
// scrolls, so gs-table shows them, and loads more as they're scrolled to,
// following the listing's cursor, after the last object which is loaded. It
// loads them again when they change, of the changes which are given to it,
// keeping what's shown until they're loaded. The listing can't leave out
// folders, so they're left out of its pages, of which more are loaded,
// until one has an object, or there are no more. A folder which isn't
// there any more, such as once it's deleted, is said to be gone, rather
// than that its objects couldn't be loaded, so what shows it can show
// another.

import { ApiError, type Client } from '@mutablelogic/go-server/api';
import type { Change } from '@mutablelogic/go-server/broadcaster';
import { ListController, type ListState, type Page, type Sort } from '@mutablelogic/go-server/list';
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
    private sorted?: Sort;

    // opened is the volume which was last opened, which is kept once it's
    // closed, so opening it again, such as its root, isn't another volume's
    private opened?: string;
    private state: FolderState = { count: null, more: false, loading: false, failed: false };
    private readonly listeners = new Set<(state: FolderState) => void>();
    private errorFn?: (title: string, error: unknown) => void;
    private goneFn?: (volume: string, path: string) => void;

    constructor(client: Client, options: FolderOptions = {}) {
        this.client = client;
        const limit = options.pageLimit ?? 100;
        this.list = new ListController<FilerObject>({
            name: 'The objects',
            scroll: true,
            pageSize: limit,
            same: (a, b) => a.name === b.name && a.path === b.path,
            list: (_offset, _limit, sort, after) => this.page(after, limit, sort),
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

    // onGone calls the function with a folder which isn't there, other than
    // a volume's root, such as once it's deleted, whose objects are none
    onGone(fn: (volume: string, path: string) => void): this {
        this.goneFn = fn;
        return this;
    }

    // open shows a folder, from its first page, unless it's shown, in the
    // order it's sorted, or, of another volume, in the order which was last
    // chosen of it, in the browser, when it can be sorted, or else of its
    // keys
    open(volume: string, path: string, sortable = false): Promise<void> {
        if (volume === this.volume && path === this.path) {
            return Promise.resolve();
        }
        const another = volume !== this.opened;
        this.volume = volume;
        this.path = path;
        this.opened = volume;
        if (another) {
            const sort = sortable ? readSort(volume) : undefined;
            if (!sameSort(sort, this.list.current.sort)) {
                this.list.close();
                this.unsorted(sort);
                return this.loaded();
            }
        }
        return this.list.reset();
    }

    // unsorted sorts the folder in the order, or of its keys, without one,
    // without remembering it, such as of a volume which can't be sorted
    // now, whose order is kept until it can be
    unsorted(sort?: Sort) {
        this.sorted = sort;
        this.list.setSort(sort);
    }

    // loaded resolves once the list is loaded, such as once it's sorted,
    // which loads it, without loading it again
    private loaded(): Promise<void> {
        return new Promise((resolve) => {
            let unsubscribe: (() => void) | undefined;
            let done = false;
            unsubscribe = this.list.subscribe((state) => {
                if (state.items && !state.loading && !done) {
                    done = true;
                    queueMicrotask(() => unsubscribe?.());
                    resolve();
                }
            });
            if (done) {
                unsubscribe();
            }
        });
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
    // more, and whether there are more, in the order of the sort, when it's
    // sorted, by the server. Without a folder, there are none.
    private async page(after: FilerObject | undefined, limit: number, sort?: Sort): Promise<Page<FilerObject>> {
        const { volume, path } = this;
        if (!volume || !path) {
            return { body: [], more: false };
        }
        const order = sort ? `${sort.direction === 'descending' ? '-' : ''}${sort.key}` : undefined;
        const body: FilerObject[] = [];
        let next = after?.path;
        do {
            let list;
            try {
                list = await listObjects(this.client, volume, { path, start_after: next, limit, sort: order });
            } catch (error) {
                // A folder which isn't there is gone, once, of the folder
                // which is shown, and has none
                if (error instanceof ApiError && error.status === 404 && path !== '/') {
                    if (volume === this.volume && path === this.path) {
                        queueMicrotask(() => this.goneFn?.(volume, path));
                    }
                    return { body: [], more: false };
                }
                throw error;
            }
            body.push(...list.body.filter((object) => !object.dir));
            next = list.next;
        } while (next && body.length === 0 && volume === this.volume && path === this.path);
        return { body, more: !!next };
    }

    // follow keeps the folder's state, of the list's, and remembers the
    // order it's sorted in, of its volume, when it's chosen
    private follow(list: ListState<FilerObject>) {
        if (this.volume && !sameSort(list.sort, this.sorted)) {
            writeSort(this.volume, list.sort);
        }
        this.sorted = list.sort;
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

// The prefix of the key of the order which was chosen of a volume, in the
// browser's storage, before its name
const SortKey = 'gf-file-view-sort:';

// sameSort returns whether two orders are the same
function sameSort(a?: Sort, b?: Sort): boolean {
    return a?.key === b?.key && a?.direction === b?.direction;
}

// readSort returns the order which was chosen of a volume, or undefined,
// when none was, or the browser's storage can't be read
function readSort(volume: string): Sort | undefined {
    try {
        const value = JSON.parse(localStorage.getItem(SortKey + volume) ?? 'null');
        if (value && typeof value.key === 'string' && (value.direction === 'ascending' || value.direction === 'descending')) {
            return { key: value.key, direction: value.direction };
        }
    } catch {
        // None was
    }
    return undefined;
}

// writeSort remembers the order which is chosen of a volume, or forgets it,
// when it isn't sorted, when the browser's storage can be written
function writeSort(volume: string, sort?: Sort) {
    try {
        if (sort) {
            localStorage.setItem(SortKey + volume, JSON.stringify(sort));
        } else {
            localStorage.removeItem(SortKey + volume);
        }
    } catch {
        // It isn't remembered
    }
}

