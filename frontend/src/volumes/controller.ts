// The volumes controller keeps the volumes, which it loads, and loads again
// when they, or what the registry knows of them, such as whether they're
// mounted, change, of the changes which are given to it, and tells those
// which subscribe to it, such as the sidebar, and the volumes' pages.

import type { Client } from '@mutablelogic/go-server/api';
import type { Change, ChangeFilter } from '@mutablelogic/go-server/broadcaster';
import { listVolumes } from '../filer/httpclient.ts';
import { RegistryChangeSchema, StoreChangeSchema, VolumeChangeTable, type Backend, type Volume } from '../filer/schema.ts';

// VolumeFilters are the filters of the changes of the volumes
export const VolumeFilters: ChangeFilter[] = [
    { schema: StoreChangeSchema, table: VolumeChangeTable },
    { schema: RegistryChangeSchema, table: VolumeChangeTable },
];

// VolumesListener is called with the volumes, when they change
export type VolumesListener = (volumes: readonly Volume[]) => void;

export class VolumesController {
    readonly client: Client;

    // The volumes, and the backends they can be of, once they're loaded
    private list?: Volume[];
    private backendList?: Backend[];
    private load = 0;

    // Whether changes are loading the volumes, and whether another change
    // came meanwhile, so they're loaded once more, rather than of each
    private changing = false;
    private changedAgain = false;

    private readonly listeners = new Set<VolumesListener>();
    private errorFn?: (title: string, error: unknown) => void;

    constructor(client: Client) {
        this.client = client;
    }

    // volumes are the volumes, in order of their names, once they're loaded
    get volumes(): readonly Volume[] | undefined {
        return this.list;
    }

    // backends are the backends which volumes can be of, once they're loaded
    get backends(): readonly Backend[] | undefined {
        return this.backendList;
    }

    // volume returns a volume, of its name, once they're loaded
    volume(name: string): Volume | undefined {
        return this.list?.find((volume) => volume.name === name);
    }

    // subscribe calls the function with the volumes, now, when they're
    // loaded, and whenever they change, until the function it returns is
    // called
    subscribe(fn: VolumesListener): () => void {
        this.listeners.add(fn);
        if (this.list) {
            fn(this.list);
        }
        return () => this.listeners.delete(fn);
    }

    // onError calls the function when the volumes can't be loaded
    onError(fn: (title: string, error: unknown) => void): this {
        this.errorFn = fn;
        return this;
    }

    // change loads the volumes again, when a change is of them, or when
    // changes may have been missed, without one, and those which come while
    // they're loaded load them once more, after, such as those of each
    // volume whose index's state changed
    change(change?: Change) {
        if (change && !VolumeFilters.some((filter) => filter.schema === change.schema && filter.table === change.table)) {
            return;
        } else if (this.changing) {
            this.changedAgain = true;
            return;
        }
        this.changing = true;
        void (async () => {
            try {
                do {
                    this.changedAgain = false;
                    await this.reload();
                } while (this.changedAgain);
            } finally {
                this.changing = false;
            }
        })();
    }

    // reload loads the volumes, of which only the latest load is kept
    async reload(): Promise<void> {
        const load = ++this.load;
        try {
            const list = await listVolumes(this.client);
            if (load === this.load) {
                this.list = list.body;
                this.backendList = list.backends;
                this.listeners.forEach((fn) => fn(list.body));
            }
        } catch (error) {
            if (load === this.load) {
                this.errorFn?.("The volumes couldn't be loaded", error);
            }
        }
    }
}
