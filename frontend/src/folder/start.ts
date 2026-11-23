// Which folder a volume's root shows the objects of, so it always shows
// some, without its location changing: the root's own, when it has any,
// or else the first folder beneath it which has objects, or, of a volume
// whose folders are of years and months, such as a calendar's, or a feed's,
// the current month's, or the nearest month which has something in it.

import type { Client } from '@mutablelogic/go-server/api';
import { listObjects } from '../filer/httpclient.ts';
import { DirectoryType, FolderNamespace, FolderObjectCount, FolderSubfolderCount, metaValue, type FilerObject } from '../filer/schema.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// StartLister lists what a volume's folders have: the subfolders of a
// folder, and whether it has objects which aren't folders
export interface StartLister {
    folders(path: string): Promise<readonly FilerObject[]>;
    hasObjects(path: string): Promise<boolean>;
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// DatedSchemes are the backends whose folders are of years and months, such
// as /2026/2026-11, whose volumes start at the current month
export const DatedSchemes: ReadonlySet<string> = new Set(['gcalendar', 'rss', 'webcal']);

// The folders of a year, and of a month, of their paths, and how deep a
// volume's folders are descended, at most
const reYear = /^\/(\d{4})$/;
const reMonth = /^\/\d{4}\/(\d{4}-\d{2})$/;
const maxDepth = 32;

// The most pages of a folder which are listed, to find its subfolders, or
// whether it has objects, of pageLimit each
const pageLimit = 100;
const maxPages = 5;

///////////////////////////////////////////////////////////////////////////////
// PUBLIC FUNCTIONS

// startFolder returns the path of the folder whose objects a volume's root
// shows, of its backend's scheme: of a dated volume, the current month's,
// or the nearest month's, which has something in it; or else the root,
// when it has objects, or else the first folder beneath it which has, of
// its subfolders' counts, or of their listings, when they have none; or the
// root, when none has
export async function startFolder(scheme: string, lister: StartLister, now: Date = new Date()): Promise<string> {
    if (DatedSchemes.has(scheme)) {
        const month = await monthFolder(lister, now);
        if (month) {
            return month;
        }
    }
    if (await lister.hasObjects('/')) {
        return '/';
    }
    return (await descend(lister, '/')) ?? '/';
}

// apiLister returns the lister of a volume, of the filer's API
export function apiLister(client: Client, volume: string): StartLister {
    return {
        async folders(path: string) {
            const folders: FilerObject[] = [];
            let next: string | undefined;
            for (let page = 0; page < maxPages; page++) {
                const list = await listObjects(client, volume, { path, type: DirectoryType, start_after: next, limit: pageLimit });
                folders.push(...list.body.filter((object) => object.dir));
                if (!(next = list.next)) {
                    break;
                }
            }
            return folders;
        },
        async hasObjects(path: string) {
            let next: string | undefined;
            for (let page = 0; page < maxPages; page++) {
                const list = await listObjects(client, volume, { path, start_after: next, limit: pageLimit });
                if (list.body.some((object) => !object.dir)) {
                    return true;
                } else if (!(next = list.next)) {
                    break;
                }
            }
            return false;
        },
    };
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE FUNCTIONS

// descend returns the first folder beneath the path which has objects: of
// its subfolders, the first with objects, or else of the first which has
// subfolders, its first, and so on, or undefined, when none has
async function descend(lister: StartLister, path: string): Promise<string | undefined> {
    for (let depth = 0; depth < maxDepth; depth++) {
        const folders = await lister.folders(path);
        for (const folder of folders) {
            const objects = count(folder, FolderObjectCount);
            if (objects !== undefined ? objects > 0 : await lister.hasObjects(folder.path)) {
                return folder.path;
            }
        }
        const next = folders.find((folder) => (count(folder, FolderSubfolderCount) ?? 1) > 0);
        if (!next) {
            return undefined;
        }
        path = next.path;
    }
    return undefined;
}

// monthFolder returns the folder of the current month, or else the first
// month after it which has a folder, or else the last before it, of the
// folders of years, and their months, or undefined, when there are none
async function monthFolder(lister: StartLister, now: Date): Promise<string | undefined> {
    const current = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    const year = now.getFullYear();
    const years = (await lister.folders('/')).map((folder) => folder.path).filter((path) => reYear.test(path)).sort();
    const months = async (path: string) => (await lister.folders(path)).map((folder) => folder.path).filter((p) => reMonth.test(p)).sort();
    const month = (path: string) => path.match(reMonth)?.[1] ?? '';

    // The current month, or the first after it
    for (const path of years.filter((p) => Number(p.slice(1)) >= year)) {
        const found = (await months(path)).find((p) => month(p) >= current);
        if (found) {
            return found;
        }
    }

    // Or the last before it
    for (const path of years.filter((p) => Number(p.slice(1)) <= year).reverse()) {
        const found = (await months(path)).reverse().find((p) => month(p) < current);
        if (found) {
            return found;
        }
    }
    return undefined;
}

// count returns a folder's count of its metadata, such as how many objects
// it has, or undefined, when it hasn't one, such as when it isn't indexed
function count(folder: FilerObject, key: string): number | undefined {
    const value = metaValue(folder, FolderNamespace, key);
    return typeof value === 'number' ? value : undefined;
}
