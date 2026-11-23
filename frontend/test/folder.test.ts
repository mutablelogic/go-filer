import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Client } from '@mutablelogic/go-server/api';
import { FolderController } from '../src/folder/controller.ts';
import { availableColumns, formatSize, formatTime, visibleColumns } from '../src/folder/format.ts';
import { objectFormat, objectKind, volumeFileNames, volumeSortable } from '../src/filer/schema.ts';

// fakeServer is a volume's folders, of their objects' paths, whose listings
// are paged, and counted, which records each listing's folder and start
function fakeServer(folders: Record<string, string[]>) {
    const listed: string[] = [];
    const client: Client = {
        base: 'http://localhost/api',
        fetch: (async (input: RequestInfo | URL) => {
            const url = new URL(String(input));
            const path = (url.searchParams.get('path') ?? '/').replace(/(.)\/$/, '$1');
            const after = url.searchParams.get('start_after') ?? '';
            listed.push(`${path}@${after}`);
            const objects = folders[path];
            if (!objects) {
                return new Response(JSON.stringify({ code: 404 }), { status: 404 });
            }
            const limit = Number(url.searchParams.get('limit') ?? 100);
            const rest = objects.filter((p) => p > after);
            const page = rest.slice(0, limit);
            return new Response(JSON.stringify({
                name: 'media', count: objects.length,
                next: rest.length > page.length ? page[page.length - 1] : undefined,
                body: page.map((p) => ({ name: 'media', path: p, dir: p.endsWith('/') || undefined })),
            }), { headers: { 'Content-Type': 'application/json' } });
        }) as typeof fetch,
    };
    return { client, listed, folders };
}

test('FolderController', async (t) => {
    await t.test('a page at a time, and more', async () => {
        const { client, listed } = fakeServer({ '/a': ['/a/1', '/a/2', '/a/3'] });
        const folder = new FolderController(client, { pageLimit: 2 });
        await folder.open('media', '/a');
        assert.deepEqual(folder.current.objects?.map((o) => o.path), ['/a/1', '/a/2']);
        assert.equal(folder.current.more, true);
        assert.equal(folder.current.count, null, "it isn't known until they're all loaded");
        await folder.loadMore();
        assert.deepEqual(folder.current.objects?.map((o) => o.path), ['/a/1', '/a/2', '/a/3']);
        assert.equal(folder.current.more, false);
        assert.equal(folder.current.count, 3);
        await folder.loadMore();
        assert.deepEqual(listed, ['/a@', '/a@/a/2']);

        // The folder which is shown isn't loaded again
        await folder.open('media', '/a');
        assert.equal(listed.length, 2);
    });

    await t.test('folders are left out, of pages until one has an object', async () => {
        const { client, listed } = fakeServer({ '/': ['/a/', '/b/', '/c/', '/d/', '/x.txt', '/y/', '/z.txt'] });
        const folder = new FolderController(client, { pageLimit: 2 });
        await folder.open('media', '/');
        assert.deepEqual(folder.current.objects?.map((o) => o.path), ['/x.txt', '/z.txt'], 'a page of objects, of as many pages as it takes');
        assert.deepEqual(listed, ['/@', '/@/b/', '/@/d/', '/@/x.txt'], 'the pages of only folders are skipped, and more start after the last object');
        assert.equal(folder.current.more, false);
        assert.equal(folder.current.count, 2, 'its objects, without its folders');
    });

    await t.test('another folder replaces it, and an older load is ignored', async () => {
        const { client } = fakeServer({ '/a': ['/a/1'], '/b': ['/b/1'] });
        const folder = new FolderController(client);
        const a = folder.open('media', '/a');
        const b = folder.open('media', '/b');
        await Promise.all([a, b]);
        assert.equal(folder.current.path, '/b');
        assert.deepEqual(folder.current.objects?.map((o) => o.path), ['/b/1']);
        folder.close();
        assert.equal(folder.current.objects, undefined);
    });

    await t.test('loaded again, as many as are shown, when what is in it changes', async () => {
        const { client, listed, folders } = fakeServer({ '/a': ['/a/1', '/a/2', '/a/3', '/a/4', '/a/5'] });
        const folder = new FolderController(client, { pageLimit: 2 });
        await folder.open('media', '/a');
        await folder.loadMore();
        assert.equal(folder.current.objects?.length, 4);
        listed.length = 0;

        // An object in it, which is created, keeps as many as are shown
        folders['/a'] = ['/a/0', ...folders['/a']];
        folder.change({ action: 'create', schema: 'filer', table: 'media', row: '/a/0' });
        await settle();
        assert.deepEqual(folder.current.objects?.map((o) => o.path), ['/a/0', '/a/1', '/a/2', '/a/3']);
        assert.equal(folder.current.count, null);
        assert.deepEqual(listed, ['/a@', '/a@/a/1']);
    });

    await t.test('which changes are of it', async () => {
        const { client, listed } = fakeServer({ '/a/b': ['/a/b/1'] });
        const folder = new FolderController(client);
        await folder.open('media', '/a/b');
        const changes: [string, string | undefined, boolean][] = [
            ['media', '/a/b/1', true], // an object in it
            ['media', '/a/b', true], // itself, such as once it's indexed
            ['media', '/a/b/c/1', false], // an object beneath it
            ['media', '/a/x', false], // an object beside it
            ['other', '/a/b/1', false], // another volume's
            ['media', undefined, true], // a rescan of the volume
            ['media', '/a/', true], // a rescan of what it's in
            ['media', '/a/b/c/', true], // a rescan of a folder in it
            ['media', '/x/', false], // a rescan of another folder
        ];
        for (const [table, row, reloads] of changes) {
            listed.length = 0;
            folder.change({ action: 'update', schema: 'filer', table, row });
            await settle();
            assert.equal(listed.length > 0, reloads, `${table} ${row}`);
        }
        listed.length = 0;
        folder.change({ action: 'update', schema: 'store', table: 'volume', row: 'media' });
        folder.change();
        await settle();
        assert.equal(listed.length, 1, 'without a change, it is loaded again, and a change of a volume is not of it');
    });

    await t.test("an error, when it can't be loaded, which can be tried again", async () => {
        const { client, folders } = fakeServer({});
        const errors: string[] = [];
        const folder = new FolderController(client).onError((title) => errors.push(title));
        await folder.open('media', '/missing');
        assert.deepEqual([folder.current.objects, folder.current.failed, folder.current.more], [[], true, true]);
        assert.equal(errors.length, 1);
        folders['/missing'] = ['/missing/1'];
        await folder.loadMore();
        assert.deepEqual(folder.current.objects?.map((o) => o.path), ['/missing/1']);
        assert.deepEqual([folder.current.failed, folder.current.more, folder.current.count], [false, false, 1]);
    });

    await t.test("more which can't be loaded are tried again, from where they couldn't be", async () => {
        const { client, folders, listed } = fakeServer({ '/a': ['/a/1', '/a/2', '/a/3'] });
        const folder = new FolderController(client, { pageLimit: 2 });
        await folder.open('media', '/a');
        const saved = folders['/a'];
        delete folders['/a'];
        await folder.loadMore();
        assert.deepEqual([folder.current.objects?.length, folder.current.failed, folder.current.more, folder.current.count], [2, true, true, null], "it's still more, and isn't counted");
        folders['/a'] = saved;
        listed.length = 0;
        await folder.loadMore();
        assert.deepEqual(listed, ['/a@/a/2']);
        assert.deepEqual([folder.current.objects?.length, folder.current.failed, folder.current.count], [3, false, 3]);
    });

    await t.test('more are not loaded while it is loaded again', async () => {
        const { client, listed } = fakeServer({ '/a': ['/a/1', '/a/2', '/a/3'] });
        const folder = new FolderController(client, { pageLimit: 2 });
        await folder.open('media', '/a');
        listed.length = 0;
        const reload = folder.reload();
        assert.equal(folder.current.loading, true);
        await folder.loadMore();
        await reload;
        assert.deepEqual(listed, ['/a@'], 'only the reload');
        assert.deepEqual(folder.current.objects?.map((o) => o.path), ['/a/1', '/a/2']);
    });

    await t.test('a reload without a folder does nothing, and later reloads still do', async () => {
        const { client, listed } = fakeServer({ '/a': ['/a/1'] });
        const folder = new FolderController(client);
        await folder.reload();
        await folder.open('media', '/a');
        listed.length = 0;
        await folder.reload();
        assert.deepEqual(listed, ['/a@']);
    });
});

test('formatSize and formatTime', () => {
    assert.equal(formatSize(undefined), '');
    assert.equal(formatSize(999), '999 B');
    assert.equal(formatSize(1500), '1.5 KB');
    assert.equal(formatSize(25_000_000), '25 MB');
    assert.equal(formatTime(undefined), '');
    assert.equal(formatTime('0001-01-01T00:00:00Z'), '');
    assert.notEqual(formatTime('2026-11-20T12:00:00Z'), '');
});

// settle waits for the loads which a change starts
async function settle() {
    for (let i = 0; i < 10; i++) {
        await new Promise((resolve) => setTimeout(resolve, 0));
    }
}

test('objectKind', () => {
    // Its metadata:kind, or else of its major type, or none
    assert.equal(objectKind({ name: 'media', path: '/a.epub', type: 'application/epub+zip', meta: [{ namespace: 'metadata', key: 'kind', value: 'book' }] }), 'Book');
    assert.equal(objectKind({ name: 'media', path: '/a.mp3', type: 'audio/mpeg' }), 'Audio');
    assert.equal(objectKind({ name: 'media', path: '/a.heic', type: 'image/heic' }), 'Image');
    assert.equal(objectKind({ name: 'media', path: '/a.zip', type: 'application/zip' }), '');
    assert.equal(objectKind({ name: 'media', path: '/a' }), '');
});

test('objectFormat', () => {
    // Its extension, or else its subtype, without x- or vnd., or none
    assert.equal(objectFormat({ name: 'media', path: '/Mac Blue.heic', type: 'image/heic' }), 'HEIC');
    assert.equal(objectFormat({ name: 'media', path: '/a/report.final.pdf' }), 'PDF');
    assert.equal(objectFormat({ name: 'media', path: '/song', type: 'audio/mpeg' }), 'MPEG');
    assert.equal(objectFormat({ name: 'media', path: '/notes', type: 'text/x-markdown; charset=utf-8' }), 'MARKDOWN');
    assert.equal(objectFormat({ name: 'media', path: '/doc', type: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' }), '');
    assert.equal(objectFormat({ name: 'media', path: '/.profile' }), '');
    assert.equal(objectFormat({ name: 'media', path: '/a' }), '');
});

test('volumeFileNames', () => {
    // A folder's, a bucket's and Drive's objects have file names, and
    // contacts', events' and tasks' are ids
    const volume = (url: string) => ({ name: 'v', url, enabled: true, mounted: true });
    assert.equal(volumeFileNames(volume('file://media/mnt/media')), true);
    assert.equal(volumeFileNames(volume('s3://bucket/')), true);
    assert.equal(volumeFileNames(volume('gdrive://drive')), true);
    assert.equal(volumeFileNames(volume('gcontacts://contacts')), false);
    assert.equal(volumeFileNames(volume('webcal://example.com/feed.ics')), false);
});

test('fileViewColumns', () => {
    // File and Format are only of volumes whose objects have file names
    assert.deepEqual(availableColumns(true), ['file', 'kind', 'format', 'size', 'modified']);
    assert.deepEqual(availableColumns(false), ['kind', 'size', 'modified']);

    // Those which are chosen, which can be, in order, or else the defaults
    assert.deepEqual(visibleColumns(true), ['kind', 'size', 'modified']);
    assert.deepEqual(visibleColumns(true, ['modified', 'file']), ['file', 'modified']);
    assert.deepEqual(visibleColumns(false, ['file', 'format', 'size']), ['size']);
    assert.deepEqual(visibleColumns(true, ['unknown']), ['kind', 'size', 'modified'], 'none are the defaults');
    assert.deepEqual(visibleColumns(true, []), ['kind', 'size', 'modified']);
});

test('FolderController sort', async () => {
    // A folder is listed in the order it's sorted, by the server, after the
    // last object, and another volume's isn't sorted
    const queries: string[] = [];
    const client: Client = {
        base: 'http://localhost/api',
        fetch: (async (input: RequestInfo | URL) => {
            const url = new URL(String(input));
            queries.push(`${url.pathname} ${url.searchParams.get('sort') ?? ''} ${url.searchParams.get('start_after') ?? ''}`);
            return new Response(JSON.stringify({ name: 'v', count: null, body: [{ name: 'v', path: '/a/1' }, { name: 'v', path: '/a/2' }], next: '/a/2' }), { headers: { 'Content-Type': 'application/json' } });
        }) as typeof fetch,
    };
    const folder = new FolderController(client, { pageLimit: 2 });
    await folder.open('media', '/a');
    folder.list.setSort({ key: 'size', direction: 'descending' });
    await settle();
    await folder.loadMore();
    assert.deepEqual(queries, ['/api/object/media  ', '/api/object/media -size ', '/api/object/media -size /a/2']);
    queries.length = 0;
    await folder.open('photos', '/');
    assert.equal(folder.list.current.sort, undefined);
    assert.deepEqual(queries, ['/api/object/photos  ']);
});

test('FolderController remembers the sort of each volume', async () => {
    // The order which is chosen of a volume is remembered, in the browser,
    // and is its order when it's opened again, when it can be sorted
    const storage = new Map<string, string>();
    const saved = Object.getOwnPropertyDescriptor(globalThis, 'localStorage');
    Object.defineProperty(globalThis, 'localStorage', {
        configurable: true,
        value: {
            getItem: (key: string) => storage.get(key) ?? null,
            setItem: (key: string, value: string) => { storage.set(key, value); },
            removeItem: (key: string) => { storage.delete(key); },
        },
    });
    try {
        const sorts: string[] = [];
        const client: Client = {
            base: 'http://localhost/api',
            fetch: (async (input: RequestInfo | URL) => {
                const url = new URL(String(input));
                sorts.push(`${url.pathname.split('/').pop()} ${url.searchParams.get('sort') ?? ''}`);
                return new Response(JSON.stringify({ name: 'v', count: 1, body: [{ name: 'v', path: '/1' }] }), { headers: { 'Content-Type': 'application/json' } });
            }) as typeof fetch,
        };
        const folder = new FolderController(client);
        await folder.open('media', '/', true);
        folder.list.setSort({ key: 'modified', direction: 'descending' });
        await settle();
        assert.equal(storage.get('gf-file-view-sort:media'), JSON.stringify({ key: 'modified', direction: 'descending' }));
        await folder.open('photos', '/', true);
        await folder.open('media', '/', true);
        assert.deepEqual(folder.list.current.sort, { key: 'modified', direction: 'descending' });
        await folder.open('photos', '/', true);
        await folder.open('media', '/', false);
        assert.equal(folder.list.current.sort, undefined, "it isn't sorted when it can't be");
        assert.ok(storage.has('gf-file-view-sort:media'), 'its order is kept until it can be');
        folder.list.setSort(undefined);
        await settle();
        folder.list.setSort({ key: 'size', direction: 'ascending' });
        await settle();
        folder.list.setSort(undefined);
        await settle();
        assert.equal(storage.has('gf-file-view-sort:media'), false, 'an order which is chosen of none is forgotten');
        sorts.splice(6);
        assert.deepEqual(sorts, ['media ', 'media -modified', 'photos ', 'media -modified', 'photos ', 'media ']);
    } finally {
        if (saved) {
            Object.defineProperty(globalThis, 'localStorage', saved);
        } else {
            delete (globalThis as { localStorage?: unknown }).localStorage;
        }
    }
});

test('volumeSortable', () => {
    // A volume whose files are indexed, and ready
    const volume = (mode: string, status?: string) => ({ name: 'v', url: 'file://v/tmp', enabled: true, mounted: true, index: { mode, status } });
    assert.equal(volumeSortable(volume('objects', 'ready')), true);
    assert.equal(volumeSortable(volume('artwork', 'ready')), true);
    assert.equal(volumeSortable(volume('objects', 'scanning')), false);
    assert.equal(volumeSortable(volume('folders', 'ready')), false);
    assert.equal(volumeSortable({ name: 'v', url: 'file://v/tmp', enabled: true, mounted: true }), false);
});
