import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Client } from '@mutablelogic/go-server/api';
import { FolderController } from '../src/folder/controller.ts';
import { formatSize, formatTime } from '../src/folder/view.ts';

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
        assert.deepEqual(folder.current.objects?.map((o) => o.path), ['/x.txt']);
        assert.deepEqual(listed, ['/@', '/@/b/', '/@/d/'], 'the pages of only folders are skipped');
        await folder.loadMore();
        assert.deepEqual(folder.current.objects?.map((o) => o.path), ['/x.txt', '/z.txt']);
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

    await t.test("an error, when it can't be loaded", async () => {
        const { client } = fakeServer({});
        const errors: string[] = [];
        const folder = new FolderController(client).onError((title) => errors.push(title));
        await folder.open('media', '/missing');
        assert.deepEqual(folder.current.objects, []);
        assert.equal(errors.length, 1);
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
