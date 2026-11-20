import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Client } from '@mutablelogic/go-server/api';
import { TreeController } from '../src/tree/controller.ts';
import { VolumesController } from '../src/volumes/controller.ts';
import { listObjects } from '../src/filer/httpclient.ts';
import { baseName, objectTitle, parentPath, subfolderCount, type FilerObject } from '../src/filer/schema.ts';

// fakeServer is a filer of volumes, and their folders, by their paths,
// whose listings are paged, which records each listing's folder
function fakeServer(volumes: Record<string, Record<string, string[]>>, mounted = Object.keys(volumes)) {
    const listed: string[] = [];
    const client: Client = {
        base: 'http://localhost/api',
        fetch: (async (input: RequestInfo | URL) => {
            const url = new URL(String(input));
            const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });
            if (url.pathname === '/api/volume') {
                return json({ body: Object.keys(volumes).map((name) => ({ name, url: `file://${name}`, enabled: true, mounted: mounted.includes(name) })), backends: [] });
            }
            const name = decodeURIComponent(url.pathname.replace('/api/object/', ''));
            const path = (url.searchParams.get('path') ?? '/').replace(/(.)\/$/, '$1');
            listed.push(`${name}:${path}`);
            assert.equal(url.searchParams.get('type'), 'inode/directory');
            const children = volumes[name]?.[path];
            if (!children) {
                return json({ code: 404, reason: 'not found' }, 404);
            }
            const limit = Number(url.searchParams.get('limit') ?? 100);
            const after = url.searchParams.get('start_after') ?? '';
            const page = children.filter((p) => p > after).slice(0, limit);
            const more = children.filter((p) => p > after).length > page.length;
            return json({
                name, count: children.length,
                next: more ? page[page.length - 1] : undefined,
                body: page.map((p) => ({ name, path: p, dir: true, type: 'inode/directory', meta: [{ namespace: 'folder', key: 'subfolder-count', value: volumes[name][p]?.length ?? 0 }] })),
            });
        }) as typeof fetch,
    };
    return { client, listed, volumes };
}

test('schema', async (t) => {
    await t.test('paths', () => {
        assert.equal(baseName('/a/b'), 'b');
        assert.equal(baseName('/'), '');
        assert.equal(parentPath('/a/b'), '/a');
        assert.equal(parentPath('/a'), '/');
        assert.equal(parentPath('/'), '/');
    });

    await t.test("a folder's title, and subfolders", () => {
        const folder: FilerObject = { name: 'tasks', path: '/MTA3', meta: [{ namespace: 'dc', key: 'title', value: 'Shopping' }, { namespace: 'folder', key: 'subfolder-count', value: 0 }] };
        assert.equal(objectTitle(folder), 'Shopping');
        assert.equal(objectTitle({ name: 'media', path: '/a/b' }), 'b');
        assert.equal(subfolderCount(folder), 0);
        assert.equal(subfolderCount({ name: 'media', path: '/a' }), undefined);
    });

    await t.test('a folder is listed with a "/" after it', async () => {
        const { client, listed } = fakeServer({ media: { '/a': [] } });
        await listObjects(client, 'media', { path: '/a', type: 'inode/directory' });
        assert.deepEqual(listed, ['media:/a']);
    });
});

test('TreeController', async (t) => {
    await t.test('folders, a page at a time', async () => {
        const { client } = fakeServer({ media: { '/': ['/a', '/b', '/c'], '/a': [] } });
        const tree = new TreeController(client, { pageLimit: 2 });
        await tree.expand('media', '/');
        await tree.loadMore('media', '/');
        const folders = tree.folders('media', '/');
        assert.equal(folders?.state, 'loaded');
        assert.deepEqual(folders?.state === 'loaded' && folders.folders.map((f) => f.path), ['/a', '/b', '/c']);
        assert.equal(folders?.state === 'loaded' && folders.more, false);
        assert.ok(tree.isExpanded('media', '/'));
        assert.equal(tree.folder('media', '/b')?.path, '/b');
    });

    await t.test('a page at first, and more as they are scrolled to', async () => {
        const { client, listed, volumes } = fakeServer({ media: { '/': ['/a', '/b', '/c', '/d', '/e'] } });
        const tree = new TreeController(client, { pageLimit: 2 });
        await tree.expand('media', '/');
        let folders = tree.folders('media', '/');
        assert.deepEqual(folders?.state === 'loaded' && [folders.folders.map((f) => f.path), folders.more], [['/a', '/b'], true]);
        await tree.loadMore('media', '/');
        await tree.loadMore('media', '/');
        folders = tree.folders('media', '/');
        assert.deepEqual(folders?.state === 'loaded' && [folders.folders.map((f) => f.path), folders.more], [['/a', '/b', '/c', '/d', '/e'], false]);
        await tree.loadMore('media', '/');
        assert.equal(listed.length, 3, 'there are no more');

        // A change loads as many as are loaded again
        listed.length = 0;
        volumes.media['/'] = ['/0', ...volumes.media['/']];
        tree.change({ action: 'create', schema: 'filer', table: 'media', row: '/0', data: { folder: true } });
        await settle();
        folders = tree.folders('media', '/');
        assert.deepEqual(folders?.state === 'loaded' && [folders.folders.map((f) => f.path), folders.more], [['/0', '/a', '/b', '/c', '/d', '/e'], false]);
        assert.equal(listed.length, 3);
    });

    await t.test("a folder which can't be loaded is an error", async () => {
        const { client } = fakeServer({ media: {} });
        const errors: string[] = [];
        const tree = new TreeController(client).onError((title) => errors.push(title));
        await tree.expand('media', '/x');
        assert.equal(tree.folders('media', '/x')?.state, 'error');
        assert.equal(errors.length, 1);
    });

    await t.test('a folder which is expanded again is loaded once', async () => {
        const { client, listed } = fakeServer({ media: { '/': ['/a'] } });
        const tree = new TreeController(client);
        await tree.expand('media', '/');
        tree.collapse('media', '/');
        assert.ok(!tree.isExpanded('media', '/'));
        await tree.expand('media', '/');
        assert.deepEqual(listed, ['media:/']);
    });

    await t.test("a folder's change loads its folder, and its folder's, again", async () => {
        const { client, listed, volumes } = fakeServer({ media: { '/': ['/a', '/b'], '/a': ['/a/x'], '/a/x': [], '/b': [] } });
        const tree = new TreeController(client);
        await tree.expand('media', '/');
        await tree.expand('media', '/a');
        await tree.expand('media', '/b');
        listed.length = 0;

        // A folder which is created in /a/x changes /a/x's, and /a's count of
        // it, but not /b's, nor of another volume, nor of a file
        volumes.media['/a/x'] = ['/a/x/new'];
        tree.change({ action: 'create', schema: 'filer', table: 'media', row: '/a/x/new', data: { folder: true } });
        tree.change({ action: 'create', schema: 'filer', table: 'other', row: '/a/x/new', data: { folder: true } });
        tree.change({ action: 'create', schema: 'filer', table: 'media', row: '/a/file.txt' });
        await settle();
        assert.deepEqual(listed.sort(), ['media:/a'], "/a/x isn't loaded, so it's loaded when it's expanded");

        // A folder which is updated, such as once it's indexed, loads its
        // own subfolders again, too
        listed.length = 0;
        tree.change({ action: 'update', schema: 'filer', table: 'media', row: '/a/x', data: { folder: true } });
        await settle();
        assert.deepEqual(listed.sort(), ['media:/', 'media:/a'], "/a/x isn't loaded, so it isn't loaded again");

        // A folder which is deleted is forgotten, with what's beneath it
        await tree.expand('media', '/a/x');
        listed.length = 0;
        volumes.media['/a'] = [];
        tree.change({ action: 'delete', schema: 'filer', table: 'media', row: '/a/x', data: { folder: true } });
        await settle();
        assert.deepEqual(listed.sort(), ['media:/', 'media:/a']);
        assert.equal(tree.folders('media', '/a/x'), undefined);
        assert.ok(!tree.isExpanded('media', '/a/x'));
    });

    await t.test('a rescan loads what it is of again, and those which are collapsed are forgotten', async () => {
        const { client, listed } = fakeServer({ media: { '/': ['/a', '/b'], '/a': [], '/b': [] } });
        const tree = new TreeController(client);
        await tree.expand('media', '/');
        await tree.expand('media', '/a');
        await tree.expand('media', '/b');
        tree.collapse('media', '/b');
        listed.length = 0;

        // Of a folder, and its folder
        tree.change({ action: 'update', schema: 'filer', table: 'media', row: '/a/' });
        await settle();
        assert.deepEqual(listed.sort(), ['media:/', 'media:/a']);

        // Of the volume
        listed.length = 0;
        tree.change({ action: 'update', schema: 'filer', table: 'media' });
        await settle();
        assert.deepEqual(listed.sort(), ['media:/', 'media:/a']);
        assert.equal(tree.folders('media', '/b'), undefined);
    });

    await t.test("a volume's folders are forgotten", async () => {
        const { client } = fakeServer({ media: { '/': ['/a'] }, other: { '/': [] } });
        const tree = new TreeController(client);
        await tree.expand('media', '/');
        await tree.expand('other', '/');
        tree.forgetVolume('media');
        assert.equal(tree.folders('media', '/'), undefined);
        assert.ok(!tree.isExpanded('media', '/'));
        assert.equal(tree.folders('other', '/')?.state, 'loaded');
    });

    await t.test("an older load doesn't replace a newer one", async () => {
        const { client, volumes } = fakeServer({ media: { '/': ['/old'] } });
        const tree = new TreeController(client);
        const first = tree.expand('media', '/');
        volumes.media['/'] = ['/new'];
        const second = tree.load('media', '/');
        await Promise.all([first, second]);
        const folders = tree.folders('media', '/');
        assert.deepEqual(folders?.state === 'loaded' && folders.folders.map((f) => f.path), ['/new']);
    });
});

// settle waits for the loads which a change starts
async function settle() {
    for (let i = 0; i < 10; i++) {
        await new Promise((resolve) => setTimeout(resolve, 0));
    }
}

test('TreeController.reveal', async () => {
    const { client, listed } = fakeServer({ media: { '/': ['/a'], '/a': ['/a/b'], '/a/b': [] } });
    const tree = new TreeController(client);
    await tree.reveal('media', '/a/b');
    assert.deepEqual(listed, ['media:/', 'media:/a']);
    assert.ok(tree.isExpanded('media', '/') && tree.isExpanded('media', '/a'));
    assert.ok(!tree.isExpanded('media', '/a/b'), "it's shown, not expanded");

    // The root is, of the root, such as once its volume is mounted again
    tree.forgetVolume('media');
    await tree.reveal('media', '/');
    assert.ok(tree.isExpanded('media', '/'));
    assert.equal(tree.folders('media', '/')?.state, 'loaded');
});

test('VolumesController', async (t) => {
    await t.test('the volumes, which are loaded again when they change', async () => {
        const volumes: Record<string, Record<string, string[]>> = { media: {}, down: {} };
        const { client } = fakeServer(volumes, ['media']);
        const controller = new VolumesController(client);
        const seen: string[][] = [];
        controller.subscribe((list) => seen.push(list.map((v) => `${v.name}:${v.mounted}`)));
        await controller.reload();
        assert.deepEqual(seen, [['media:true', 'down:false']]);
        assert.equal(controller.volume('down')?.mounted, false);

        // A change of a volume, of the store, or the registry, or without
        // one, loads them again, and other changes don't
        volumes.added = {};
        controller.change({ action: 'update', schema: 'filer', table: 'media', row: '/a' });
        await settle();
        assert.equal(seen.length, 1);
        controller.change({ action: 'create', schema: 'store', table: 'volume', row: 'added' });
        await settle();
        controller.change({ action: 'update', schema: 'registry', table: 'volume', row: 'media' });
        await settle();
        controller.change();
        await settle();
        assert.equal(seen.length, 4);
        assert.deepEqual(seen.at(-1), ['media:true', 'down:false', 'added:false']);

        // A subscriber is called with them at once, once they're loaded
        let now: number | undefined;
        controller.subscribe((list) => { now ??= list.length; });
        assert.equal(now, 3);
    });

    await t.test("an error, when they can't be loaded", async () => {
        const client: Client = { base: 'http://localhost/api', fetch: (async () => new Response('{"code":500}', { status: 500 })) as typeof fetch };
        const errors: string[] = [];
        const controller = new VolumesController(client).onError((title) => errors.push(title));
        await controller.reload();
        assert.equal(controller.volumes, undefined);
        assert.equal(errors.length, 1);
    });
});

test('volume labels', async () => {
    const { volumeBackend, volumeLabel, volumeTitle } = await import('../src/filer/schema.ts');
    const backends = [{ scheme: 'gdrive', description: 'Google Drive' }];
    const drive = { name: 'drive', url: 'gdrive://drive?description=me%40example.com&index=folders', enabled: true, mounted: true };
    assert.equal(volumeLabel(drive), 'me@example.com');
    assert.equal(volumeBackend(drive, backends), 'Google Drive');
    assert.equal(volumeTitle(drive, backends), 'me@example.com\ndrive · Google Drive');

    // Without a description, it's its name, and a backend which isn't known
    // is its scheme
    const media = { name: 'media', url: 'file://media/mnt/media', enabled: true, mounted: true };
    assert.equal(volumeLabel(media), 'media');
    assert.equal(volumeTitle(media, backends), 'media\nmedia · file');
    assert.equal(volumeLabel({ ...media, url: 'not a url' }), 'media');
});

test("a folder which is deleted, which doesn't say it was one, is one the tree has", async () => {
    const { client, listed, volumes } = fakeServer({ media: { '/': ['/a'], '/a': ['/a/x'], '/a/x': [] } });
    const tree = new TreeController(client);
    await tree.expand('media', '/');
    await tree.expand('media', '/a');
    await tree.expand('media', '/a/x');
    listed.length = 0;

    // As the filesystem's watch sends it, without {"folder":true}
    volumes.media['/a'] = [];
    tree.change({ action: 'delete', schema: 'filer', table: 'media', row: '/a/x' });
    await settle();
    assert.deepEqual(listed.sort(), ['media:/', 'media:/a']);
    assert.equal(tree.folders('media', '/a/x'), undefined);

    // A file which is deleted isn't a folder, but, in a bucket, may delete
    // its folder, so its folder's folder is loaded again
    listed.length = 0;
    tree.change({ action: 'delete', schema: 'filer', table: 'media', row: '/a/file.txt' });
    await settle();
    assert.deepEqual(listed, ['media:/']);
    assert.equal(tree.folders('media', '/a')?.state, 'loaded', "it isn't forgotten");
});

test("a bucket's folders, which are created, and deleted, with their objects", async () => {
    const { client, listed, volumes } = fakeServer({ media: { '/': ['/a'], '/a': [] } });
    const tree = new TreeController(client);
    await tree.expand('media', '/');
    listed.length = 0;

    // An object in a folder which the tree doesn't have creates it, and its
    // folder, which the tree has, is loaded again
    volumes.media['/a'] = ['/a/b'];
    tree.change({ action: 'create', schema: 'filer', table: 'media', row: '/a/b/c/object.pdf' });
    await settle();
    assert.deepEqual(listed, ['media:/'], "/a isn't expanded, so the root is loaded again, of its count");

    // An object in a folder which the tree has doesn't
    await tree.expand('media', '/a');
    listed.length = 0;
    tree.change({ action: 'create', schema: 'filer', table: 'media', row: '/a/b/other.pdf' });
    await settle();
    assert.deepEqual(listed, []);

    // An object which is deleted may delete its folder
    tree.change({ action: 'delete', schema: 'filer', table: 'media', row: '/a/b/other.pdf' });
    await settle();
    assert.deepEqual(listed.sort(), ['media:/', 'media:/a']);

    // An object in the root doesn't change its folders
    listed.length = 0;
    tree.change({ action: 'create', schema: 'filer', table: 'media', row: '/object.pdf' });
    tree.change({ action: 'delete', schema: 'filer', table: 'media', row: '/object.pdf' });
    await settle();
    assert.deepEqual(listed, []);
});
