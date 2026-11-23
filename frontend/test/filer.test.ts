import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ApiError, type Client } from '@mutablelogic/go-server/api';
import { artworkURL, deleteObject, listVolumes, uploadObjects } from '../src/filer/httpclient.ts';

// fakeClient returns a client whose fetch records each request, and
// returns the response of the function
function fakeClient(respond: (method: string, path: string) => Response) {
    const requests: { method: string; path: string }[] = [];
    const client: Client = {
        base: 'http://localhost/api',
        fetch: (async (input: RequestInfo | URL, init: RequestInit = {}) => {
            const path = new URL(String(input)).pathname;
            const method = init.method ?? 'GET';
            requests.push({ method, path });
            return respond(method, path);
        }) as typeof fetch,
    };
    return { client, requests };
}

const json = (status: number, body?: unknown) => new Response(body === undefined ? null : JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
});

test('listVolumes', async (t) => {
    await t.test('gets the volumes', async () => {
        const list = { body: [{ name: 'media', url: 'file:///media', enabled: true, mounted: true }], backends: [{ scheme: 'file' }] };
        const { client, requests } = fakeClient(() => json(200, list));
        assert.deepEqual(await listVolumes(client), list);
        assert.deepEqual(requests, [{ method: 'GET', path: '/api/volume' }]);
    });

    await t.test("an error is the API's", async () => {
        const { client } = fakeClient(() => json(500, { code: 500, reason: 'broken' }));
        await assert.rejects(listVolumes(client), ApiError);
    });
});

test('artworkURL', () => {
    // Of the client's base, as the browser reaches it, of the page's location
    const saved = Object.getOwnPropertyDescriptor(globalThis, 'location');
    Object.defineProperty(globalThis, 'location', { value: { href: 'http://localhost/app/' }, configurable: true });
    try {
        assert.equal(artworkURL({ base: '/api/' }, 'ab12'), 'http://localhost/api/artwork/ab12');
        assert.equal(artworkURL({ base: 'http://server/api' }, 'ab12'), 'http://server/api/artwork/ab12');
    } finally {
        if (saved) {
            Object.defineProperty(globalThis, 'location', saved);
        } else {
            delete (globalThis as { location?: unknown }).location;
        }
    }
});

test('deleteObject', async () => {
    // A file is deleted, of its path, whose segments are escaped
    const { client, requests } = fakeClient(() => json(200, { body: [{ name: 'media', path: '/a b/c.txt' }] }));
    const deleted = await deleteObject(client, 'media', '/a b/c.txt');
    assert.deepEqual(requests, [{ method: 'DELETE', path: '/api/object/media/a%20b/c.txt' }]);
    assert.deepEqual(deleted.map((o) => o.path), ['/a b/c.txt']);
});

test('uploadObjects', async () => {
    // Files are uploaded into a folder, as a multipart form of their paths,
    // and overwrite those which exist, when it's asked to
    let form: FormData | undefined;
    let query = '';
    const client: Client = {
        base: 'http://localhost/api',
        fetch: (async (input: RequestInfo | URL, init: RequestInit = {}) => {
            const u = new URL(String(input));
            query = `${init.method} ${u.pathname}${u.search}`;
            form = init.body as FormData;
            return json(201, { body: [{ name: 'media', path: '/music/Photos/a.jpg' }], skipped: [{ path: '/music/b.txt', exists: true, reason: 'it exists' }] });
        }) as typeof fetch,
    };
    const result = await uploadObjects(client, 'media', '/music', [
        { file: new Blob(['a']), path: 'Photos/a.jpg' },
        { file: new Blob(['b']), path: 'b.txt' },
    ], true);
    assert.equal(query, 'POST /api/object/media/music?overwrite=true');
    const files = form?.getAll('file') as File[];
    assert.deepEqual(files.map((f) => f.name), ['Photos/a.jpg', 'b.txt']);
    assert.deepEqual(result.skipped?.map((s) => s.path), ['/music/b.txt']);

    // Into the root, without overwriting, and an error is thrown
    const failing = fakeClient(() => json(403, { code: 403, reason: 'read-only' }));
    await assert.rejects(uploadObjects(failing.client, 'media', '/', [{ file: new Blob(['a']), path: 'a.txt' }]));
    assert.deepEqual(failing.requests, [{ method: 'POST', path: '/api/object/media/' }]);
});
