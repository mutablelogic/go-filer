import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ApiError, type Client } from '@mutablelogic/go-server/api';
import { artworkURL, listVolumes } from '../src/filer/httpclient.ts';

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
