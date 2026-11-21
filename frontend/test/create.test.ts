import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ApiError } from '@mutablelogic/go-server/api';
import { canBeWritable, signInMessage, signInNeeded, volumeCreate, type VolumeDraft } from '../src/volumes/create.ts';

// draft returns a draft of a backend, read-only and searched, by default
function draft(scheme: string, name: string, fields: Record<string, string> = {}, more: Partial<VolumeDraft> = {}): VolumeDraft {
    return { scheme, name, description: '', readonly: true, index: 'objects', fields, ...more };
}

// query returns a URL's options, of its keys, in order
function query(url: string): Record<string, string[]> {
    const result: Record<string, string[]> = {};
    for (const [key, value] of new URL(url).searchParams) {
        (result[key] ??= []).push(value);
    }
    return result;
}

test('volumeCreate', async (t) => {
    await t.test('a directory on the server', () => {
        const v = volumeCreate(draft('file', 'media', { path: '/mnt/My Media' }, { description: ' Films ', readonly: false }));
        assert.equal(v.name, 'media');
        assert.equal(v.url, 'file://media/mnt/My%20Media?description=Films&index=objects&readonly=false');
        assert.throws(() => volumeCreate(draft('file', 'media', { path: 'mnt/media' })), /absolute/);
        assert.throws(() => volumeCreate(draft('file', 'media', {})), /path is needed/);
        assert.match(volumeCreate(draft('file', 'media', { path: 'C:\\Media' })).url, /^file:\/\/media\/C%3A\/Media\?/);
        // A path is as it is, of its spaces
        assert.match(volumeCreate(draft('file', 'media', { path: '/mnt/media ' })).url, /^file:\/\/media\/mnt\/media%20\?/);
    });

    await t.test('an S3 bucket', () => {
        const v = volumeCreate(draft('s3', 'photos', {
            bucket: 'my.bucket/family/photos/', region: 'eu-central-1', endpoint: 'http://localhost:9000',
            'access-key-id': 'S3_KEY', 'secret-access-key': 'S3_SECRET', 'seaweedfs-filer-grpc': '',
        }));
        assert.match(v.url, /^s3:\/\/my\.bucket\/family\/photos\?/);
        assert.deepEqual(query(v.url), {
            'access-key-id': ['${S3_KEY}'], endpoint: ['http://localhost:9000'], index: ['objects'], name: ['photos'],
            readonly: ['true'], region: ['eu-central-1'], 'secret-access-key': ['${S3_SECRET}'],
        });
        // A bucket, and its prefix, are as they are, of its spaces
        assert.match(volumeCreate(draft('s3', 'media', { bucket: 'media/my photos ' })).url, /^s3:\/\/media\/my%20photos%20\?/);
        // Its name isn't an option when it's the bucket's
        assert.equal(query(volumeCreate(draft('s3', 'media', { bucket: 'media' })).url).name, undefined);
        assert.throws(() => volumeCreate(draft('s3', 'media', { bucket: 'media', 'access-key-id': 'not a name' })), /credential/);
    });

    await t.test('a feed', () => {
        const v = volumeCreate(draft('rss', 'news', { feed: 'http://example.com/feeds/world.xml?edition=uk' }));
        assert.match(v.url, /^rss:\/\/news\/example\.com\/feeds\/world\.xml\?/);
        assert.deepEqual(query(v.url), { edition: ['uk'], index: ['objects'], insecure: ['true'] }, "a feed isn't writable, and has no readonly");
        assert.equal(query(volumeCreate(draft('rss', 'news', { feed: 'https://example.com/rss' })).url).insecure, undefined);
        assert.throws(() => volumeCreate(draft('rss', 'news', { feed: 'ftp://example.com/rss' })), /http or https/);
        assert.ok(!canBeWritable('rss'));
    });

    await t.test("a Google account's", () => {
        assert.deepEqual(query(volumeCreate(draft('gdrive', 'drive')).url), { index: ['objects'], readonly: ['true'] });
        const cal = volumeCreate(draft('gcalendar', 'cal', { credential: 'GOOGLE_WORK', window: '30d' }, { readonly: false }));
        assert.match(cal.url, /^gcalendar:\/\/cal\?/);
        assert.deepEqual(query(cal.url), { credential: ['GOOGLE_WORK'], index: ['objects'], readonly: ['false'], window: ['30d'] });
        assert.equal(query(volumeCreate(draft('gtasks', 'tasks', { window: '30d' })).url).window, undefined, 'only of a calendar');
        assert.throws(() => volumeCreate(draft('gdrive', 'drive', { credential: 'GOOGLE TOKEN' })), /credential/);
    });

    await t.test('its name, and an unknown backend', () => {
        assert.throws(() => volumeCreate(draft('gdrive', '1drive')), /name/);
        assert.throws(() => volumeCreate(draft('gdrive', '')), /name/);
        assert.throws(() => volumeCreate(draft('ftp', 'files')), /can't be added/);
    });
});

test('signInNeeded', () => {
    // An error of the server, of how to sign in, of a volume whose user needs
    // to, of its credential, and scopes
    assert.deepEqual(signInNeeded(new ApiError(401, 'sign in', { server: {}, client_id: 'x', credential: 'GOOGLE_WORK', scopes: ['a', 1, 'b'] })), { key: 'GOOGLE_WORK', scopes: ['a', 'b'] });
    assert.deepEqual(signInNeeded(new ApiError(401, 'sign in', { client_id: 'x' })), { key: 'GOOGLE_TOKEN', scopes: [] });
    assert.equal(signInNeeded(new ApiError(401, 'unauthorized')), undefined);
    assert.equal(signInNeeded(new ApiError(400, 'bad', { client_id: 'x' })), undefined);
    assert.equal(signInNeeded(new Error('other')), undefined);
});

test('signInMessage', () => {
    const origin = 'http://localhost:8084';
    assert.deepEqual(signInMessage({ origin, data: { type: 'credential-signin', key: 'GOOGLE_TOKEN', ok: true, error: '' } }, origin), { key: 'GOOGLE_TOKEN', ok: true, error: '' });
    assert.deepEqual(signInMessage({ origin, data: { type: 'credential-signin', key: 'GOOGLE_TOKEN', ok: false, error: 'access_denied' } }, origin), { key: 'GOOGLE_TOKEN', ok: false, error: 'access_denied' });
    // Not of another origin, nor another type, nor without a key
    assert.equal(signInMessage({ origin: 'https://evil.example.com', data: { type: 'credential-signin', key: 'GOOGLE_TOKEN', ok: true } }, origin), undefined);
    assert.equal(signInMessage({ origin, data: { type: 'other', key: 'GOOGLE_TOKEN', ok: true } }, origin), undefined);
    assert.equal(signInMessage({ origin, data: 'credential-signin' }, origin), undefined);
});
