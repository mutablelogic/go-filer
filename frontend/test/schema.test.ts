import { test } from 'node:test';
import assert from 'node:assert/strict';
import { volumeBackend, volumeIndexing, volumeNoun, volumeObjects, volumeStatus, volumeObjectsUnknown, volumeScheme, type Volume } from '../src/filer/schema.ts';

// volume returns a mounted volume of a URL, and its index
function volume(url: string, index?: Volume['index']): Volume {
    return { name: 'v', url, enabled: true, mounted: true, index };
}

test('volumeScheme', () => {
    assert.equal(volumeScheme(volume('gdrive://drive')), 'gdrive');
    assert.equal(volumeScheme(volume('file://media/mnt/media?description=Media')), 'file');
    assert.equal(volumeScheme(volume('not a url')), '');
});

test('volumeBackend', () => {
    const backends = [{ scheme: 'gdrive', description: 'Google Drive' }];
    assert.equal(volumeBackend(volume('gdrive://drive'), backends), 'Google Drive');
    assert.equal(volumeBackend(volume('s3://bucket/'), backends), 's3');
    assert.equal(volumeBackend(volume('not a url'), backends), '');
});

test('volumeObjects', async (t) => {
    // Its count, or undefined, with why, when it isn't known
    const check = (index: Volume['index'], want: number | undefined, why: string | RegExp) => {
        const v = volume('file://v/tmp', index);
        assert.equal(volumeObjects(v), want);
        if (typeof why === 'string') {
            assert.equal(volumeObjectsUnknown(v), why);
        } else {
            assert.match(volumeObjectsUnknown(v), why);
        }
    };
    await t.test('not indexed', () => check(undefined, undefined, "It isn't indexed"));
    await t.test('folders only', () => check({ mode: 'folders', status: 'ready', folders: 3 }, undefined, 'Only its folders are indexed'));
    await t.test('ready', () => check({ mode: 'metadata', status: 'ready', objects: 805 }, 805, ''));
    await t.test('ready and empty, which is left out', () => check({ mode: 'objects', status: 'ready' }, 0, ''));
    await t.test('never scanned', () => check({ mode: 'objects' }, undefined, "It hasn't been indexed yet"));
    await t.test('scanning, before a file is stored', () => check({ mode: 'objects', status: 'scanning' }, undefined, "It's being indexed"));
    await t.test('scanning, of the objects stored', () => check({ mode: 'objects', status: 'scanning', objects: 12 }, 12, ''));
    await t.test("its state can't be read", () => check({ mode: 'objects', error: 'database is locked' }, undefined, /database is locked/));
});

test('volumeNoun', () => {
    // What a backend's objects are called, of a count of them, or objects
    for (const [url, one, many] of [
        ['gtasks://tasks', 'task', 'tasks'],
        ['gcalendar://cal', 'event', 'events'],
        ['s3://media/', 'object', 'objects'],
        ['file://media/mnt/media', 'file', 'files'],
        ['rss://news/example.com/rss', 'item', 'items'],
        ['gdrive://drive', 'file', 'files'],
        ['gcontacts://contacts', 'contact', 'contacts'],
        ['other://x', 'object', 'objects'],
        // Of a scheme which is a property of every object, too
        ['constructor://x', 'object', 'objects'],
        ['tostring://x', 'object', 'objects'],
        ['__proto__://x', 'object', 'objects'],
    ]) {
        assert.equal(volumeNoun(volume(url), 1), one, url);
        assert.equal(volumeNoun(volume(url), 0), many, url);
        assert.equal(volumeNoun(volume(url), 805), many, url);
    }
});

test('volumeIndexing', () => {
    // Of the task which indexes their metadata, whose count of those it's
    // done has those which failed, or all of them without one
    const of = (index: Volume['index']) => volumeIndexing(volume('file://v/tmp', index));
    assert.deepEqual(of({ mode: 'metadata', status: 'ready', objects: 10, metadata: { indexed: 7, queued: 3 } }), { unindexed: 3, indexed: 7, failed: 0 });
    assert.deepEqual(of({ mode: 'metadata', status: 'ready', objects: 10, metadata: { indexed: 7, failed: 2, queued: 3 } }), { unindexed: 3, indexed: 5, failed: 2 });
    assert.deepEqual(of({ mode: 'metadata', status: 'ready', objects: 10, metadata: {} }), { unindexed: 10, indexed: 0, failed: 0 });
    assert.deepEqual(of({ mode: 'metadata', status: 'ready', objects: 10, metadata: { indexed: 12, failed: 20 } }), { unindexed: 0, indexed: 0, failed: 10 }, 'at most its objects');
    assert.deepEqual(of({ mode: 'objects', status: 'ready', objects: 10 }), { unindexed: 0, indexed: 10, failed: 0 });
    assert.deepEqual(of({ mode: 'objects', status: 'ready' }), { unindexed: 0, indexed: 0, failed: 0 });
    assert.equal(of({ mode: 'folders', status: 'ready', folders: 3 }), undefined);
    assert.equal(of({ mode: 'objects' }), undefined);
    assert.equal(of(undefined), undefined);
});

test('volumeStatus', () => {
    const of = (index: Volume['index'], mounted = true, error?: string) => volumeStatus({ ...volume('file://v/tmp', index), mounted, error });
    assert.equal(of(undefined, false), 'Not mounted');
    assert.equal(of(undefined, false, 'down'), 'Offline');
    assert.equal(of({ mode: 'objects', status: 'ready' }, true, 'down'), 'Offline', 'mounted, of an index, while its backend is offline');
    assert.equal(of(undefined), 'Not indexed');
    assert.equal(of({ mode: 'objects', status: 'scanning', phase: 'scanning' }), 'Scanning');
    assert.equal(of({ mode: 'objects', status: 'scanning' }), 'Scanning');
    assert.equal(of({ mode: 'objects', status: 'scanning', phase: 'indexing' }), 'Indexing');
    assert.equal(of({ mode: 'metadata', status: 'ready', metadata: { queued: 4 } }), 'Indexing');
    assert.equal(of({ mode: 'objects', status: 'ready', phase: 'scanning' }), 'Scanning', 'again');
    assert.equal(of({ mode: 'objects', status: 'ready' }), 'Ready');
    assert.equal(of({ mode: 'objects', status: 'scanning', error: 'it failed' }), 'Failed', 'its first scan failed, which cleared its phase');
    assert.equal(of({ mode: 'objects', status: 'ready', error: 'it failed' }), 'Failed', 'a scan again, or its watch, failed');
    assert.equal(of({ mode: 'metadata', status: 'ready', error: 'it failed', metadata: { queued: 4 } }), 'Failed');
    assert.equal(of({ mode: 'objects', error: 'it failed' }), 'Failed');
    assert.equal(of({ mode: 'objects' }), 'Not indexed yet');
});
