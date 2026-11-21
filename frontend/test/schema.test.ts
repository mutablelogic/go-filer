import { test } from 'node:test';
import assert from 'node:assert/strict';
import { volumeBackend, volumeObjects, volumeObjectsUnknown, volumeScheme, type Volume } from '../src/filer/schema.ts';

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
    await t.test('ready', () => check({ mode: 'metadata', status: 'ready', files: 805 }, 805, ''));
    await t.test('ready and empty, which is left out', () => check({ mode: 'objects', status: 'ready' }, 0, ''));
    await t.test('never scanned', () => check({ mode: 'objects' }, undefined, "It hasn't been indexed yet"));
    await t.test('scanning, before a file is stored', () => check({ mode: 'objects', status: 'scanning' }, undefined, "It's being indexed"));
    await t.test('scanning, of the files stored', () => check({ mode: 'objects', status: 'scanning', files: 12 }, 12, ''));
    await t.test("its state can't be read", () => check({ mode: 'objects', error: 'database is locked' }, undefined, /database is locked/));
});
