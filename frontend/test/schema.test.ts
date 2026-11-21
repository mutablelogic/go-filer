import { test } from 'node:test';
import assert from 'node:assert/strict';
import { volumeBackend, volumeObjects, volumeScheme, type Volume } from '../src/filer/schema.ts';

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
    await t.test('not indexed', () => {
        assert.equal(volumeObjects(volume('file://v/tmp')), undefined);
    });
    await t.test('folders only', () => {
        assert.equal(volumeObjects(volume('file://v/tmp', { mode: 'folders', folders: 3 })), undefined);
    });
    await t.test('files', () => {
        assert.equal(volumeObjects(volume('file://v/tmp', { mode: 'metadata', files: 805 })), 805);
    });
    await t.test('none, which are left out', () => {
        assert.equal(volumeObjects(volume('file://v/tmp', { mode: 'objects' })), 0);
    });
});
