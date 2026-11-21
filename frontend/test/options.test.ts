import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Volume } from '../src/filer/schema.ts';
import { volumeOptions, volumePatch, volumeWritable, type VolumeOptions } from '../src/volumes/options.ts';

// volume returns a mounted volume of a URL
function volume(url: string): Volume {
    return { name: 'v', url, enabled: true, mounted: true };
}

test('volumeOptions', () => {
    // Its description, readonly and index, and its others, of their keys
    assert.deepEqual(volumeOptions(volume('file://media/mnt/media?description=Emby&index=metadata&readonly=false&skip=%5E%5C.&skip=~%24&package=x')), {
        description: 'Emby', readonly: false, index: 'metadata',
        others: [['package', ['x']], ['skip', ['^\\.', '~$']]],
    });
    // Of its backend's default, without the option
    assert.equal(volumeOptions(volume('gdrive://drive')).readonly, true);
    assert.equal(volumeOptions(volume('file://media/mnt')).readonly, false);
    assert.equal(volumeOptions(volume('file://media/mnt?readonly=')).readonly, true);
    assert.equal(volumeOptions(volume('file://media/mnt')).index, 'none');
    // A feed can't be writable, so it has no readonly
    assert.equal(volumeWritable(volume('rss://news/example.com/rss?readonly=true')), false);
    assert.equal(volumeOptions(volume('rss://news/example.com/rss?readonly=true')).readonly, undefined);
    assert.deepEqual(volumeOptions(volume('not a url')).others, []);
});

test('volumePatch', () => {
    const from: VolumeOptions = {
        description: 'Emby', readonly: false, index: 'metadata',
        others: [['quota', ['120']], ['skip', ['a', 'b']], ['package', ['x']]],
    };
    const edit = (to: Partial<VolumeOptions>) => volumePatch(from, { ...from, ...to });

    // Nothing, when nothing is changed
    assert.deepEqual(edit({}), {});
    // Those which are changed
    assert.deepEqual(edit({ description: ' Media ', readonly: true, index: 'objects' }), { description: 'Media', readonly: true, index: 'objects' });
    // A description which is empty is removed
    assert.deepEqual(edit({ description: '  ' }), { description: null });
    // An option of one value, which is removed when it's empty
    assert.deepEqual(edit({ others: [['quota', ['60']], ['skip', ['a', 'b']], ['package', ['x']]] }), { quota: '60' });
    assert.deepEqual(edit({ others: [['quota', ['']], ['skip', ['a', 'b']], ['package', ['x']]] }), { quota: null });
    // An option of many, as a list, of those which aren't empty, even of one
    assert.deepEqual(edit({ others: [['quota', ['120']], ['skip', ['a', '', 'c']], ['package', ['x']]] }), { skip: ['a', 'c'] });
    assert.deepEqual(edit({ others: [['quota', ['120']], ['skip', ['a', 'b']], ['package', []]] }), { package: [] });
    // A volume which can't be writable doesn't change it
    assert.deepEqual(volumePatch({ ...from, readonly: undefined }, { ...from, readonly: undefined }), {});
});
