import { test } from 'node:test';
import assert from 'node:assert/strict';
import { startFolder, type StartLister } from '../src/folder/start.ts';
import type { FilerObject } from '../src/filer/schema.ts';

// lister returns a lister of folders, of their paths, each of which has the
// counts given, or none, when they're undefined, and the folders with
// objects, of their listings, and counts which folders are listed
function lister(tree: Record<string, { objects?: number; subfolders?: number }>, withObjects: string[] = []): StartLister & { listed: string[] } {
    const listed: string[] = [];
    const parent = (path: string) => path.slice(0, path.lastIndexOf('/')) || '/';
    return {
        listed,
        async folders(path: string) {
            listed.push(path);
            return Object.keys(tree).filter((p) => p !== '/' && parent(p) === path).sort().map((p): FilerObject => {
                const meta = [];
                if (tree[p].objects !== undefined) meta.push({ namespace: 'folder', key: 'object-count', value: tree[p].objects });
                if (tree[p].subfolders !== undefined) meta.push({ namespace: 'folder', key: 'subfolder-count', value: tree[p].subfolders });
                return { name: 'v', path: p, dir: true, meta };
            });
        },
        async hasObjects(path: string) {
            return withObjects.includes(path) || (tree[path]?.objects ?? 0) > 0;
        },
    };
}

const now = new Date(2026, 10, 21); // November 2026

test('startFolder', async (t) => {
    await t.test('root', async () => {
        // A root with objects is the root, and one with no folders, too
        assert.equal(await startFolder('file', lister({ '/': { objects: 2 }, '/a': { objects: 1 } })), '/');
        assert.equal(await startFolder('file', lister({})), '/');
    });

    await t.test('descend', async () => {
        // An empty root is its first folder with objects, of their counts
        assert.equal(await startFolder('file', lister({ '/a': { objects: 0, subfolders: 0 }, '/b': { objects: 3, subfolders: 0 } })), '/b');

        // Or the first with objects, of the first with subfolders, and so on
        const deep = lister({ '/a': { objects: 0, subfolders: 1 }, '/a/x': { objects: 0, subfolders: 1 }, '/a/x/y': { objects: 4, subfolders: 0 }, '/b': { objects: 0, subfolders: 1 } });
        assert.equal(await startFolder('file', deep), '/a/x/y');

        // Of folders without counts, such as of a volume which isn't
        // indexed, their listings
        assert.equal(await startFolder('file', lister({ '/a': {}, '/b': {} }, ['/b'])), '/b');

        // None with objects is the root
        assert.equal(await startFolder('file', lister({ '/a': { objects: 0, subfolders: 0 } })), '/');
    });

    await t.test('dated', async () => {
        const months = (...paths: string[]) => Object.fromEntries(paths.map((p) => [p, { objects: 1 }]));

        // The current month
        assert.equal(await startFolder('gcalendar', lister(months('/2025', '/2025/2025-12', '/2026', '/2026/2026-03', '/2026/2026-11', '/2026/2026-12')), now), '/2026/2026-11');

        // Or the next which has something, of a later year, too
        assert.equal(await startFolder('gcalendar', lister(months('/2026', '/2026/2026-03', '/2026/2026-12')), now), '/2026/2026-12');
        assert.equal(await startFolder('gcalendar', lister(months('/2026', '/2026/2026-03', '/2027', '/2027/2027-02')), now), '/2027/2027-02');

        // Or else the latest before it, as a feed's is
        assert.equal(await startFolder('rss', lister(months('/2025', '/2025/2025-06', '/2026', '/2026/2026-03', '/2026/2026-10', '/undated')), now), '/2026/2026-10');
        assert.equal(await startFolder('rss', lister(months('/2024', '/2024/2024-01', '/2025', '/2025/2025-07')), now), '/2025/2025-07');

        // A feed of only undated items is of its folders, as another volume's
        assert.equal(await startFolder('rss', lister({ '/undated': { objects: 2 } }), now), '/undated');

        // Other volumes aren't dated
        assert.equal(await startFolder('file', lister(months('/2025', '/2025/2025-06', '/2026', '/2026/2026-11')), now), '/2025');
    });
});
