// The options of a volume's URL, which are edited, and the patch of those
// which are changed, which updates it

import { volumeScheme, type Volume } from '../filer/schema.ts';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// VolumeOptions are a volume's options, of its URL: its description,
// whether it's read-only, unless its backend is (see volumeWritable), how
// it's indexed, and its other options, by their keys, in order, of their
// values, of which most have one, and some, such as skip, many
export interface VolumeOptions {
    description: string;
    readonly?: boolean;
    index: string;
    others: [string, string[]][];
}

// VolumePatch is the options which are changed, of their values, where null
// removes one, so it's its default, and a list is of many values
export type VolumePatch = Record<string, string | boolean | string[] | null>;

// IndexMode is a way a volume is indexed, as it's chosen, which can be one
// which is to come, so it can't be chosen yet
export interface IndexMode {
    value: string;
    label: string;
    description: string;
    disabled?: boolean;
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// The options which are edited of their own, rather than of the others
export const OptDescription = 'description';
export const OptReadonly = 'readonly';
export const OptIndex = 'index';

// AccessDescriptions are what a volume's access is, of whether it's
// read-only
export const AccessDescriptions = {
    readonly: 'Objects cannot be uploaded or deleted',
    writable: 'Objects can be uploaded or deleted, and additional actions may be available',
};

// IndexModes are the ways a volume is indexed: Off, On, of its files, and
// Metadata, of their metadata, too, and Artwork and Content, which are to
// come. A volume of folders, which isn't one of them, has none chosen.
export const IndexModes: readonly IndexMode[] = [
    { value: 'none', label: 'Off', description: 'The volume is not searchable' },
    { value: 'objects', label: 'On', description: "The volume's objects will be indexed for searching" },
    { value: 'metadata', label: 'Metadata', description: 'Additional metadata is added to improve searchability' },
    { value: 'artwork', label: 'Artwork', description: 'Artwork for each object is also added', disabled: true },
    { value: 'content', label: 'Content', description: "The object's file content (where available) is also used for search", disabled: true },
];

// FoldersDescription is what a volume of folders is, whose mode isn't one of
// IndexModes
const FoldersDescription = 'Only its folders are indexed';

// indexDescription returns what a way a volume is indexed is, of its value
export function indexDescription(value: string): string {
    return IndexModes.find((mode) => mode.value === value)?.description ?? (value === 'folders' ? FoldersDescription : '');
}

// ManyOptions are the options which have many values, of a list, even when
// they have one, or none
const ManyOptions = new Set(['skip', 'package']);

// ReadonlySchemes are the schemes of the backends which are read-only, so
// they can't be made writable
const ReadonlySchemes = new Set(['rss', 'webcal']);

// DefaultReadonly are the schemes of the backends which are read-only,
// unless they're made writable, of a URL without the option
const DefaultReadonly = new Set(['gcalendar', 'gcontacts', 'gdrive', 'gtasks']);

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// volumeWritable returns whether a volume can be made writable, which a
// feed, for one, can't
export function volumeWritable(volume: Volume): boolean {
    return !ReadonlySchemes.has(volumeScheme(volume));
}

// isMany returns whether an option has many values, of a list
export function isMany(key: string, values: readonly string[]): boolean {
    return ManyOptions.has(key) || values.length > 1;
}

// volumeOptions returns a volume's options, of its URL: its description,
// whether it's read-only, of its readonly option, or its backend's default,
// unless it can't be writable, its index option, which is none by default,
// and its others, in order of their keys
export function volumeOptions(volume: Volume): VolumeOptions {
    let query = new URLSearchParams();
    try {
        query = new URL(volume.url).searchParams;
    } catch {
        // A URL which can't be parsed has no options
    }
    let readonly: boolean | undefined;
    if (volumeWritable(volume)) {
        const value = query.get(OptReadonly);
        readonly = value === null ? DefaultReadonly.has(volumeScheme(volume)) : value === '' || value === 'true' || value === '1';
    }
    const others = new Map<string, string[]>();
    for (const [key, value] of query) {
        if (key !== OptDescription && key !== OptReadonly && key !== OptIndex) {
            others.set(key, [...(others.get(key) ?? []), value]);
        }
    }
    return {
        description: query.get(OptDescription) ?? '',
        readonly,
        index: query.get(OptIndex) || 'none',
        others: [...others].sort(([a], [b]) => a.localeCompare(b)),
    };
}

// volumePatch returns the patch of the options which are changed, of those
// which were: a description which is empty is removed, as is an option of
// one value, and an option of many is a list, of those which aren't empty.
// Options are as they are, without their spaces trimmed, as a space can be
// of them, such as a pattern of skip, ^ , so those which aren't changed
// aren't.
export function volumePatch(from: VolumeOptions, to: VolumeOptions): VolumePatch {
    const patch: VolumePatch = {};
    if (to.description.trim() !== from.description.trim()) {
        patch[OptDescription] = to.description.trim() || null;
    }
    if (to.readonly !== undefined && to.readonly !== from.readonly) {
        patch[OptReadonly] = to.readonly;
    }
    if (to.index !== from.index) {
        patch[OptIndex] = to.index;
    }
    const before = new Map(from.others);
    for (const [key, values] of to.others) {
        const previous = before.get(key) ?? [];
        if (isMany(key, previous)) {
            const list = values.filter((v) => v !== '');
            if (list.length !== previous.length || list.some((v, i) => v !== previous[i])) {
                patch[key] = list;
            }
        } else {
            const value = values[0] ?? '';
            if (value !== (previous[0] ?? '')) {
                patch[key] = value === '' ? null : value;
            }
        }
    }
    return patch;
}
