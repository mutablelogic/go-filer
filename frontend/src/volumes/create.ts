// A volume which is added: its fields, of its backend, of the CLI's
// volume-create commands, and the URL of its backend, which they make, as
// the CLI does

import { ApiError } from '@mutablelogic/go-server/api';

///////////////////////////////////////////////////////////////////////////////
// TYPES

// VolumeField is a field of a backend's volume, which is added: its key, its
// label, what it is, an example, and whether it's needed
export interface VolumeField {
    key: string;
    label: string;
    helper?: string;
    placeholder?: string;
    required?: boolean;
}

// VolumeDraft is a volume which is added: its backend's scheme, its name,
// its description, whether it's read-only, how it's searched, and its
// backend's fields, by their keys
export interface VolumeDraft {
    scheme: string;
    name: string;
    description: string;
    readonly: boolean;
    index: string;
    fields: Record<string, string>;
}

// VolumeCreate is a volume which is created: its name, and its backend's URL
export interface VolumeCreate {
    name: string;
    url: string;
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// The schemes of the backends
const SchemeFile = 'file';
const SchemeS3 = 's3';
const SchemeRSS = 'rss';
const SchemeCalendar = 'gcalendar';
const GoogleSchemes = new Set(['gcalendar', 'gcontacts', 'gdrive', 'gtasks']);

// The fields of a Google account's sign-in, and of a calendar's window
const CredentialField: VolumeField = {
    key: 'credential', label: 'Sign-in', placeholder: 'GOOGLE_TOKEN',
    helper: "The key of the server's sign-in with Google, which is GOOGLE_TOKEN, unless it's another account's, such as GOOGLE_WORK",
};
const WindowField: VolumeField = {
    key: 'window', label: 'Window', placeholder: '365d',
    helper: 'The time of events before today, and after it, such as 30d, 52w or 720h',
};

// VolumeFields are the fields of each backend's volume, of its scheme
export const VolumeFields = new Map<string, readonly VolumeField[]>([
    [SchemeFile, [
        { key: 'path', label: 'Path', placeholder: '/mnt/media', required: true, helper: 'The absolute path of a directory, on the server' },
    ]],
    [SchemeS3, [
        { key: 'bucket', label: 'Bucket', placeholder: 'my.bucket/photos', required: true, helper: 'The bucket, and optionally a prefix of its keys' },
        { key: 'region', label: 'Region', helper: "The bucket's region, which is its own, when it isn't set" },
        { key: 'endpoint', label: 'Endpoint', placeholder: 'http://localhost:9000', helper: 'The URL of a service which is compatible with S3, such as MinIO or SeaweedFS' },
        { key: 'access-key-id', label: 'Access key ID credential', placeholder: 'S3_KEY', helper: "The name of the server's credential of the access key ID, which it looks up" },
        { key: 'secret-access-key', label: 'Secret access key credential', placeholder: 'S3_SECRET', helper: "The name of the server's credential of the secret access key, which it looks up" },
        { key: 'seaweedfs-filer-grpc', label: 'SeaweedFS filer', placeholder: 'host:18888', helper: "The gRPC service of the SeaweedFS filer, whose bucket's objects are watched" },
    ]],
    [SchemeRSS, [
        { key: 'feed', label: 'Feed', placeholder: 'https://example.com/rss.xml', required: true, helper: 'The URL of the feed, which is http or https' },
    ]],
    ['gcalendar', [CredentialField, WindowField]],
    ['gcontacts', [CredentialField]],
    ['gdrive', [CredentialField]],
    ['gtasks', [CredentialField]],
]);

// reIdentifier is a volume's name, as the server's identifiers are, and
// reCredential the name of a credential, as an environment variable's
const reIdentifier = /^[a-zA-Z][a-zA-Z0-9_-]{0,63}$/;
const reCredential = /^[A-Za-z_][A-Za-z0-9_]*$/;

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// canBeWritable returns whether a backend's volume can be writable, which a
// feed's can't
export function canBeWritable(scheme: string): boolean {
    return scheme !== SchemeRSS;
}

// volumeCreate returns the volume of a draft, of its name, and its backend's
// URL, as the CLI's volume-create commands make it, or throws why it can't
// be, such as a name which isn't an identifier, or a field which is needed
// and isn't set
export function volumeCreate(draft: VolumeDraft): VolumeCreate {
    const name = draft.name.trim();
    if (!reIdentifier.test(name)) {
        throw new Error('The name is a letter, and then letters, digits, - and _, such as media');
    }
    for (const field of VolumeFields.get(draft.scheme) ?? []) {
        if (field.required && !value(draft, field.key)) {
            throw new Error(`The ${field.label.toLowerCase()} is needed`);
        }
    }
    const query = new URLSearchParams();
    let url: string;
    switch (draft.scheme) {
        case SchemeFile:
            url = fileURL(name, value(draft, 'path'));
            break;
        case SchemeS3:
            url = s3URL(name, draft, query);
            break;
        case SchemeRSS:
            url = rssURL(name, value(draft, 'feed'), query);
            break;
        default:
            if (!GoogleSchemes.has(draft.scheme)) {
                throw new Error(`A volume of ${draft.scheme} can't be added here`);
            }
            url = `${draft.scheme}://${name}`;
            setCredential(query, 'credential', value(draft, 'credential'), false);
            if (draft.scheme === SchemeCalendar && value(draft, 'window')) {
                query.set('window', value(draft, 'window'));
            }
    }
    if (canBeWritable(draft.scheme)) {
        query.set('readonly', String(draft.readonly));
    }
    if (draft.description.trim()) {
        query.set('description', draft.description.trim());
    }
    query.set('index', draft.index);
    query.sort();
    return { name, url: `${url}?${query}` };
}

// signInNeeded returns whether a volume can't be added, as its user needs to
// sign in, such as with Google, which the server says with how they sign in
export function signInNeeded(error: unknown): boolean {
    const detail = error instanceof ApiError ? error.detail : undefined;
    return error instanceof ApiError && error.status === 401 && typeof detail === 'object' && detail !== null && 'client_id' in detail;
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// value returns a field of a draft, without its spaces, other than a path,
// or a bucket, and its prefix, which are as they are, as a space can be of
// them, as the CLI's are
function value(draft: VolumeDraft, key: string): string {
    const v = draft.fields[key] ?? '';
    return RawFields.has(key) ? v : v.trim();
}

// RawFields are the fields which aren't trimmed, as a space can be of them
const RawFields = new Set(['path', 'bucket']);

// fileURL returns the URL of a directory on the server, whose path is
// absolute, of its elements, which are escaped
function fileURL(name: string, path: string): string {
    if (/^[A-Za-z]:[\\/]/.test(path)) {
        path = '/' + path.replaceAll('\\', '/');
    } else if (!path.startsWith('/')) {
        throw new Error('The path of the directory, on the server, is absolute, such as /mnt/media');
    }
    return `file://${name}${path.split('/').map(encodeURIComponent).join('/')}`;
}

// s3URL returns the URL of a bucket, and its prefix, whose name is the
// volume's, unless it's the bucket's, of its options, and its credentials,
// which are their names, as placeholders the server looks up
function s3URL(name: string, draft: VolumeDraft, query: URLSearchParams): string {
    const [bucket, ...rest] = value(draft, 'bucket').replace(/^\/+/, '').split('/');
    const prefix = rest.join('/').replace(/\/+$/, '');
    if (!bucket) {
        throw new Error('The bucket is needed');
    }
    if (name !== bucket) {
        query.set('name', name);
    }
    for (const key of ['region', 'endpoint', 'seaweedfs-filer-grpc']) {
        if (value(draft, key)) {
            query.set(key, value(draft, key));
        }
    }
    setCredential(query, 'access-key-id', value(draft, 'access-key-id'), true);
    setCredential(query, 'secret-access-key', value(draft, 'secret-access-key'), true);
    return `s3://${bucket}${prefix ? '/' + prefix.split('/').map(encodeURIComponent).join('/') : ''}`;
}

// rssURL returns the URL of a feed's volume, of its host and path, and its
// query, of which it's insecure, when it's http
function rssURL(name: string, feed: string, query: URLSearchParams): string {
    let u: URL;
    try {
        u = new URL(feed);
    } catch {
        throw new Error('The URL of the feed is http or https, such as https://example.com/rss.xml');
    }
    if ((u.protocol !== 'http:' && u.protocol !== 'https:') || !u.host) {
        throw new Error('The URL of the feed is http or https, such as https://example.com/rss.xml');
    }
    for (const [key, v] of u.searchParams) {
        query.append(key, v);
    }
    if (u.protocol === 'http:') {
        query.set('insecure', 'true');
    }
    return `rss://${name}/${u.host}${u.pathname}`;
}

// setCredential sets an option of the name of a credential, which is
// checked, as a placeholder of it, ${NAME}, when it's of a placeholder, or
// as it is
function setCredential(query: URLSearchParams, key: string, name: string, placeholder: boolean) {
    if (!name) {
        return;
    } else if (!reCredential.test(name)) {
        throw new Error(`The name of a credential is letters, digits and _, such as S3_SECRET: ${name}`);
    }
    query.set(key, placeholder ? '${' + name + '}' : name);
}
