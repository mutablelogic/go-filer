// The client of the filer's API, which go-filer's registry/httphandler
// serves, at the client's prefix, such as /api

import { errorResponse, request, url, type Client } from '@mutablelogic/go-server/api';
import type { FilerObject, ObjectList, ObjectListQuery, ObjectUpload, Volume, VolumeList } from './schema.ts';

// listVolumes returns the volumes, in order of their names, and the
// backends volumes can be added of
export async function listVolumes(client: Client, signal?: AbortSignal): Promise<VolumeList> {
    return request<VolumeList>(client, 'GET', 'volume', { signal });
}

// updateVolume changes the options of a volume's URL, of a patch, where an
// option which is null is removed, so it's its default, and those which
// aren't in it are kept, and whether it's enabled, so it's mounted, or
// unmounted, when enabled is set, and returns the volume
export async function updateVolume(client: Client, name: string, patch: Record<string, unknown>, enabled?: boolean, signal?: AbortSignal): Promise<Volume> {
    return request<Volume>(client, 'PATCH', `volume/${encodeURIComponent(name)}`, {
        body: patch,
        query: enabled === undefined ? undefined : { enabled: String(enabled) },
        signal,
    });
}

// createVolume creates a volume, of its name, and its backend's URL, with its
// options, which is mounted, and returns it
export async function createVolume(client: Client, name: string, url: string, signal?: AbortSignal): Promise<Volume> {
    return request<Volume>(client, 'POST', 'volume', { body: { name, url }, signal });
}

// authorizeSignIn returns the URL a person signs in at, in a browser, as the
// credential of the key, such as GOOGLE_TOKEN, with the scopes, which
// redirects to the server's callback, which stores the sign-in, and tells
// the window which opened it
export async function authorizeSignIn(client: Client, key: string, scopes: readonly string[], signal?: AbortSignal): Promise<string> {
    const response = await request<{ url: string }>(client, 'POST', `credential/${encodeURIComponent(key)}/authorize`, {
        body: { redirect_uri: signInCallback(client), scopes },
        signal,
    });
    return response.url;
}

// signInCallback returns the URL of the server's callback of a sign-in, as
// the browser reaches it, which is registered with the server's web client
export function signInCallback(client: Client): string {
    return new URL(client.base.replace(/\/$/, '') + '/credential-signin/callback', location.href).href;
}

// deleteVolume deletes a volume, and its index, but not its files, and
// returns it
export async function deleteVolume(client: Client, name: string, signal?: AbortSignal): Promise<Volume> {
    return request<Volume>(client, 'DELETE', `volume/${encodeURIComponent(name)}`, { signal });
}

// objectPath returns the path of the API of a volume's file, or folder, of
// its path, beneath the client's base, which is the volume's root of "/"
function objectPath(name: string, path: string): string {
    const segments = path.replace(/^\/+/, '').split('/').filter(Boolean).map(encodeURIComponent).join('/');
    return `object/${encodeURIComponent(name)}/${segments}`;
}

// deleteObject deletes a volume's file, of its path, from its backend, and
// returns the objects which were deleted
export async function deleteObject(client: Client, name: string, path: string, signal?: AbortSignal): Promise<FilerObject[]> {
    const response = await request<{ body?: FilerObject[] }>(client, 'DELETE', objectPath(name, path), { signal });
    return response?.body ?? [];
}

// UploadFile is a file which is uploaded, of its path in the folder it's
// uploaded into, such as its name
export interface UploadFile {
    file: Blob;
    path: string;
}

// uploadObjects uploads files into a volume's folder, of a multipart form,
// replacing those which exist, when it overwrites them, and otherwise
// skipping them, and returns those which were uploaded, and skipped
export async function uploadObjects(client: Client, name: string, folder: string, files: readonly UploadFile[], overwrite = false, signal?: AbortSignal): Promise<ObjectUpload> {
    const form = new FormData();
    for (const { file, path } of files) {
        form.append('file', file, path);
    }
    const response = await (client.fetch ?? fetch)(url(client.base, objectPath(name, folder), overwrite ? { overwrite: 'true' } : undefined), {
        method: 'POST',
        headers: { Accept: 'application/json' },
        body: form,
        signal,
    });
    if (!response.ok) {
        throw await errorResponse(response);
    }
    return await response.json() as ObjectUpload;
}

// objectURL returns the URL of the content of a volume's file, of its path,
// as the browser reaches it, which shows it, or, when it's a download, is
// an attachment, which the browser saves
export function objectURL(client: Client, name: string, path: string, download = false): string {
    const segments = path.replace(/^\/+/, '').split('/').map(encodeURIComponent).join('/');
    const url = new URL(`${client.base.replace(/\/$/, '')}/object/${encodeURIComponent(name)}/${segments}`, location.href);
    if (download) {
        url.searchParams.set('download', 'true');
    }
    return url.href;
}

// backendIconURL returns the URL of the icon of the backend of the scheme,
// such as file, as the browser reaches it
export function backendIconURL(client: Client, scheme: string): string {
    return new URL(`${client.base.replace(/\/$/, '')}/backend/${encodeURIComponent(scheme)}/icon`, location.href).href;
}

// artworkURL returns the URL of the image of artwork, of its hash, as the
// browser reaches it, which it keeps, as it never changes
export function artworkURL(client: Client, hash: string): string {
    return new URL(`${client.base.replace(/\/$/, '')}/artwork/${encodeURIComponent(hash)}`, location.href).href;
}

// MarkdownPart is the start of a file's content, as Markdown, and whether
// there's more of it, which isn't shown
export interface MarkdownPart {
    text: string;
    more: boolean;
}

// readMarkdown returns the start of a file's content, as Markdown, such as a
// PDF's, which the server reads it as, or undefined, when it can't be read
// as Markdown
export async function readMarkdown(client: Client, name: string, path: string, signal?: AbortSignal): Promise<MarkdownPart | undefined> {
    const response = await (client.fetch ?? fetch)(objectURL(client, name, path), { headers: { Accept: 'text/markdown' }, signal });
    if (response.status === 406) {
        return undefined;
    } else if (!response.ok) {
        throw new Error(`The file couldn't be read: ${response.status} ${response.statusText}`);
    }
    return { text: await response.text(), more: response.headers.has('X-Markdown-Next') };
}

// listObjects returns a page of the objects of a volume's folder, which is
// its root by default. A folder's path is listed with a "/" after it, so a
// bucket's object of the same name isn't listed instead.
export async function listObjects(client: Client, name: string, query: ObjectListQuery = {}, signal?: AbortSignal): Promise<ObjectList> {
    const path = query.path && query.path !== '/' && !query.path.endsWith('/') ? query.path + '/' : query.path;
    return request<ObjectList>(client, 'GET', `object/${encodeURIComponent(name)}`, {
        query: { ...query, path },
        signal,
    });
}
