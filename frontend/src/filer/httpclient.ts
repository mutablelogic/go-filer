// The client of the filer's API, which go-filer's registry/httphandler
// serves, at the client's prefix, such as /api

import { request, type Client } from '@mutablelogic/go-server/api';
import type { ObjectList, ObjectListQuery, Volume, VolumeList } from './schema.ts';

// listVolumes returns the volumes, in order of their names, and the
// backends volumes can be added of
export async function listVolumes(client: Client, signal?: AbortSignal): Promise<VolumeList> {
    return request<VolumeList>(client, 'GET', 'volume', { signal });
}

// updateVolume changes the options of a volume's URL, of a patch, where an
// option which is null is removed, so it's its default, and those which
// aren't in it are kept, and returns the volume
export async function updateVolume(client: Client, name: string, patch: Record<string, unknown>, signal?: AbortSignal): Promise<Volume> {
    return request<Volume>(client, 'PATCH', `volume/${encodeURIComponent(name)}`, { body: patch, signal });
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
