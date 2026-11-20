// The client of the filer's API, which go-filer's registry/httphandler
// serves, at the client's prefix, such as /api

import { request, type Client } from '@mutablelogic/go-server/api';
import type { VolumeList } from './schema.ts';

// listVolumes returns the volumes, in order of their names, and the
// backends volumes can be added of
export async function listVolumes(client: Client, signal?: AbortSignal): Promise<VolumeList> {
    return request<VolumeList>(client, 'GET', 'volume', { signal });
}
