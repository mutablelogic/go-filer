// The client of the filer's API, which go-filer's registry/httphandler
// serves, at the client's prefix, such as /api
import { request } from '@mutablelogic/go-server/api';
// listVolumes returns the volumes, in order of their names, and the
// backends volumes can be added of
export async function listVolumes(client, signal) {
    return request(client, 'GET', 'volume', { signal });
}
//# sourceMappingURL=httpclient.js.map