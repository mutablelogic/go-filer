// The filer's schema, which mirrors go-filer's schema, of what the UI shows

// Volume is a volume: its name, the URL of its backend, without secrets,
// whether it's enabled and mounted, and why it isn't, when it isn't
export interface Volume {
    name: string;
    url: string;
    enabled: boolean;
    mounted: boolean;
    created?: string;
    modified?: string;
    error?: string;
}

// Backend is a type of backend, of the scheme of its volumes' URLs
export interface Backend {
    scheme: string;
    description?: string;
}

// VolumeList is the volumes, in order of their names, and the backends
// volumes can be added of
export interface VolumeList {
    body: Volume[];
    backends: Backend[];
}

// The schema and table of the changes of the stored volumes, and the schema
// of the changes of their objects, whose table is the volume's name
export const StoreChangeSchema = 'store';
export const VolumeChangeTable = 'volume';
export const ObjectChangeSchema = 'filer';
