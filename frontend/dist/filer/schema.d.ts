export interface Volume {
    name: string;
    url: string;
    enabled: boolean;
    mounted: boolean;
    created?: string;
    modified?: string;
    error?: string;
}
export interface Backend {
    scheme: string;
    description?: string;
}
export interface VolumeList {
    body: Volume[];
    backends: Backend[];
}
export declare const StoreChangeSchema = "store";
export declare const VolumeChangeTable = "volume";
export declare const ObjectChangeSchema = "filer";
//# sourceMappingURL=schema.d.ts.map