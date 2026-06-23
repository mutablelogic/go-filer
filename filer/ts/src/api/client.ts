export interface Meta {
  key: string;
  value: unknown;
}

export interface Volume {
  name: string;
  url: string;
  enabled: boolean;
  index_delta: string | null;
  created_at: string;
  indexed_at: string | null;
  objects: number;
}

export interface VolumeList {
  count: number;
  body: Volume[];
}

export interface FilerObject {
  volume: string;
  path: string;
  size: number;
  type: string;
  etag: string | null;
  modified_at: string;
  meta: Meta[];
}

export interface SearchResult extends FilerObject {
  title: string;
  summary: string;
  rank: number;
}

export interface SearchList {
  count: number;
  body: SearchResult[];
}

export interface SearchParams {
  query: string;
  volumes?: string[];
  offset?: number;
  limit?: number;
}

export class FilerClient {
  constructor(private readonly baseUrl: string = '/api') {}

  async listVolumes(): Promise<VolumeList> {
    return this.get<VolumeList>('volume');
  }

  async search(params: SearchParams): Promise<SearchList> {
    const q = new URLSearchParams({ query: params.query });
    if (params.volumes?.length) q.set('volumes', params.volumes.join(','));
    if (params.offset != null) q.set('offset', String(params.offset));
    if (params.limit != null) q.set('limit', String(params.limit));
    return this.get<SearchList>(`search?${q}`);
  }

  private async get<T>(path: string): Promise<T> {
    const resp = await fetch(`${this.baseUrl}/${path}`);
    if (!resp.ok) throw new Error(`${resp.status} ${resp.statusText}`);
    return resp.json() as Promise<T>;
  }
}
