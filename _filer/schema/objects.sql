-- filer.object
CREATE TABLE IF NOT EXISTS ${"schema"}."object" (
    "volume"      TEXT NOT NULL,
    "path"        TEXT NOT NULL,        -- relative or absolute path
    "size"        BIGINT,
    "modified_at" TIMESTAMPTZ,
    "type"        TEXT,
    "etag"        TEXT,
    "meta"        JSONB NOT NULL DEFAULT '{}'::JSONB,
    "indexed_at"  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("volume", "path"),
    FOREIGN KEY ("volume") REFERENCES ${"schema"}."volume"("name") ON DELETE CASCADE
);

-- filer.metadata
CREATE TABLE IF NOT EXISTS ${"schema"}."metadata" (
    "volume"     TEXT NOT NULL,
    "path"       TEXT NOT NULL,
    "title"      TEXT,
    "summary"    TEXT,
    "text"       TEXT,
    "tags"       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    "created_at" TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY ("volume", "path"),
    FOREIGN KEY ("volume", "path") REFERENCES ${"schema"}."object"("volume", "path") ON DELETE CASCADE
);

-- filer.metadata.tsv
ALTER TABLE ${"schema"}."metadata"
    ADD COLUMN IF NOT EXISTS "tsv" TSVECTOR
    GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce("title", '')), 'A') ||
        setweight(array_to_tsvector("tags"), 'A') ||
        setweight(to_tsvector('simple', coalesce("summary", '')), 'B') ||
        setweight(to_tsvector('simple', coalesce("text", '')), 'C')
    ) STORED
;

-- filer.metadata.index
CREATE INDEX IF NOT EXISTS idx_file_metadata_tsv ON ${"schema"}."metadata" USING GIN("tsv");
