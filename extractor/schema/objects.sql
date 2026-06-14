-- extractor.metadata
CREATE TABLE IF NOT EXISTS ${"schema"}."metadata" (
    "key"         TEXT NOT NULL,
    "etag"        TEXT NOT NULL,
    "filename"    TEXT NOT NULL,
    "size"        BIGINT NOT NULL,
    "modified_at" TIMESTAMPTZ,
    "title"       TEXT,
    "media_type"  TEXT,
    "summary"     TEXT,
    "tags"        TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    "indexed_at"  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("key")
);

-- extractor.metadata_kv
CREATE TABLE IF NOT EXISTS ${"schema"}."metadata_kv" (
    "metadata"    TEXT NOT NULL,
    "key"         TEXT NOT NULL,
    "value"       JSONB,
    PRIMARY KEY ("metadata", "key"),
    CHECK ("key" ~ '^[A-Za-z_][A-Za-z0-9_-]*$'),
    FOREIGN KEY ("metadata") REFERENCES ${"schema"}."metadata"("key") ON DELETE CASCADE
);

-- extractor.metadata.tsv
ALTER TABLE ${"schema"}."metadata"
    ADD COLUMN IF NOT EXISTS "tsv" TSVECTOR
    GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce("title", '')), 'A') ||
        setweight(array_to_tsvector("tags"), 'A') ||
        setweight(to_tsvector('simple', coalesce("summary", '')), 'B')
    ) STORED
;

-- extractor.metadata.index
CREATE INDEX IF NOT EXISTS idx_extractor_metadata_tsv ON ${"schema"}."metadata" USING GIN("tsv");
