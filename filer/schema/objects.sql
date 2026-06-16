-- filer.volume
CREATE TABLE IF NOT EXISTS ${"schema"}."volume" (
    "name"        TEXT NOT NULL,
    "url"         TEXT NOT NULL,
    "enabled"     BOOLEAN NOT NULL DEFAULT TRUE,
    "index_delta" INTERVAL,
    "created_at"  TIMESTAMPTZ NOT NULL DEFAULT now(),
    "indexed_at"  TIMESTAMPTZ,
    PRIMARY KEY ("name"),
    CHECK ("name" ~ '^[a-z_][a-z0-9_]*$'),
    UNIQUE ("url")
);

-- filer.metadata
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

-- filer.metadata_kv
CREATE TABLE IF NOT EXISTS ${"schema"}."metadata_kv" (
    "metadata"    TEXT NOT NULL,
    "key"         TEXT NOT NULL,
    "value"       JSONB,
    PRIMARY KEY ("metadata", "key"),
    CHECK ("key" ~ '^[A-Za-z_][A-Za-z0-9_-]*$'),
    FOREIGN KEY ("metadata") REFERENCES ${"schema"}."metadata"("key") ON DELETE CASCADE
);

-- filer.metadata.tsv
ALTER TABLE ${"schema"}."metadata"
    ADD COLUMN IF NOT EXISTS "tsv" TSVECTOR
    GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', coalesce("title", '')), 'A') ||
        setweight(array_to_tsvector("tags"), 'A') ||
        setweight(to_tsvector('simple', coalesce("summary", '')), 'B')
    ) STORED
;

-- filer.metadata.index
CREATE INDEX IF NOT EXISTS idx_filer_metadata_tsv ON ${"schema"}."metadata" USING GIN("tsv");

-- filer.notify.function
CREATE OR REPLACE FUNCTION ${"schema"}.notify_table()
RETURNS trigger AS $$
DECLARE
  lock_id BIGINT;
BEGIN
  lock_id := hashtextextended(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, 0);
  IF pg_try_advisory_xact_lock(lock_id) THEN
    PERFORM pg_notify(
      ${'notify_channel'},
      json_build_object(
        'schema', TG_TABLE_SCHEMA,
        'table', TG_TABLE_NAME,
        'action', TG_OP
      )::text
    );
  END IF;
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- filer.notify.volume.trigger
DO $$ BEGIN
  DROP TRIGGER IF EXISTS volume_table_changes_notify ON ${"schema"}.volume;
  CREATE TRIGGER volume_table_changes_notify
  AFTER INSERT OR UPDATE OR DELETE ON ${"schema"}.volume
  FOR EACH STATEMENT
  EXECUTE FUNCTION ${"schema"}.notify_table();
END $$;

