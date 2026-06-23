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

-- filter.object
CREATE TABLE IF NOT EXISTS ${"schema"}."object" (
    "volume"      TEXT NOT NULL,
    "path"        TEXT NOT NULL,        -- absolute path or S3 key
    "size"        BIGINT NOT NULL,
    "type"        TEXT NOT NULL,
    "etag"        TEXT,
    "modified_at" TIMESTAMPTZ NOT NULL,
    "indexed_at" TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("volume", "path"),
    FOREIGN KEY ("volume") REFERENCES ${"schema"}."volume"("name") ON DELETE CASCADE
);

-- filter.meta
CREATE TABLE IF NOT EXISTS ${"schema"}."meta" (
    "volume"      TEXT NOT NULL,
    "path"        TEXT NOT NULL,
    "key"         TEXT NOT NULL,
    "value"       JSONB,
    PRIMARY KEY ("volume", "path", "key"),
    CHECK ("key" ~ '^[A-Za-z_][A-Za-z0-9_-]*$'),
    FOREIGN KEY ("volume", "path") REFERENCES ${"schema"}."object"("volume", "path") ON DELETE CASCADE
);

-- filer.search
CREATE TABLE IF NOT EXISTS ${"schema"}."search" (
    "volume"     TEXT NOT NULL,
    "path"       TEXT NOT NULL,
    "tsv"        TSVECTOR NOT NULL,
    "indexed_at" TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("volume", "path"),
    FOREIGN KEY ("volume", "path") REFERENCES ${"schema"}."object"("volume", "path") ON DELETE CASCADE
);

-- filer.search.index
CREATE INDEX IF NOT EXISTS idx_search_tsv ON ${"schema"}."search" USING GIN("tsv");

-- filer.search.update.function
CREATE OR REPLACE FUNCTION ${"schema"}.search_update(search_volume TEXT, search_path TEXT)
RETURNS void AS $$
DECLARE
  search_tsv TSVECTOR;
BEGIN
  SELECT
    setweight(to_tsvector('english', COALESCE((
      SELECT string_agg(part, ' ')
      FROM regexp_split_to_table(o."path", '[/.]+') AS part
      WHERE length(part) >= 3
    ), '')), 'C') ||
      setweight(to_tsvector('english', COALESCE(o."type", '')), 'B') ||
      setweight(to_tsvector('english', COALESCE((
        SELECT string_agg(m."value"::TEXT, ' ')
        FROM ${"schema"}."meta" AS m
        WHERE m."volume" = o."volume"
        AND m."path" = o."path"
        AND lower(m."key") = 'title'
      ), '')), 'A') ||
      setweight(to_tsvector('english', COALESCE((
        SELECT string_agg(m."value"::TEXT, ' ')
        FROM ${"schema"}."meta" AS m
        WHERE m."volume" = o."volume"
        AND m."path" = o."path"
        AND lower(m."key") = 'tags'
      ), '')), 'A') ||
      setweight(to_tsvector('english', COALESCE((
        SELECT string_agg(m."value"::TEXT, ' ')
        FROM ${"schema"}."meta" AS m
        WHERE m."volume" = o."volume"
        AND m."path" = o."path"
        AND lower(m."key") NOT IN ('title', 'tags')
      ), '')), 'D')
  INTO search_tsv
  FROM ${"schema"}."object" AS o
  WHERE o."volume" = search_volume
  AND o."path" = search_path;

  IF NOT FOUND THEN
    DELETE FROM ${"schema"}."search"
    WHERE "volume" = search_volume
    AND "path" = search_path;
    RETURN;
  END IF;

  INSERT INTO ${"schema"}."search" (
    "volume", "path", "tsv", "indexed_at"
  )
  VALUES (
    search_volume,
    search_path,
    search_tsv,
    now()
  )
  ON CONFLICT ("volume", "path") DO UPDATE
  SET
    "tsv" = EXCLUDED."tsv",
    "indexed_at" = EXCLUDED."indexed_at";

  RETURN;
END;
$$ LANGUAGE plpgsql;

-- filer.object.search.function
CREATE OR REPLACE FUNCTION ${"schema"}.object_search_update()
RETURNS trigger AS $$
BEGIN
  PERFORM ${"schema"}.search_update(NEW."volume", NEW."path");
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- filer.meta.search.function
CREATE OR REPLACE FUNCTION ${"schema"}.meta_search_update()
RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    PERFORM ${"schema"}.search_update(OLD."volume", OLD."path");
    RETURN OLD;
  END IF;

  PERFORM ${"schema"}.search_update(NEW."volume", NEW."path");
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- filer.object.search.trigger
DO $$ BEGIN
  DROP TRIGGER IF EXISTS object_search_update ON ${"schema"}."object";
  CREATE TRIGGER object_search_update
  AFTER INSERT OR UPDATE OF "path", "type" ON ${"schema"}."object"
  FOR EACH ROW
  EXECUTE FUNCTION ${"schema"}.object_search_update();
END $$;

-- filer.meta.search.trigger
DO $$ BEGIN
  DROP TRIGGER IF EXISTS meta_search_update ON ${"schema"}."meta";
  CREATE TRIGGER meta_search_update
  AFTER INSERT OR UPDATE OR DELETE ON ${"schema"}."meta"
  FOR EACH ROW
  EXECUTE FUNCTION ${"schema"}.meta_search_update();
END $$;

-- filer.credential
CREATE TABLE IF NOT EXISTS ${"schema"}."credential" (
  "key"                TEXT NOT NULL,
  "pv"                 INT NOT NULL DEFAULT 0,
  "credential"         BYTEA NOT NULL,
  "updated_at"         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY ("key"),
  CHECK ("key" ~ '^[A-Za-z_][A-Za-z0-9_]*$')
);

-- filer.llmprovider
CREATE TABLE IF NOT EXISTS ${"schema"}."llmprovider" (
  "name"               TEXT NOT NULL, -- unique name for the provider (for model selection)
  "provider"           TEXT NOT NULL, -- ollama, anthropic, openai, gemini, etc.
  "url"                TEXT,          -- used by local providers
  "credential"         TEXT,          -- used by providers that require an API Key
  "created_at"  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY ("name"),
  CHECK ("name" ~ '^[A-Za-z_][A-Za-z0-9_]*$'),
  FOREIGN KEY ("credential") REFERENCES ${"schema"}."credential"("key") ON DELETE RESTRICT
);

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

-- filer.notify.llmprovider.trigger
DO $$ BEGIN
  DROP TRIGGER IF EXISTS llmprovider_table_changes_notify ON ${"schema"}.llmprovider;
  CREATE TRIGGER llmprovider_table_changes_notify
  AFTER INSERT OR UPDATE OR DELETE ON ${"schema"}.llmprovider
  FOR EACH STATEMENT
  EXECUTE FUNCTION ${"schema"}.notify_table();
END $$;

-- filer.notify.credential.trigger
DO $$ BEGIN
  DROP TRIGGER IF EXISTS credential_table_changes_notify ON ${"schema"}.credential;
  CREATE TRIGGER credential_table_changes_notify
  AFTER INSERT OR UPDATE OR DELETE ON ${"schema"}.credential
  FOR EACH STATEMENT
  EXECUTE FUNCTION ${"schema"}.notify_table();
END $$;


