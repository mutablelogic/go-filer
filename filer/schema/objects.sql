-- filer.object
CREATE TABLE IF NOT EXISTS ${"schema"}."object" (
    name          TEXT NOT NULL,   
    path          TEXT NOT NULL,        -- relative or absolute path
    size          BIGINT,
    modified_at   TIMESTAMPTZ,
    type          TEXT,
    etag          TEXT, 
    meta JSONB    NOT NULL DEFAULT '{}'::JSONB,
    indexed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (name, path)
);
