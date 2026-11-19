-- schema.volume
-- The volumes, which the server mounts when they're enabled, and the state
-- of each one's index
CREATE TABLE IF NOT EXISTS volume (
  name            TEXT PRIMARY KEY,
  url             TEXT NOT NULL,          -- its backend's, with its options, such as index
  enabled         BOOLEAN NOT NULL,       -- whether it's mounted, when the server starts
  created_at      TIMESTAMP NOT NULL,
  modified_at     TIMESTAMP,              -- when its url, or enabled, last changed
  status          TEXT,                   -- its index's: scanning, or ready
  folders         INTEGER,                -- how many folders the scan in progress, or the last, has indexed
  files           INTEGER,                -- how many files the last scan indexed, when its files are
  scan_mode       TEXT,                   -- what the last scan which finished indexed: folders, or objects
  phase           TEXT,                   -- what the scan in progress is doing: scanning, or indexing, or NULL, when it's idle
  index_started   TIMESTAMP,              -- when the scan in progress, or the last, started
  index_timestamp TIMESTAMP,              -- when the last scan which finished did
  error           TEXT                    -- why the last scan, or the watch, failed
);

-- schema.object
-- The objects of the volumes, by their keys, which are their paths, with a
-- / after a folder's, so a file, and a folder, of the same path, which a
-- bucket can have, are two, in byte order, so a folder's subtree is a range
-- of them, and its parent is the key of its folder, of its key
CREATE TABLE IF NOT EXISTS object (
  volume          TEXT NOT NULL REFERENCES volume (name) ON DELETE CASCADE ON UPDATE CASCADE,
  key             TEXT NOT NULL CHECK (key GLOB '/*' AND key NOT GLOB '*//*'),  -- such as /a/b.txt, or /a/, or the root, /
  type            TEXT,                   -- its content type
  size            INTEGER,                -- a file's size, or NULL
  etag            TEXT,
  modified_at     TIMESTAMP,              -- or NULL, such as of a filesystem's folder
  media_type      TEXT GENERATED ALWAYS AS (
                    CASE WHEN type IS NULL AND key GLOB '*/' THEN 'inode/directory'
                    ELSE lower(trim(CASE WHEN instr(type, ';') > 0 THEN substr(type, 1, instr(type, ';') - 1) ELSE type END))
                    END
                  ) VIRTUAL,              -- its type, without its parameters, in lower case, of a listing of a type
  media_subtype   TEXT GENERATED ALWAYS AS (
                    substr(media_type, instr(media_type, '/') + 1)
                  ) VIRTUAL,              -- its subtype, such as jpeg, of a listing of a subtype
  parent          TEXT GENERATED ALWAYS AS (
                    rtrim(rtrim(key, '/'), replace(rtrim(key, '/'), '/', ''))
                  ) VIRTUAL,              -- the key of its folder, such as /a/, or empty for the root
  PRIMARY KEY (volume, key)
);

-- schema.object_parent
CREATE INDEX IF NOT EXISTS object_parent ON object (volume, parent, key);

-- schema.object_type
CREATE INDEX IF NOT EXISTS object_type ON object (volume, media_type, key);

-- schema.object_subtype
CREATE INDEX IF NOT EXISTS object_subtype ON object (volume, media_subtype, key);

-- schema.metadata
-- The metadata of the objects, one row per entry, in the order they're put,
-- of its source: the backend's, which is empty, or a task's, such as
-- metadata, which a read of the object merges, the task's replacing the
-- backend's of the same namespace and key
CREATE TABLE IF NOT EXISTS metadata (
  volume          TEXT NOT NULL,
  object          TEXT NOT NULL,          -- the key of its object
  source          TEXT NOT NULL DEFAULT '' CHECK (source = '' OR (
                    source GLOB '[a-z]*' AND source NOT GLOB '*[^a-z_]*')),   -- the backend's, or a task's kind
  namespace       TEXT CHECK (namespace IS NULL OR (
                    length(namespace) <= 64 AND namespace GLOB '[a-zA-Z]*'
                    AND namespace NOT GLOB '*[^a-zA-Z0-9_-]*')),   -- an identifier, or NULL for none
  key             TEXT NOT NULL CHECK (
                    length(key) <= 64 AND key GLOB '[a-zA-Z]*'
                    AND key NOT GLOB '*[^a-zA-Z0-9_-]*'),          -- an identifier
  value           TEXT NOT NULL,                                  -- its JSON
  text            TEXT,                                           -- its text, or NULL when it's value's string (#77)
  FOREIGN KEY (volume, object) REFERENCES object (volume, key) ON DELETE CASCADE ON UPDATE CASCADE
);

-- schema.metadata_key
CREATE UNIQUE INDEX IF NOT EXISTS metadata_key ON metadata (volume, object, source, ifnull(namespace, ''), key);

-- schema.object_state
-- The state of the objects of the volumes, by key and kind: the work of
-- indexing them, which is the queue, which outlasts a restart, whose key
-- needn't be indexed, as it can be of an object which is new, or gone, and
-- a rescan of which is of a folder's key, and the revision of an indexed
-- file, of kind listed, which is what the backend's listing said of it when
-- it was stored, which isn't work.
CREATE TABLE IF NOT EXISTS object_state (
  volume          TEXT NOT NULL REFERENCES volume (name) ON DELETE CASCADE ON UPDATE CASCADE,
  key             TEXT NOT NULL CHECK (key GLOB '/*' AND key NOT GLOB '*//*' AND (kind <> 'rescan' OR key GLOB '*/')),
  kind            TEXT NOT NULL CHECK (kind GLOB '[a-z]*' AND kind NOT GLOB '*[^a-z_]*'),
  etag            TEXT,                   -- of the object the work was done of, for enrichment
  after           TIMESTAMP NOT NULL,     -- when it's due
  attempts        INTEGER NOT NULL DEFAULT 0,
  leased_until    TIMESTAMP,              -- when its claim ends, which Done and Failed are of
  error           TEXT,                   -- why it last failed
  revision        TEXT,                   -- of a listed file, what the backend's listing said of it
  PRIMARY KEY (volume, key, kind)
);

-- schema.object_updated
-- A file which is put again has no revision, until it's set again
CREATE TRIGGER IF NOT EXISTS object_updated AFTER UPDATE ON object BEGIN
  DELETE FROM object_state WHERE volume = old.volume AND key = old.key AND kind = 'listed';
END;

-- schema.object_deleted
-- A file which is deleted has no revision, nor the work of its tasks, nor
-- the revisions they were done of, but the work of indexing it, and of a
-- scan, is kept, as it's of the backend
CREATE TRIGGER IF NOT EXISTS object_deleted AFTER DELETE ON object BEGIN
  DELETE FROM object_state WHERE volume = old.volume AND key = old.key AND kind NOT IN ('index', 'rescan', 'seen');
END;

-- schema.object_state_due
CREATE INDEX IF NOT EXISTS object_state_due ON object_state (volume, kind, after);
