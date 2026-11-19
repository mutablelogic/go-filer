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
  parent          TEXT GENERATED ALWAYS AS (
                    rtrim(rtrim(key, '/'), replace(rtrim(key, '/'), '/', ''))
                  ) VIRTUAL,              -- the key of its folder, such as /a/, or empty for the root
  PRIMARY KEY (volume, key)
);

-- schema.object_parent
CREATE INDEX IF NOT EXISTS object_parent ON object (volume, parent, key);

-- schema.metadata
-- The metadata of the objects, one row per entry, in the order they're put
CREATE TABLE IF NOT EXISTS metadata (
  volume          TEXT NOT NULL,
  object          TEXT NOT NULL,          -- the key of its object
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
CREATE UNIQUE INDEX IF NOT EXISTS metadata_key ON metadata (volume, object, ifnull(namespace, ''), key);

-- schema.object_state
-- The work of indexing the objects of the volumes, by key and kind, which is
-- the queue, which outlasts a restart. A key needn't be indexed, as it can
-- be of an object which is new, or gone. A rescan is of a folder's key.
CREATE TABLE IF NOT EXISTS object_state (
  volume          TEXT NOT NULL REFERENCES volume (name) ON DELETE CASCADE ON UPDATE CASCADE,
  key             TEXT NOT NULL CHECK (key GLOB '/*' AND key NOT GLOB '*//*' AND (kind <> 'rescan' OR key GLOB '*/')),
  kind            TEXT NOT NULL CHECK (kind GLOB '[a-z]*' AND kind NOT GLOB '*[^a-z_]*'),
  etag            TEXT,                   -- of the object the work was done of, for enrichment
  after           TIMESTAMP NOT NULL,     -- when it's due
  attempts        INTEGER NOT NULL DEFAULT 0,
  leased_until    TIMESTAMP,              -- when its claim ends, which Done and Failed are of
  error           TEXT,                   -- why it last failed
  PRIMARY KEY (volume, key, kind)
);

-- schema.object_state_due
CREATE INDEX IF NOT EXISTS object_state_due ON object_state (volume, kind, after);
