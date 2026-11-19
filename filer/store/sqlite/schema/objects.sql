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
  index_started   TIMESTAMP,              -- when the scan in progress, or the last, started
  index_timestamp TIMESTAMP,              -- when the last scan which finished did
  error           TEXT                    -- why the last scan, or the watch, failed
);

-- schema.object
-- The objects of the volumes, by their paths, which are in byte order, so a
-- folder's subtree is a range of them
CREATE TABLE IF NOT EXISTS object (
  volume          TEXT NOT NULL REFERENCES volume (name) ON DELETE CASCADE ON UPDATE CASCADE,
  path            TEXT NOT NULL,          -- such as /a/b.txt
  key             TEXT NOT NULL,          -- its path, with a / after a folder's, the order of a listing
  parent          TEXT NOT NULL,          -- the folder it's in, or empty for the root
  is_dir          BOOLEAN NOT NULL,
  type            TEXT,                   -- its content type
  size            INTEGER,                -- a file's size, or NULL
  etag            TEXT,
  modified_at     TIMESTAMP,              -- or NULL, such as of a filesystem's folder
  PRIMARY KEY (volume, path)
);

-- schema.object_parent
CREATE INDEX IF NOT EXISTS object_parent ON object (volume, parent, key);

-- schema.object_key
CREATE INDEX IF NOT EXISTS object_key ON object (volume, key);

-- schema.metadata
-- The metadata of the objects, one row per entry, in the order they're put
CREATE TABLE IF NOT EXISTS metadata (
  volume          TEXT NOT NULL,
  path            TEXT NOT NULL,
  namespace       TEXT CHECK (namespace IS NULL OR (
                    length(namespace) <= 64 AND namespace GLOB '[a-zA-Z]*'
                    AND namespace NOT GLOB '*[^a-zA-Z0-9_-]*')),   -- an identifier, or NULL for none
  key             TEXT NOT NULL CHECK (
                    length(key) <= 64 AND key GLOB '[a-zA-Z]*'
                    AND key NOT GLOB '*[^a-zA-Z0-9_-]*'),          -- an identifier
  value           TEXT NOT NULL,                                  -- its JSON
  text            TEXT,                                           -- its text, or NULL when it's value's string (#77)
  FOREIGN KEY (volume, path) REFERENCES object (volume, path) ON DELETE CASCADE ON UPDATE CASCADE
);

-- schema.metadata_key
CREATE UNIQUE INDEX IF NOT EXISTS metadata_key ON metadata (volume, path, ifnull(namespace, ''), key);
