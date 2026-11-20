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

-- schema.object_geo
-- The shapes of where the objects are, of GeoJSON, such as where a photo
-- was taken, of their sources, as metadata is, which a task finds, and the
-- boxes of them, of their longitudes and latitudes, of which a search finds
-- them
CREATE TABLE IF NOT EXISTS object_geo (
  volume          TEXT NOT NULL,
  object          TEXT NOT NULL,          -- the key of its object
  source          TEXT NOT NULL DEFAULT '' CHECK (source = '' OR (
                    source GLOB '[a-z]*' AND source NOT GLOB '*[^a-z_]*')),   -- the backend's, or a task's kind
  shape           TEXT NOT NULL CHECK (json_valid(shape) AND shape ->> '$.type' IN ('Point', 'Polygon', 'MultiPolygon')),   -- its GeoJSON
  min_long        REAL NOT NULL CHECK (min_long BETWEEN -180 AND 180),
  min_lat         REAL NOT NULL CHECK (min_lat BETWEEN -90 AND 90),
  max_long        REAL NOT NULL CHECK (max_long BETWEEN min_long AND 180),
  max_lat         REAL NOT NULL CHECK (max_lat BETWEEN min_lat AND 90),
  FOREIGN KEY (volume, object) REFERENCES object (volume, key) ON DELETE CASCADE ON UPDATE CASCADE
);

-- schema.object_geo_shape
-- An object has a shape once, of a source
CREATE UNIQUE INDEX IF NOT EXISTS object_geo_shape ON object_geo (volume, object, source, shape);

-- schema.object_event
-- The times of the events of the objects, such as a calendar's, of their
-- sources, as metadata is, which a task finds: each time an event, or a
-- task, occurs, of its UID, and recurrence ID, which identify it, as an
-- event can be of more than one object, and its start, and end, which is
-- exclusive, which are dates, of an all-day event, or times, in UTC, of a
-- fixed width, so they sort, and the zone of its times
CREATE TABLE IF NOT EXISTS object_event (
  volume          TEXT NOT NULL,
  object          TEXT NOT NULL,          -- the key of its object
  source          TEXT NOT NULL DEFAULT '' CHECK (source = '' OR (
                    source GLOB '[a-z]*' AND source NOT GLOB '*[^a-z_]*')),   -- the backend's, or a task's kind
  uid             TEXT NOT NULL CHECK (uid <> ''),
  recurrence_id   TEXT,                   -- of a time an event which repeats occurs, the start it was to have
  kind            TEXT NOT NULL CHECK (kind IN ('event', 'task')),
  title           TEXT,
  start_date      TEXT CHECK (start_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
  start_at        TEXT CHECK (start_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]Z'),
  end_date        TEXT CHECK (end_date IS NULL OR (end_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' AND start_date IS NOT NULL AND end_date >= start_date)),
  end_at          TEXT CHECK (end_at IS NULL OR (end_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]Z' AND start_at IS NOT NULL AND end_at >= start_at)),
  tz              TEXT,                   -- such as Europe/Berlin, or NULL, of a date
  CHECK ((start_date IS NULL) <> (start_at IS NULL)),
  FOREIGN KEY (volume, object) REFERENCES object (volume, key) ON DELETE CASCADE ON UPDATE CASCADE
);

-- schema.object_event_occurrence
-- An object has an occurrence once, of a source
CREATE UNIQUE INDEX IF NOT EXISTS object_event_occurrence ON object_event (volume, object, source, uid, ifnull(recurrence_id, ''));

-- schema.object_event_at
-- The events of a volume, of their times, and of their dates, apart, of which
-- a search finds those of a time, or a day
CREATE INDEX IF NOT EXISTS object_event_at ON object_event (volume, start_at) WHERE start_at IS NOT NULL;

-- schema.object_event_date
CREATE INDEX IF NOT EXISTS object_event_date ON object_event (volume, start_date) WHERE start_date IS NOT NULL;

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
  outages         INTEGER NOT NULL DEFAULT 0, -- failures while its backend was unavailable, which aren't attempts
  leased_until    TIMESTAMP,              -- when its claim ends, which Done and Failed are of
  error           TEXT,                   -- why it last failed
  revision        TEXT,                   -- of a listed file, what the backend's listing said of it
  failed          TIMESTAMP,              -- when it last failed, of its error, rather than when it's due again
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

-- schema.object_search
-- The search of the objects, of their names, the main fields of their
-- metadata, which are its title, creator, identifier, description and
-- subject, the names of their folders, their own, the one above it, and
-- those above them, which are weighed less, the further they are, and the
-- text of their other metadata, of the rowids of their objects, which it doesn't keep the text
-- of, so it's an index alone. A VACUUM, which can renumber the objects'
-- rowids, needs it rebuilt, which an upgrade of the schema does.
CREATE VIRTUAL TABLE IF NOT EXISTS object_search USING fts5(
  name, title, folder, parent, path, meta,
  content = '', contentless_delete = 1,
  tokenize = 'unicode61 remove_diacritics 2', prefix = '2 3'
);

-- schema.object_search_deleted
-- An object which is deleted isn't searched
CREATE TRIGGER IF NOT EXISTS object_search_deleted AFTER DELETE ON object BEGIN
  DELETE FROM object_search WHERE rowid = old.rowid;
END;

-- schema.object_search_build
-- The search of every object, which is built again when the schema is
-- upgraded, as of search.refresh
DELETE FROM object_search;

-- schema.object_search_built
INSERT INTO object_search (rowid, name, title, folder, parent, path, meta)
SELECT object.rowid, search_name(object.key),
  coalesce((
    SELECT group_concat(search_text(metadata.value), ' ') FROM metadata
    WHERE metadata.volume = object.volume AND metadata.object = object.key
      AND metadata.namespace = 'dc' AND metadata.key IN ('title', 'creator', 'identifier', 'description', 'subject')
      AND NOT (metadata.source = '' AND EXISTS (
        SELECT 1 FROM metadata AS task
        WHERE task.volume = metadata.volume AND task.object = metadata.object AND task.source <> ''
          AND ifnull(task.namespace, '') = ifnull(metadata.namespace, '') AND task.key = metadata.key))
  ), ''),
  search_folder(object.key, 1), search_folder(object.key, 2), search_path(object.key),
  coalesce((
    SELECT group_concat(search_text(metadata.value), ' ') FROM metadata
    WHERE metadata.volume = object.volume AND metadata.object = object.key
      AND NOT (ifnull(metadata.namespace, '') = 'dc' AND metadata.key IN ('title', 'creator', 'identifier', 'description', 'subject'))
      AND NOT (ifnull(metadata.namespace, '') = 'metadata' AND metadata.key = 'hint') AND ifnull(metadata.namespace, '') <> 'folder'
      AND NOT (metadata.source = '' AND EXISTS (
        SELECT 1 FROM metadata AS task
        WHERE task.volume = metadata.volume AND task.object = metadata.object AND task.source <> ''
          AND ifnull(task.namespace, '') = ifnull(metadata.namespace, '') AND task.key = metadata.key))
  ), '')
FROM object;
