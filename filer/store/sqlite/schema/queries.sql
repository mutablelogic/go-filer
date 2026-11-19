-- volume.create
-- A volume, which is a conflict when one of its name is stored
INSERT INTO volume (name, url, enabled, created_at)
VALUES (@name, @url, @enabled, @now)
RETURNING name, url, enabled, created_at, modified_at;

-- volume.update
-- A volume's url and enabled, which are only modified, and returned, when
-- they change
UPDATE volume SET url = @url, enabled = @enabled, modified_at = @now
WHERE name = @name AND (url IS NOT @url OR enabled IS NOT @enabled)
RETURNING name, url, enabled, created_at, modified_at;

-- volume.get
SELECT name, url, enabled, created_at, modified_at FROM volume WHERE name = @name;

-- volume.list
SELECT name, url, enabled, created_at, modified_at FROM volume ORDER BY name;

-- volume.delete
-- A volume, and its index, which cascades
DELETE FROM volume WHERE name = @name RETURNING name, url, enabled, created_at, modified_at;

-- volume.state
-- Its state, whose NULLs are its zero values
SELECT coalesce(status, ''), coalesce(folders, 0), coalesce(files, 0), coalesce(scan_mode, ''), index_started, index_timestamp, coalesce(error, '')
FROM volume WHERE name = @name;

-- volume.set_state
UPDATE volume SET
  status = nullif(@status, ''), folders = nullif(@folders, 0), files = nullif(@files, 0), scan_mode = nullif(@mode, ''),
  index_started = @started, index_timestamp = @scanned, error = nullif(@error, '')
WHERE name = @name
RETURNING coalesce(status, ''), coalesce(folders, 0), coalesce(files, 0), coalesce(scan_mode, ''), index_started, index_timestamp, coalesce(error, '');

-- object.put
-- An object, which replaces the one of its key, and its metadata, of a JSON
-- array of entries, which replaces its metadata
INSERT INTO object (volume, key, type, size, etag, modified_at)
VALUES (@volume, @key, nullif(@type, ''), @size, @etag, @modified_at)
ON CONFLICT (volume, key) DO UPDATE SET
  type = excluded.type, size = excluded.size, etag = excluded.etag, modified_at = excluded.modified_at;
DELETE FROM metadata WHERE volume = @volume AND object = @key;
INSERT INTO metadata (volume, object, namespace, key, value)
SELECT @volume, @key, nullif(entry.value ->> '$.namespace', ''), entry.value ->> '$.key', coalesce(entry.value -> '$.value', 'null')
FROM json_each(@meta) AS entry
ORDER BY entry.id;

-- object.get
SELECT key, coalesce(type, ''), size, etag, modified_at, (
  SELECT json_group_array(json_object('namespace', namespace, 'key', metadata.key, 'value', json(value)) ORDER BY rowid)
  FROM metadata WHERE metadata.volume = object.volume AND metadata.object = object.key
)
FROM object WHERE volume = @volume AND key = @key;

-- object.list
-- The objects of a folder, or beneath it, of the where clause, in the order
-- of their keys, after the key the listing starts after
SELECT key, coalesce(type, ''), size, etag, modified_at, (
  SELECT json_group_array(json_object('namespace', namespace, 'key', metadata.key, 'value', json(value)) ORDER BY rowid)
  FROM metadata WHERE metadata.volume = object.volume AND metadata.object = object.key
)
FROM object WHERE volume = @volume AND ${where} AND key > @after
ORDER BY key;

-- object.count
-- How many objects a listing has, of the where clause, of every page
SELECT count(*) FROM object WHERE volume = @volume AND ${where};

-- object.delete
-- An object, and, of a folder's key, those beneath it, of the range of their
-- keys, and their metadata, which cascades
DELETE FROM object
WHERE volume = @volume AND (key = @key OR (@folder AND key > @lower AND key < @upper));

-- queue.push_index
-- The index of a key, unless a rescan of it, or a folder above it, but the
-- root, is queued, and isn't claimed, as a rescan which is claimed can have
-- passed it, and a rescan of the root is a catch-up, of changes which can
-- have been missed, which doesn't hold up reads, so the index of a change,
-- which does, isn't of it, which is due at the earlier of the two, so work
-- which is pushed again and again isn't put off, or, when it's claimed, is
-- claimed again, when the push is due
INSERT INTO object_state (volume, key, kind, after)
SELECT @volume, @key, @kind, @after
WHERE NOT EXISTS (
  SELECT 1 FROM object_state
  WHERE volume = @volume AND kind = 'rescan' AND key IN (SELECT value FROM json_each(@ancestors)) AND key <> '/'
    AND (leased_until IS NULL OR julianday(leased_until) <= julianday('now'))
)
ON CONFLICT (volume, key, kind) DO UPDATE SET
  after = CASE
    WHEN leased_until IS NOT NULL AND julianday(leased_until) > julianday('now') THEN excluded.after
    WHEN julianday(excluded.after) < julianday(after) THEN excluded.after
    ELSE after
  END,
  leased_until = NULL;

-- queue.push_rescan
-- The rescan of a folder, which replaces the index and rescan work beneath
-- it, unless a rescan of a folder above it, but the root, is queued, and
-- isn't claimed. A rescan of the root is a catch-up, which doesn't hold up
-- reads, so it doesn't replace work, nor is a rescan of a folder of it, as
-- the index of a change, and a rescan of a folder, which can have absorbed
-- one, do.
DELETE FROM object_state
WHERE volume = @volume AND kind IN ('index', 'rescan') AND (key > @lower AND key < @upper OR (key = @key AND kind = 'index'))
AND @key <> '/'
AND NOT EXISTS (
  SELECT 1 FROM object_state AS above
  WHERE above.volume = @volume AND above.kind = 'rescan' AND above.key IN (SELECT value FROM json_each(@ancestors)) AND above.key <> '/'
    AND (above.leased_until IS NULL OR julianday(above.leased_until) <= julianday('now'))
);
INSERT INTO object_state (volume, key, kind, after)
SELECT @volume, @key, @kind, @after
WHERE NOT EXISTS (
  SELECT 1 FROM object_state
  WHERE volume = @volume AND kind = 'rescan' AND key IN (SELECT value FROM json_each(@ancestors)) AND key <> '/'
    AND (leased_until IS NULL OR julianday(leased_until) <= julianday('now'))
)
ON CONFLICT (volume, key, kind) DO UPDATE SET
  after = CASE
    WHEN leased_until IS NOT NULL AND julianday(leased_until) > julianday('now') THEN excluded.after
    WHEN julianday(excluded.after) < julianday(after) THEN excluded.after
    ELSE after
  END,
  leased_until = NULL;

-- queue.due
-- The work of the kinds which is due, and isn't leased, earliest first
SELECT key, kind, after, attempts, coalesce(error, ''), leased_until FROM object_state
WHERE volume = @volume AND kind IN (SELECT value FROM json_each(@kinds))
  AND julianday(after) <= julianday(@now)
  AND (leased_until IS NULL OR julianday(leased_until) <= julianday(@now))
ORDER BY julianday(after), key;

-- queue.lease
UPDATE object_state SET leased_until = @leased
WHERE volume = @volume AND key = @key AND kind = @kind
RETURNING key, kind, after, attempts, coalesce(error, ''), leased_until;

-- queue.done
-- Work which is done, of its claim, which is kept when it's pushed again
-- since, as its lease is cleared
DELETE FROM object_state
WHERE volume = @volume AND key = @key AND kind = @kind AND leased_until = @leased;

-- queue.failed
UPDATE object_state SET attempts = attempts + 1, error = @error, after = @after, leased_until = NULL
WHERE volume = @volume AND key = @key AND kind = @kind AND leased_until = @leased
RETURNING key, kind, after, attempts, coalesce(error, ''), leased_until;

-- queue.pending
-- Whether a key has work which would change its index: an index or a
-- rescan of it, or of a folder above it, which can remove what's beneath
-- it, queued, or claimed, or, of a folder's listing, of a key beneath it,
-- but not a rescan of the root, which is a catch-up, of changes which can
-- have been missed, such as while the server wasn't running, which the index
-- is served while it's done, as it would be, had it not missed them
SELECT EXISTS (
  SELECT 1 FROM object_state
  WHERE volume = @volume AND key IN (SELECT value FROM json_each(@ancestors)) AND kind IN ('index', 'rescan')
    AND NOT (kind = 'rescan' AND key = '/')
) OR (@beneath AND EXISTS (
  SELECT 1 FROM object_state
  WHERE volume = @volume AND key > @lower AND key < @upper AND kind IN ('index', 'rescan')
));

-- revision.list
-- The revisions of the files of a folder, or beneath it, of the where clause
SELECT key, revision FROM object_state
WHERE volume = @volume AND kind = 'listed' AND ${where};

-- revision.put
INSERT INTO object_state (volume, key, kind, after, revision)
VALUES (@volume, @key, 'listed', @now, @revision)
ON CONFLICT (volume, key, kind) DO UPDATE SET revision = excluded.revision;

-- scan.seen
-- A key which a scan has seen, with the revision of its listing
INSERT INTO object_state (volume, key, kind, after, revision)
VALUES (@volume, @key, 'seen', @now, nullif(@revision, ''))
ON CONFLICT (volume, key, kind) DO UPDATE SET revision = excluded.revision;

-- scan.unsee
-- What a scan has seen beneath a folder
DELETE FROM object_state
WHERE volume = @volume AND kind = 'seen' AND key > @lower AND key < @upper;

-- scan.sweep
-- The objects beneath a folder which a scan of it hasn't seen, and their
-- metadata, which cascades, and their revisions, of their triggers
DELETE FROM object
WHERE volume = @volume AND key > @lower AND key < @upper AND key NOT IN (
  SELECT key FROM object_state WHERE volume = @volume AND kind = 'seen' AND key > @lower AND key < @upper
);

-- scan.changed
-- The files beneath a folder which a scan of it has seen, which aren't
-- indexed, or whose revisions aren't those they were stored of, after the
-- key, in the order of their keys
SELECT seen.key, coalesce(seen.revision, ''), object.key IS NOT NULL
FROM object_state AS seen
LEFT JOIN object ON object.volume = seen.volume AND object.key = seen.key
LEFT JOIN object_state AS listed ON listed.volume = seen.volume AND listed.key = seen.key AND listed.kind = 'listed'
WHERE seen.volume = @volume AND seen.kind = 'seen' AND seen.key > @lower AND seen.key < @upper
  AND seen.key NOT GLOB '*/' AND seen.key > @after
  AND (object.key IS NULL OR listed.revision IS NOT seen.revision)
ORDER BY seen.key;

-- scan.count_changed
-- How many files scan.changed returns, of every page
SELECT count(*)
FROM object_state AS seen
LEFT JOIN object ON object.volume = seen.volume AND object.key = seen.key
LEFT JOIN object_state AS listed ON listed.volume = seen.volume AND listed.key = seen.key AND listed.kind = 'listed'
WHERE seen.volume = @volume AND seen.kind = 'seen' AND seen.key > @lower AND seen.key < @upper
  AND seen.key NOT GLOB '*/'
  AND (object.key IS NULL OR listed.revision IS NOT seen.revision);
