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
SELECT coalesce(status, ''), coalesce(folders, 0), coalesce(files, 0), coalesce(scan_mode, ''), coalesce(phase, ''), index_started, index_timestamp, coalesce(error, '')
FROM volume WHERE name = @name;

-- volume.set_state
UPDATE volume SET
  status = nullif(@status, ''), folders = nullif(@folders, 0), files = nullif(@files, 0), scan_mode = nullif(@mode, ''), phase = nullif(@phase, ''),
  index_started = @started, index_timestamp = @scanned, error = nullif(@error, '')
WHERE name = @name
RETURNING coalesce(status, ''), coalesce(folders, 0), coalesce(files, 0), coalesce(scan_mode, ''), coalesce(phase, ''), index_started, index_timestamp, coalesce(error, '');

-- object.put
-- An object, which replaces the one of its key, and its metadata, of a JSON
-- array of entries, which replaces the backend's metadata, but not a task's,
-- unless it changed, as of its entity tag, size or modification time, when
-- what tasks found of it, their metadata and shapes, is out of date, which
-- is removed, until they're done of it again
DELETE FROM metadata WHERE volume = @volume AND object = @key AND source <> '' AND EXISTS (
  SELECT 1 FROM object WHERE volume = @volume AND key = @key
    AND (etag IS NOT @etag OR size IS NOT @size OR modified_at IS NOT @modified_at)
);
DELETE FROM object_geo WHERE volume = @volume AND object = @key AND source <> '' AND EXISTS (
  SELECT 1 FROM object WHERE volume = @volume AND key = @key
    AND (etag IS NOT @etag OR size IS NOT @size OR modified_at IS NOT @modified_at)
);
DELETE FROM object_event WHERE volume = @volume AND object = @key AND source <> '' AND EXISTS (
  SELECT 1 FROM object WHERE volume = @volume AND key = @key
    AND (etag IS NOT @etag OR size IS NOT @size OR modified_at IS NOT @modified_at)
);
INSERT INTO object (volume, key, type, size, etag, modified_at)
VALUES (@volume, @key, nullif(@type, ''), @size, @etag, @modified_at)
ON CONFLICT (volume, key) DO UPDATE SET
  type = excluded.type, size = excluded.size, etag = excluded.etag, modified_at = excluded.modified_at;
DELETE FROM metadata WHERE volume = @volume AND object = @key AND source = '';
INSERT INTO metadata (volume, object, source, namespace, key, value)
SELECT @volume, @key, '', nullif(entry.value ->> '$.namespace', ''), entry.value ->> '$.key', coalesce(entry.value -> '$.value', 'null')
FROM json_each(@meta) AS entry
ORDER BY entry.id;

-- object.set_meta
-- The metadata of an object, of a source, a task's, of a JSON array of
-- entries, which replaces the source's
DELETE FROM metadata WHERE volume = @volume AND object = @key AND source = @source;
INSERT INTO metadata (volume, object, source, namespace, key, value)
SELECT @volume, @key, @source, nullif(entry.value ->> '$.namespace', ''), entry.value ->> '$.key', coalesce(entry.value -> '$.value', 'null')
FROM json_each(@meta) AS entry
ORDER BY entry.id;
DELETE FROM object_geo WHERE volume = @volume AND object = @key AND source = @source;
INSERT INTO object_geo (volume, object, source, shape, min_long, min_lat, max_long, max_lat)
SELECT @volume, @key, @source, json(entry.value -> '$.shape'), entry.value ->> '$.min_long', entry.value ->> '$.min_lat', entry.value ->> '$.max_long', entry.value ->> '$.max_lat'
FROM json_each(@geo) AS entry
WHERE true
ORDER BY entry.id
ON CONFLICT (volume, object, source, shape) DO NOTHING;
DELETE FROM object_event WHERE volume = @volume AND object = @key AND source = @source;
INSERT INTO object_event (volume, object, source, uid, recurrence_id, kind, title, start_date, start_at, end_date, end_at, tz)
SELECT @volume, @key, @source, entry.value ->> '$.uid', entry.value ->> '$.recurrence_id', entry.value ->> '$.kind', entry.value ->> '$.title',
  entry.value ->> '$.start_date', entry.value ->> '$.start_at', entry.value ->> '$.end_date', entry.value ->> '$.end_at', entry.value ->> '$.tz'
FROM json_each(@events) AS entry
WHERE true
ORDER BY entry.id
ON CONFLICT DO NOTHING;

-- object.get
SELECT key, coalesce(type, ''), size, etag, modified_at, (
  SELECT json_group_array(json_object('namespace', namespace, 'key', metadata.key, 'value', json(value)) ORDER BY source <> '', rowid)
  FROM metadata WHERE metadata.volume = object.volume AND metadata.object = object.key
    AND NOT (source = '' AND EXISTS (
      SELECT 1 FROM metadata AS task
      WHERE task.volume = metadata.volume AND task.object = metadata.object AND task.source <> ''
        AND ifnull(task.namespace, '') = ifnull(metadata.namespace, '') AND task.key = metadata.key
    ))
), (
  SELECT json_group_array(json(shape) ORDER BY source, rowid)
  FROM object_geo WHERE object_geo.volume = object.volume AND object_geo.object = object.key
), (
  SELECT json_group_array(json_object('uid', uid, 'recurrence_id', recurrence_id, 'kind', kind, 'title', title,
    'start', coalesce(start_date, start_at), 'end', coalesce(end_date, end_at), 'tz', tz) ORDER BY coalesce(start_at, start_date || 'T00:00:00Z'), rowid)
  FROM object_event WHERE object_event.volume = object.volume AND object_event.object = object.key
)
FROM object WHERE volume = @volume AND key = @key;

-- object.list
-- The objects of a folder, or beneath it, of the where clause, in the order
-- of their keys, after the key the listing starts after
SELECT key, coalesce(type, ''), size, etag, modified_at, (
  SELECT json_group_array(json_object('namespace', namespace, 'key', metadata.key, 'value', json(value)) ORDER BY source <> '', rowid)
  FROM metadata WHERE metadata.volume = object.volume AND metadata.object = object.key
    AND NOT (source = '' AND EXISTS (
      SELECT 1 FROM metadata AS task
      WHERE task.volume = metadata.volume AND task.object = metadata.object AND task.source <> ''
        AND ifnull(task.namespace, '') = ifnull(metadata.namespace, '') AND task.key = metadata.key
    ))
), (
  SELECT json_group_array(json(shape) ORDER BY source, rowid)
  FROM object_geo WHERE object_geo.volume = object.volume AND object_geo.object = object.key
), (
  SELECT json_group_array(json_object('uid', uid, 'recurrence_id', recurrence_id, 'kind', kind, 'title', title,
    'start', coalesce(start_date, start_at), 'end', coalesce(end_date, end_at), 'tz', tz) ORDER BY coalesce(start_at, start_date || 'T00:00:00Z'), rowid)
  FROM object_event WHERE object_event.volume = object.volume AND object_event.object = object.key
)
FROM object WHERE volume = @volume AND ${where} AND key > @after
ORDER BY key;

-- object.count
-- How many objects a listing has, of the where clause, of every page
SELECT count(*) FROM object WHERE volume = @volume AND ${where};

-- object.delete_meta
-- The metadata of a source, a task's kind, of every object of a volume, and
-- the shapes it found, and its task's work, and the revisions it was done of
DELETE FROM metadata WHERE volume = @volume AND source = @source;
DELETE FROM object_geo WHERE volume = @volume AND source = @source;
DELETE FROM object_event WHERE volume = @volume AND source = @source;
DELETE FROM object_state WHERE volume = @volume AND kind IN (@source, @did);

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
SELECT key, kind, after, attempts, coalesce(error, ''), leased_until, coalesce(revision, '') FROM object_state
WHERE volume = @volume AND kind IN (SELECT value FROM json_each(@kinds))
  AND julianday(after) <= julianday(@now)
  AND (leased_until IS NULL OR julianday(leased_until) <= julianday(@now))
ORDER BY julianday(after), key;

-- queue.lease
-- Work which is claimed, until its lease ends, and, of a task's, the revision
-- of the file's listing, which it's done of
UPDATE object_state SET leased_until = @leased,
  revision = CASE WHEN kind IN ('index', 'rescan') THEN revision ELSE (
    SELECT listed.revision FROM object_state AS listed
    WHERE listed.volume = object_state.volume AND listed.key = object_state.key AND listed.kind = 'listed'
  ) END
WHERE volume = @volume AND key = @key AND kind = @kind
RETURNING key, kind, after, attempts, coalesce(error, ''), leased_until, coalesce(revision, '');

-- queue.done
-- Work which is done, of its claim, which is kept when it's pushed again
-- since, as its lease is cleared
DELETE FROM object_state
WHERE volume = @volume AND key = @key AND kind = @kind AND leased_until = @leased
RETURNING key, kind, after, attempts, coalesce(error, ''), leased_until, coalesce(revision, '');

-- queue.did
-- The revision of the file which a task's work was done of, its listing's
-- when it was claimed, of a row of the task's kind, with _done after it,
-- while it's indexed, so a file which changed while it was done is stale,
-- and why it was given up, when it was
INSERT INTO object_state (volume, key, kind, after, revision, error, failed)
SELECT @volume, @key, @did, @now, nullif(@revision, ''), nullif(@error, ''), CASE WHEN @error <> '' THEN @now END
WHERE EXISTS (SELECT 1 FROM object WHERE volume = @volume AND key = @key)
ON CONFLICT (volume, key, kind) DO UPDATE SET revision = excluded.revision, after = excluded.after, error = excluded.error, failed = excluded.failed;

-- queue.tasks
-- The work of a task of a volume's files: how many are queued, how many it
-- was done of, and given up of, and why it last failed, of a retry, or of
-- one given up, by when it failed, as a retry's after is when it's due
SELECT
  (SELECT count(*) FROM object_state WHERE volume = @volume AND kind = @kind),
  (SELECT count(*) FROM object_state WHERE volume = @volume AND kind = @did),
  (SELECT count(*) FROM object_state WHERE volume = @volume AND kind = @did AND error IS NOT NULL),
  coalesce((
    SELECT error FROM object_state
    WHERE volume = @volume AND kind IN (@kind, @did) AND error IS NOT NULL
    ORDER BY failed DESC LIMIT 1
  ), '');

-- queue.push_stale
-- The work of a task of the files beneath a folder which are indexed, whose
-- listings' revisions aren't those the task was last done of, or which it
-- hasn't been done of, including those which have no revision, such as a
-- file which is stored of a change, until it's scanned, which isn't queued
-- again
INSERT INTO object_state (volume, key, kind, after)
SELECT object.volume, object.key, @kind, @now
FROM object
LEFT JOIN object_state AS listed ON listed.volume = object.volume AND listed.key = object.key AND listed.kind = 'listed'
LEFT JOIN object_state AS did ON did.volume = object.volume AND did.key = object.key AND did.kind = @did
WHERE object.volume = @volume AND object.key > @lower AND object.key < @upper AND object.key NOT GLOB '*/'
  AND (did.key IS NULL OR did.revision IS NOT listed.revision)
ON CONFLICT (volume, key, kind) DO NOTHING;

-- queue.failed
UPDATE object_state SET attempts = attempts + 1, error = @error, failed = @failed, after = @after, leased_until = NULL
WHERE volume = @volume AND key = @key AND kind = @kind AND leased_until = @leased
RETURNING key, kind, after, attempts, coalesce(error, ''), leased_until, coalesce(revision, '');

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
