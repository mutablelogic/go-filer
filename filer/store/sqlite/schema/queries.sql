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
SELECT coalesce(status, ''), coalesce(folders, 0), index_started, index_timestamp, coalesce(error, '')
FROM volume WHERE name = @name;

-- volume.set_state
UPDATE volume SET
  status = nullif(@status, ''), folders = nullif(@folders, 0),
  index_started = @started, index_timestamp = @scanned, error = nullif(@error, '')
WHERE name = @name
RETURNING coalesce(status, ''), coalesce(folders, 0), index_started, index_timestamp, coalesce(error, '');

-- object.put
-- An object, which replaces the one at its path, and its metadata, of a
-- JSON array of entries, which replaces its metadata
INSERT INTO object (volume, path, key, parent, is_dir, type, size, etag, modified_at)
VALUES (@volume, @path, @key, @parent, @is_dir, nullif(@type, ''), @size, @etag, @modified_at)
ON CONFLICT (volume, path) DO UPDATE SET
  key = excluded.key, parent = excluded.parent, is_dir = excluded.is_dir, type = excluded.type,
  size = excluded.size, etag = excluded.etag, modified_at = excluded.modified_at;
DELETE FROM metadata WHERE volume = @volume AND path = @path;
INSERT INTO metadata (volume, path, namespace, key, value)
SELECT @volume, @path, nullif(entry.value ->> '$.namespace', ''), entry.value ->> '$.key', coalesce(entry.value -> '$.value', 'null')
FROM json_each(@meta) AS entry
ORDER BY entry.id;

-- object.get
SELECT path, is_dir, coalesce(type, ''), size, etag, modified_at, (
  SELECT json_group_array(json_object('namespace', namespace, 'key', key, 'value', json(value)) ORDER BY rowid)
  FROM metadata WHERE metadata.volume = object.volume AND metadata.path = object.path
)
FROM object WHERE volume = @volume AND path = @path;

-- object.list
-- The objects of a folder, or beneath it, of the where clause, in the order
-- of their keys, after the key the listing starts after
SELECT path, is_dir, coalesce(type, ''), size, etag, modified_at, (
  SELECT json_group_array(json_object('namespace', namespace, 'key', key, 'value', json(value)) ORDER BY rowid)
  FROM metadata WHERE metadata.volume = object.volume AND metadata.path = object.path
)
FROM object WHERE volume = @volume AND ${where} AND key > @after
ORDER BY key;

-- object.delete
-- An object, and those beneath it, of the range of their paths, and their
-- metadata, which cascades
DELETE FROM object
WHERE volume = @volume AND (path = @path OR (path > @prefix AND path < @end));

-- queue.push_index
-- The index of a path, unless a rescan of it, or a folder above it, is
-- queued, and isn't claimed, as a rescan which is claimed can have passed
-- it, which is due at the earlier of the two, so work which is pushed again
-- and again isn't put off, and is claimed again, when it's claimed
INSERT INTO object_state (volume, path, kind, after)
SELECT @volume, @path, @kind, @after
WHERE NOT EXISTS (
  SELECT 1 FROM object_state
  WHERE volume = @volume AND kind = 'rescan' AND path IN (SELECT value FROM json_each(@ancestors))
    AND (leased_until IS NULL OR julianday(leased_until) <= julianday('now'))
)
ON CONFLICT (volume, path, kind) DO UPDATE SET
  after = CASE WHEN julianday(excluded.after) < julianday(after) THEN excluded.after ELSE after END,
  leased_until = NULL;

-- queue.push_rescan
-- The rescan of a folder, which replaces the index and rescan work beneath
-- it, unless a rescan of a folder above it is queued, and isn't claimed
DELETE FROM object_state
WHERE volume = @volume AND kind IN ('index', 'rescan') AND (path > @prefix AND path < @end OR (path = @path AND kind = 'index'))
AND NOT EXISTS (
  SELECT 1 FROM object_state AS above
  WHERE above.volume = @volume AND above.kind = 'rescan' AND above.path IN (SELECT value FROM json_each(@ancestors))
    AND (above.leased_until IS NULL OR julianday(above.leased_until) <= julianday('now'))
);
INSERT INTO object_state (volume, path, kind, after)
SELECT @volume, @path, @kind, @after
WHERE NOT EXISTS (
  SELECT 1 FROM object_state
  WHERE volume = @volume AND kind = 'rescan' AND path IN (SELECT value FROM json_each(@ancestors))
    AND (leased_until IS NULL OR julianday(leased_until) <= julianday('now'))
)
ON CONFLICT (volume, path, kind) DO UPDATE SET
  after = CASE WHEN julianday(excluded.after) < julianday(after) THEN excluded.after ELSE after END,
  leased_until = NULL;

-- queue.due
-- The work of the kinds which is due, and isn't leased, earliest first
SELECT path, kind, after, attempts, coalesce(error, ''), leased_until FROM object_state
WHERE volume = @volume AND kind IN (SELECT value FROM json_each(@kinds))
  AND julianday(after) <= julianday(@now)
  AND (leased_until IS NULL OR julianday(leased_until) <= julianday(@now))
ORDER BY julianday(after), path;

-- queue.lease
UPDATE object_state SET leased_until = @leased
WHERE volume = @volume AND path = @path AND kind = @kind
RETURNING path, kind, after, attempts, coalesce(error, ''), leased_until;

-- queue.done
-- Work which is done, of its claim, which is kept when it's pushed again
-- since, as its lease is cleared
DELETE FROM object_state
WHERE volume = @volume AND path = @path AND kind = @kind AND leased_until = @leased;

-- queue.failed
UPDATE object_state SET attempts = attempts + 1, error = @error, after = @after, leased_until = NULL
WHERE volume = @volume AND path = @path AND kind = @kind AND leased_until = @leased
RETURNING path, kind, after, attempts, coalesce(error, ''), leased_until;
