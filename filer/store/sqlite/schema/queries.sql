-- volume.put
-- A volume, or its url and enabled, which are only modified when they do
INSERT INTO volume (name, url, enabled, created_at)
VALUES (@name, @url, @enabled, @now)
ON CONFLICT (name) DO UPDATE SET
  url = excluded.url, enabled = excluded.enabled, modified_at = excluded.created_at
WHERE url IS NOT excluded.url OR enabled IS NOT excluded.enabled;

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
