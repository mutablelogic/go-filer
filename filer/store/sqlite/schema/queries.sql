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
