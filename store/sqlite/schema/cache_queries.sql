-- cache.create
-- An entry, without a key, which its content is written to
INSERT INTO cache_entry (stored) VALUES (@now) RETURNING id;

-- cache.write
-- A chunk of an entry's content
INSERT INTO cache_chunk (entry, n, data) VALUES (@entry, @n, @data);

-- cache.release
-- The entry of a key, which loses its key, so it isn't found, and is
-- deleted, once it isn't read
UPDATE cache_entry SET key = NULL WHERE key = @key RETURNING id;

-- cache.release_prefix
-- The entries of the keys which start with the prefix
UPDATE cache_entry SET key = NULL WHERE key IS NOT NULL AND substr(key, 1, length(@prefix)) = @prefix RETURNING id;

-- cache.commit
-- An entry, which is written, of its key, and what it is
UPDATE cache_entry SET key = @key, version = @version, meta = @meta, size = @size, stored = @now, expires = @expires
WHERE id = @id
RETURNING id, key, version, meta, size, stored, expires;

-- cache.get
SELECT id, key, version, meta, size, stored, expires FROM cache_entry WHERE key = @key;

-- cache.read
-- A chunk of an entry's content
SELECT data FROM cache_chunk WHERE entry = @entry AND n = @n;

-- cache.delete
-- An entry, and its content, which cascades
DELETE FROM cache_entry WHERE id = @id RETURNING id;

-- cache.expired
-- The entries whose TTLs have passed, which are released
UPDATE cache_entry SET key = NULL WHERE key IS NOT NULL AND expires IS NOT NULL AND expires <= @now RETURNING id;
