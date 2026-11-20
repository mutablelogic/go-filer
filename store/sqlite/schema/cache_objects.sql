-- cache.entry
-- The entries of a volume's cache, by their keys, which are NULL while
-- they're written, and once they're replaced, or deleted, until they're no
-- longer read, of their versions, and values, as JSON, and their sizes, and
-- when they were stored, and expire, as Unix nanoseconds, or NULL, without
-- a TTL
CREATE TABLE IF NOT EXISTS cache_entry (
  id       INTEGER PRIMARY KEY,
  key      TEXT,
  version  TEXT NOT NULL DEFAULT '',
  meta     TEXT NOT NULL DEFAULT '{}',
  size     INTEGER NOT NULL DEFAULT 0,
  stored   INTEGER NOT NULL,
  expires  INTEGER
);

-- cache.entry_key
-- An entry of each key
CREATE UNIQUE INDEX IF NOT EXISTS cache_entry_key ON cache_entry (key) WHERE key IS NOT NULL;

-- cache.chunk
-- The content of the entries, in chunks, of their indexes, from zero
CREATE TABLE IF NOT EXISTS cache_chunk (
  entry  INTEGER NOT NULL REFERENCES cache_entry (id) ON DELETE CASCADE,
  n      INTEGER NOT NULL,
  data   BLOB NOT NULL,
  PRIMARY KEY (entry, n)
) WITHOUT ROWID;
