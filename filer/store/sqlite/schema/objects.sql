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
