-- filer.volume_get
SELECT
	"name", "url", "enabled", "index_delta", "created_at", "indexed_at"
FROM
	${"schema"}."volume"
WHERE
	"name" = @name
;

-- filer.volume_list
SELECT
	"name", "url", "enabled", "index_delta", "created_at", "indexed_at"
FROM
	${"schema"}."volume"
${where}

-- filer.volume_insert
INSERT INTO ${"schema"}."volume" (
	"name", "url", "enabled", "index_delta"
)
VALUES (
	@name, @url, CAST(@enabled AS BOOLEAN), CAST(@index_delta AS INTERVAL)
)
RETURNING
	"name", "url", "enabled", "index_delta", "created_at", "indexed_at"
;

-- filer.volume_patch
UPDATE ${"schema"}."volume"
SET
	${patch}
WHERE
	"name" = @name
RETURNING
	"name", "url", "enabled", "index_delta", "created_at", "indexed_at"
;

-- filer.volume_touch
UPDATE ${"schema"}."volume"
SET
	"indexed_at" = NOW()
WHERE
	"name" = @name
RETURNING
	"name", "url", "enabled", "index_delta", "created_at", "indexed_at"
;

-- filer.object_upsert
INSERT INTO ${"schema"}."object" (
	"volume", "path", "size", "type", "etag", "modified_at"
)
VALUES (
	@volume, @path, @size, @type, @etag, @modified_at
)
ON CONFLICT ("volume", "path") DO UPDATE
SET
	"size" = EXCLUDED."size",
	"type" = EXCLUDED."type",
	"etag" = EXCLUDED."etag",
	"modified_at" = EXCLUDED."modified_at"
RETURNING
	"volume", "path", "size", "type", "etag", "modified_at"
;

-- filer.meta_upsert
INSERT INTO ${"schema"}."meta" (
	"volume", "path", "key", "value"
)
VALUES (
	@volume, @path, @key, CAST(@value AS JSONB)
)
ON CONFLICT ("volume", "path", "key") DO UPDATE
SET
	"value" = EXCLUDED."value"
RETURNING
	"volume", "path", "key", "value"
;
