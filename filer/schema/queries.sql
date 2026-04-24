-- filer.object_get
SELECT
	"name",	"path",	"size",	"modified_at",	"type",	"etag",	"meta"
FROM
	${"schema"}."object"
WHERE
	"name" = @name
AND
	"path" = @path
;

-- filer.object_delete
DELETE FROM ${"schema"}."object"
WHERE
	"name" = @name
AND
	"path" = @path
RETURNING
	"name",	"path",	"size",	"modified_at",	"type",	"etag",	"meta"
;

-- filer.object_insert
INSERT INTO ${"schema"}."object" (
	"name",	"path",	"size",	"modified_at",	"type",	"etag",	"meta") 
VALUES (
	@name,	@path,	@size,	CAST(@modified_at AS TIMESTAMPTZ),	@type,	@etag,	CAST(@meta AS JSONB)
)
ON CONFLICT ("name", "path") DO UPDATE
SET
	"size" = EXCLUDED."size",
	"modified_at" = EXCLUDED."modified_at",
	"type" = EXCLUDED."type",
	"etag" = EXCLUDED."etag",
	"meta" = EXCLUDED."meta",
	"indexed_at" = now()
RETURNING
	"name",	"path",	"size",	"modified_at",	"type",	"etag",	"meta"
;
