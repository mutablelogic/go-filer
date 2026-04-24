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

-- filer.metadata_insert
INSERT INTO ${"schema"}."metadata" (
	"name", "path", "title", "summary", "text", "tags"
)
VALUES (
	@name, @path, @title, @summary, @text, CAST(@tags AS TEXT[])
)
ON CONFLICT ("name", "path") DO UPDATE
SET
	"title" = EXCLUDED."title",
	"summary" = EXCLUDED."summary",
	"text" = EXCLUDED."text",
	"tags" = EXCLUDED."tags"
RETURNING
	"name", "path", "title", "summary", "text", "tags", "created_at"
;
