-- filer.object_get
SELECT
	"volume",	"path",	"size",	"modified_at",	"type",	"etag",	"meta"
FROM
	${"schema"}."object"
WHERE
	"volume" = @volume
AND
	"path" = @path
;

-- filer.object_delete
DELETE FROM ${"schema"}."object"
WHERE
	"volume" = @volume
AND
	"path" = @path
RETURNING
	"volume",	"path",	"size",	"modified_at",	"type",	"etag",	"meta"
;

-- filer.object_insert
INSERT INTO ${"schema"}."object" (
	"volume",	"path",	"size",	"modified_at",	"type",	"etag",	"meta") 
VALUES (
	@volume,	@path,	@size,	CAST(@modified_at AS TIMESTAMPTZ),	@type,	@etag,	CAST(@meta AS JSONB)
)
ON CONFLICT ("volume", "path") DO UPDATE
SET
	"size" = EXCLUDED."size",
	"modified_at" = EXCLUDED."modified_at",
	"type" = EXCLUDED."type",
	"etag" = EXCLUDED."etag",
	"meta" = EXCLUDED."meta",
	"indexed_at" = now()
RETURNING
	"volume",	"path",	"size",	"modified_at",	"type",	"etag",	"meta"
;

-- filer.metadata_insert
INSERT INTO ${"schema"}."metadata" (
	"volume", "path", "title", "summary", "text", "tags"
)
VALUES (
	@volume, @path, @title, @summary, @text, CAST(@tags AS TEXT[])
)
ON CONFLICT ("volume", "path") DO UPDATE
SET
	"title" = EXCLUDED."title",
	"summary" = EXCLUDED."summary",
	"text" = EXCLUDED."text",
	"tags" = EXCLUDED."tags"
RETURNING
	"volume", "path", "title", "summary", "text", "tags", "created_at"
;
