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

-- filer.object_get
SELECT
	o."volume", o."path", o."size", o."type", o."etag", o."modified_at",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', m."key", 'value', m."value") ORDER BY m."key")
		FROM ${"schema"}."meta" AS m
		WHERE m."volume" = o."volume"
		AND m."path" = o."path"
	), '[]'::jsonb) AS "meta"
FROM
	${"schema"}."object" AS o
WHERE
	o."volume" = @volume
AND
	o."path" = @path
;

-- filer.object_delete
WITH deleted AS (
	DELETE FROM ${"schema"}."object"
	WHERE
		"volume" = @volume
	AND
		"path" = @path
	RETURNING
		"volume", "path", "size", "type", "etag", "modified_at"
)
SELECT
	d."volume", d."path", d."size", d."type", d."etag", d."modified_at",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', m."key", 'value', m."value") ORDER BY m."key")
		FROM ${"schema"}."meta" AS m
		WHERE m."volume" = d."volume"
		AND m."path" = d."path"
	), '[]'::jsonb) AS "meta"
FROM
	deleted AS d
;

-- filer.object_patch
WITH patched AS (
	UPDATE ${"schema"}."object"
	SET
		${patch}
	WHERE
		"volume" = @volume
	AND
		"path" = @path
	RETURNING
		"volume", "path", "size", "type", "etag", "modified_at"
)
SELECT
	p."volume", p."path", p."size", p."type", p."etag", p."modified_at",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', m."key", 'value', m."value") ORDER BY m."key")
		FROM ${"schema"}."meta" AS m
		WHERE m."volume" = p."volume"
		AND m."path" = p."path"
	), '[]'::jsonb) AS "meta"
FROM
	patched AS p
;

-- filer.object_touch
WITH touched AS (
	UPDATE ${"schema"}."object"
	SET
		"indexed_at" = NOW()
	WHERE
		"volume" = @volume
	AND
		"path" = @path
	RETURNING
		"volume", "path", "size", "type", "etag", "modified_at"
)
SELECT
	t."volume", t."path", t."size", t."type", t."etag", t."modified_at",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', m."key", 'value', m."value") ORDER BY m."key")
		FROM ${"schema"}."meta" AS m
		WHERE m."volume" = t."volume"
		AND m."path" = t."path"
	), '[]'::jsonb) AS "meta"
FROM
	touched AS t
;

-- filer.object_upsert
WITH upserted AS (
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
)
SELECT
	u."volume", u."path", u."size", u."type", u."etag", u."modified_at",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', m."key", 'value', m."value") ORDER BY m."key")
		FROM ${"schema"}."meta" AS m
		WHERE m."volume" = u."volume"
		AND m."path" = u."path"
	), '[]'::jsonb) AS "meta"
FROM
	upserted AS u
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
