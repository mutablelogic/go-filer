-- filer.volume_get
SELECT
	v."name", v."url", v."enabled", v."index_delta", v."created_at", v."indexed_at",
	COALESCE((
		SELECT COUNT(*)
		FROM ${"schema"}."object" AS o
		WHERE o."volume" = v."name"
	), 0)::BIGINT AS "objects",
	(
		SELECT MAX(o."indexed_at")
		FROM ${"schema"}."object" AS o
		WHERE o."volume" = v."name"
	) AS "last_indexed_object_at"
FROM
	${"schema"}."volume" AS v
WHERE
	v."name" = @name
;

-- filer.volume_list
SELECT
	v."name", v."url", v."enabled", v."index_delta", v."created_at", v."indexed_at",
	COALESCE((
		SELECT COUNT(*)
		FROM ${"schema"}."object" AS o
		WHERE o."volume" = v."name"
	), 0)::BIGINT AS "objects",
	(
		SELECT MAX(o."indexed_at")
		FROM ${"schema"}."object" AS o
		WHERE o."volume" = v."name"
	) AS "last_indexed_object_at"
FROM
	${"schema"}."volume" AS v
${where}

-- filer.volume_insert
WITH inserted AS (
	INSERT INTO ${"schema"}."volume" (
		"name", "url", "enabled", "index_delta"
	)
	VALUES (
		@name, @url, CAST(@enabled AS BOOLEAN), CAST(@index_delta AS INTERVAL)
	)
	RETURNING
		"name", "url", "enabled", "index_delta", "created_at", "indexed_at"
)
SELECT
	i."name", i."url", i."enabled", i."index_delta", i."created_at", i."indexed_at",
	COALESCE((
		SELECT COUNT(*)
		FROM ${"schema"}."object" AS o
		WHERE o."volume" = i."name"
	), 0)::BIGINT AS "objects",
	(
		SELECT MAX(o."indexed_at")
		FROM ${"schema"}."object" AS o
		WHERE o."volume" = i."name"
	) AS "last_indexed_object_at"
FROM
	inserted AS i
;

-- filer.volume_patch
WITH patched AS (
	UPDATE ${"schema"}."volume"
	SET
		${patch}
	WHERE
		"name" = @name
	RETURNING
		"name", "url", "enabled", "index_delta", "created_at", "indexed_at"
)
SELECT
	p."name", p."url", p."enabled", p."index_delta", p."created_at", p."indexed_at",
	COALESCE((
		SELECT COUNT(*)
		FROM ${"schema"}."object" AS o
		WHERE o."volume" = p."name"
	), 0)::BIGINT AS "objects",
	(
		SELECT MAX(o."indexed_at")
		FROM ${"schema"}."object" AS o
		WHERE o."volume" = p."name"
	) AS "last_indexed_object_at"
FROM
	patched AS p
;

-- filer.volume_touch
WITH touched AS (
	UPDATE ${"schema"}."volume"
	SET
		"indexed_at" = NOW()
	WHERE
		"name" = @name
	RETURNING
		"name", "url", "enabled", "index_delta", "created_at", "indexed_at"
)
SELECT
	t."name", t."url", t."enabled", t."index_delta", t."created_at", t."indexed_at",
	COALESCE((
		SELECT COUNT(*)
		FROM ${"schema"}."object" AS o
		WHERE o."volume" = t."name"
	), 0)::BIGINT AS "objects",
	(
		SELECT MAX(o."indexed_at")
		FROM ${"schema"}."object" AS o
		WHERE o."volume" = t."name"
	) AS "last_indexed_object_at"
FROM
	touched AS t
;

-- filer.volume_delete
WITH deleted AS (
	DELETE FROM ${"schema"}."volume"
	WHERE "name" = @name
	RETURNING "name", "url", "enabled", "index_delta", "created_at", "indexed_at"
)
SELECT
	d."name", d."url", d."enabled", d."index_delta", d."created_at", d."indexed_at",
	0::BIGINT AS "objects",
	NULL::TIMESTAMPTZ AS "last_indexed_object_at"
FROM
	deleted AS d
;

-- filer.object_get
SELECT
	o."volume", o."path", o."size", o."type", o."etag", o."modified_at",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', m."key", 'value', m."value") ORDER BY m."key")
		FROM ${"schema"}."meta" AS m
		WHERE m."volume" = o."volume"
		AND m."path" = o."path"
	), '[]'::jsonb) AS "meta",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', oa."etag", 'type', a."type", 'width', a."width", 'height', a."height", 'created_at', a."created_at") ORDER BY oa."etag")
		FROM ${"schema"}."object_artwork" AS oa
		JOIN ${"schema"}."artwork" AS a ON a."etag" = oa."etag"
		WHERE oa."volume" = o."volume"
		AND oa."path" = o."path"
	), '[]'::jsonb) AS "artwork"
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
	), '[]'::jsonb) AS "meta",
	'[]'::jsonb AS "artwork"
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
	), '[]'::jsonb) AS "meta",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', oa."etag", 'type', a."type", 'width', a."width", 'height', a."height", 'created_at', a."created_at") ORDER BY oa."etag")
		FROM ${"schema"}."object_artwork" AS oa
		JOIN ${"schema"}."artwork" AS a ON a."etag" = oa."etag"
		WHERE oa."volume" = p."volume"
		AND oa."path" = p."path"
	), '[]'::jsonb) AS "artwork"
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
	), '[]'::jsonb) AS "meta",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', oa."etag", 'type', a."type", 'width', a."width", 'height', a."height", 'created_at', a."created_at") ORDER BY oa."etag")
		FROM ${"schema"}."object_artwork" AS oa
		JOIN ${"schema"}."artwork" AS a ON a."etag" = oa."etag"
		WHERE oa."volume" = t."volume"
		AND oa."path" = t."path"
	), '[]'::jsonb) AS "artwork"
FROM
	touched AS t
;

-- filer.object_list
SELECT
	o."volume", o."path", o."size", o."type", o."etag", o."modified_at",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', m."key", 'value', m."value") ORDER BY m."key")
		FROM ${"schema"}."meta" AS m
		WHERE m."volume" = o."volume"
		AND m."path" = o."path"
	), '[]'::jsonb) AS "meta",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', oa."etag", 'type', a."type", 'width', a."width", 'height', a."height", 'created_at', a."created_at") ORDER BY oa."etag")
		FROM ${"schema"}."object_artwork" AS oa
		JOIN ${"schema"}."artwork" AS a ON a."etag" = oa."etag"
		WHERE oa."volume" = o."volume"
		AND oa."path" = o."path"
	), '[]'::jsonb) AS "artwork"
FROM
	${"schema"}."object" AS o
${where}
ORDER BY
	o."volume", o."path"


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
	), '[]'::jsonb) AS "meta",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', oa."etag", 'type', a."type", 'width', a."width", 'height', a."height", 'created_at', a."created_at") ORDER BY oa."etag")
		FROM ${"schema"}."object_artwork" AS oa
		JOIN ${"schema"}."artwork" AS a ON a."etag" = oa."etag"
		WHERE oa."volume" = u."volume"
		AND oa."path" = u."path"
	), '[]'::jsonb) AS "artwork"
FROM
	upserted AS u
;

-- filer.search_list
SELECT
	o."volume", o."path", o."size", o."type", o."etag", o."modified_at",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', m."key", 'value', m."value") ORDER BY m."key")
		FROM ${"schema"}."meta" AS m
		WHERE m."volume" = o."volume"
		AND m."path" = o."path"
	), '[]'::jsonb) AS "meta",
	COALESCE((
		SELECT jsonb_agg(jsonb_build_object('key', oa."etag", 'type', a."type", 'width', a."width", 'height', a."height", 'created_at', a."created_at") ORDER BY oa."etag")
		FROM ${"schema"}."object_artwork" AS oa
		JOIN ${"schema"}."artwork" AS a ON a."etag" = oa."etag"
		WHERE oa."volume" = o."volume"
		AND oa."path" = o."path"
	), '[]'::jsonb) AS "artwork",
	LEAST(ts_rank(s."tsv", websearch_to_tsquery('english', @query), 32) * 2, 1.0) AS "rank"
FROM
	${"schema"}."object" AS o
JOIN
	${"schema"}."search" AS s ON s."volume" = o."volume" AND s."path" = o."path"
${where}
ORDER BY
	"rank" DESC
	
-- filer.artwork_get
SELECT
	"etag", "data", "type", "width", "height", "created_at"
FROM
	${"schema"}."artwork"
WHERE
	"etag" = @etag
;

-- filer.artwork_get_meta
SELECT
	"etag", "type", "width", "height", "created_at"
FROM
	${"schema"}."artwork"
WHERE
	"etag" = @etag
;

-- filer.artwork_delete
DELETE FROM ${"schema"}."artwork"
WHERE
	"etag" = @etag
RETURNING
	"etag", "data", "type", "width", "height", "created_at"
;

-- filer.artwork_upsert
INSERT INTO ${"schema"}."artwork" (
	"etag", "data", "type", "width", "height"
)
VALUES (
	@etag, @data, @type, CAST(@width AS INT), CAST(@height AS INT)
)
ON CONFLICT ("etag") DO UPDATE SET "etag" = EXCLUDED."etag"
RETURNING
	"etag", "data", "type", "width", "height", "created_at"
;

-- filer.object_artwork_upsert
INSERT INTO ${"schema"}."object_artwork" (
	"volume", "path", "etag"
)
VALUES (
	@volume, @path, @etag
)
ON CONFLICT ("volume", "path", "etag") DO NOTHING
RETURNING
	"volume", "path", "etag"
;

-- filer.object_artwork_delete
DELETE FROM ${"schema"}."object_artwork"
WHERE
	"volume" = @volume
AND
	"path" = @path
AND
	"etag" = @etag
RETURNING
	"volume", "path", "etag"
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

-- credential.list
SELECT
	"key", "updated_at"
FROM
	${"schema"}."credential"
ORDER BY
	"key"

-- credential.get
SELECT
	"key", "pv", "credential"
FROM
	${"schema"}."credential"
WHERE
	"key" = @key
;

-- credential.get_pv
SELECT
	"key", "credential"
FROM
	${"schema"}."credential"
WHERE
	"key" = @key
AND
	"pv" = CAST(@pv AS INT)
;

-- credential.delete
DELETE FROM ${"schema"}."credential"
WHERE
	"key" = @key
RETURNING
	"key", "updated_at"
;

-- credential.upsert
INSERT INTO ${"schema"}."credential" (
	"key", "pv", "credential"
)
VALUES (
	@key, CAST(@pv AS INT), @credentials
)
ON CONFLICT ("key") DO UPDATE
SET
	"pv" = EXCLUDED."pv",
	"credential" = EXCLUDED."credential",
	"updated_at" = now()
RETURNING
	"key", "updated_at"
;

-- llmprovider.get
SELECT
	"name", "provider", "url", "credential", "created_at"
FROM
	${"schema"}."llmprovider"
WHERE
	"name" = @name
;

-- llmprovider.list
SELECT
	"name", "provider", "url", "credential", "created_at"
FROM
	${"schema"}."llmprovider"
ORDER BY
	"name"

-- llmprovider.insert
INSERT INTO ${"schema"}."llmprovider" (
	"name", "provider", "url", "credential"
)
VALUES (
	@name, @provider, @url, @credential
)
RETURNING
	"name", "provider", "url", "credential", "created_at"
;

-- llmprovider.patch
WITH patched AS (
	UPDATE ${"schema"}."llmprovider"
	SET
		${patch}
	WHERE
		"name" = @name
	RETURNING
		"name", "provider", "url", "credential", "created_at"
)
SELECT
	p."name", p."provider", p."url", p."credential", p."created_at"
FROM
	patched AS p
;

-- llmprovider.delete
DELETE FROM ${"schema"}."llmprovider"
WHERE
	"name" = @name
RETURNING
	"name", "provider", "url", "credential", "created_at"
;

