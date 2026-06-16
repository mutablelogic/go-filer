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

-- filer.metadata_get
SELECT
	"key", "etag", "filename", "size", "modified_at", "title", "media_type", "summary", "tags", "indexed_at"
FROM
	${"schema"}."metadata"
WHERE
	"key" = @key
;

-- filer.metadata_delete
DELETE FROM ${"schema"}."metadata"
WHERE
	"key" = @key
RETURNING
	"key", "etag", "filename", "size", "modified_at", "title", "media_type", "summary", "tags", "indexed_at"
;


-- filer.metadata_list
SELECT
	m."key",
	m."etag",
	m."filename",
	m."size",
	m."modified_at",
	m."title",
	m."media_type",
	m."summary",
	m."tags",
	m."indexed_at",
	COALESCE(
		jsonb_agg(
			jsonb_build_object(
				'key', kv."key",
				'value', kv."value"
			)
			ORDER BY kv."key"
		) FILTER (WHERE kv."key" IS NOT NULL),
		'[]'::jsonb
	) AS "metadata"
FROM
	${"schema"}."metadata" m
LEFT JOIN
	${"schema"}."metadata_kv" kv ON kv."metadata" = m."key"
${where}
GROUP BY
	m."key",
	m."etag",
	m."filename",
	m."size",
	m."modified_at",
	m."title",
	m."media_type",
	m."summary",
	m."tags",
	m."indexed_at"
ORDER BY
	m."indexed_at" DESC,
	m."key"


-- filer.metadata_query
SELECT
	m."key",
	m."etag",
	m."filename",
	m."size",
	m."modified_at",
	m."title",
	m."media_type",
	m."summary",
	m."tags",
	m."indexed_at",
	COALESCE(
		jsonb_agg(
			jsonb_build_object(
				'key', kv."key",
				'value', kv."value"
			)
			ORDER BY kv."key"
		) FILTER (WHERE kv."key" IS NOT NULL),
		'[]'::jsonb
	) AS "metadata"
FROM
	${"schema"}."metadata" m
LEFT JOIN
	${"schema"}."metadata_kv" kv ON kv."metadata" = m."key"
${where}
GROUP BY
	m."key",
	m."etag",
	m."filename",
	m."size",
	m."modified_at",
	m."title",
	m."media_type",
	m."summary",
	m."tags",
	m."indexed_at"
ORDER BY
	ts_rank_cd(m."tsv", websearch_to_tsquery('simple', @query)) DESC,
	m."indexed_at" DESC,
	m."key"

    
-- filer.metadata_insert
INSERT INTO ${"schema"}."metadata" (
	"key", "etag", "filename", "size", "modified_at", "title", "media_type", "summary", "tags"
)
VALUES (
	@key, @etag, @filename, @size, CAST(@modified_at AS TIMESTAMPTZ), @title, @media_type, @summary, CAST(@tags AS TEXT[])
)
ON CONFLICT ("key") DO UPDATE
SET
	"etag" = EXCLUDED."etag",
	"filename" = EXCLUDED."filename",
	"size" = EXCLUDED."size",
	"modified_at" = EXCLUDED."modified_at",
	"title" = EXCLUDED."title",
	"media_type" = EXCLUDED."media_type",
	"summary" = EXCLUDED."summary",
	"tags" = EXCLUDED."tags",
	"indexed_at" = now()
RETURNING
	"key", "etag", "filename", "size", "modified_at", "title", "media_type", "summary", "tags", "indexed_at"
;

-- filer.metadata_kv_insert
INSERT INTO ${"schema"}."metadata_kv" (
	"metadata", "key", "value"
)
VALUES (
	@metadata, @key, CAST(@value AS JSONB)
)
ON CONFLICT ("metadata", "key") DO UPDATE
SET
	"value" = EXCLUDED."value"
RETURNING
	"metadata", "key", "value"
;
