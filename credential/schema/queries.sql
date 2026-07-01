
-- credential.list
SELECT
	"key", "updated_at"
FROM
	${"schema"}."credential"
${where}
ORDER BY
	"key"
;

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
