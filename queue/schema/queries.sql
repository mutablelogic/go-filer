
-- pgqueue.get
SELECT
    "queue",
    "ttl",
    "retries",
    "retry_delay",
    "concurrency"
FROM
    ${"schema"}."queue"
WHERE
    "queue" = @id;

-- pgqueue.insert
INSERT INTO ${"schema"}."queue" (
    "queue"
) VALUES (
    @queue
)
RETURNING
    "queue",
    "ttl",
    "retries",
    "retry_delay",
    "concurrency";

-- pgqueue.patch
UPDATE
    ${"schema"}."queue"
SET
    ${patch}
WHERE
    "queue" = @id
RETURNING
    "queue",
    "ttl",
    "retries",
    "retry_delay",
    "concurrency";

-- pgqueue.delete
DELETE FROM
    ${"schema"}."queue"
WHERE
    "queue" = @id
RETURNING
    "queue",
    "ttl",
    "retries",
    "retry_delay",
    "concurrency";

-- pgqueue.list
SELECT
    "queue",
    "ttl",
    "retries",
    "retry_delay",
    "concurrency"
FROM
    ${"schema"}."queue"
ORDER BY
    "queue"

-- pgqueue.clean
SELECT
    "id",
    "queue",
    "payload",
    "result",
    "worker",
    "created_at",
    "delayed_at",
    "started_at",
    "finished_at",
    "dies_at",
    "retries"
FROM
    ${"schema"}.queue_clean(@id);

-- pgqueue.task_get
SELECT
    "id",
    "queue",
    "payload",
    "result",
    "worker",
    "created_at",
    "delayed_at",
    "started_at",
    "finished_at",
    "dies_at",
    "retries"
FROM
    ${"schema"}."task"
WHERE
    "id" = @tid;

-- pgqueue.task_insert
SELECT
    ${"schema"}.queue_insert(
        @queue,
        CAST(@payload AS JSONB),
        CAST(@delayed_at AS TIMESTAMPTZ)
    ) AS "id";

-- pgqueue.task_patch
UPDATE
    ${"schema"}."task"
SET
    ${patch}
WHERE
    "id" = @tid
RETURNING
    "id",
    "queue",
    "payload",
    "result",
    "worker",
    "created_at",
    "delayed_at",
    "started_at",
    "finished_at",
    "dies_at",
    "retries";

-- pgqueue.task_list
SELECT
    sq."id",
    sq."queue",
    sq."payload",
    sq."result",
    sq."worker",
    sq."created_at",
    sq."delayed_at",
    sq."started_at",
    sq."finished_at",
    sq."dies_at",
    sq."retries",
    sq."status"
FROM (
    SELECT
        t."id",
        t."queue",
        t."payload",
        t."result",
        t."worker",
        t."created_at",
        t."delayed_at",
        t."started_at",
        t."finished_at",
        t."dies_at",
        t."retries",
        ${"schema"}.queue_task_status(t."id") AS "status"
    FROM
        ${"schema"}."task" t
    ${taskwhere}
) sq
${where}
ORDER BY
    sq."created_at",
    sq."id"

-- pgqueue.retain
SELECT
    ${"schema"}.queue_lock(@queues, @worker) AS "id";

-- pgqueue.release
SELECT
    ${"schema"}.queue_unlock(@tid, CAST(@result AS JSONB)) AS "id";

-- pgqueue.fail
SELECT
    ${"schema"}.queue_fail(@tid, CAST(@result AS JSONB)) AS "id";

-- pgqueue.ticker_get
SELECT
    "ticker",
    "payload",
    "interval",
    "last_at"
FROM
    ${"schema"}."ticker"
WHERE
    "ticker" = @id;

-- pgqueue.ticker_insert
INSERT INTO ${"schema"}."ticker" (
    "ticker"
) VALUES (
    @ticker
)
RETURNING
    "ticker",
    "payload",
    "interval",
    "last_at";

-- pgqueue.ticker_patch
UPDATE
    ${"schema"}."ticker"
SET
    ${patch}
WHERE
    "ticker" = @id
RETURNING
    "ticker",
    "payload",
    "interval",
    "last_at";

-- pgqueue.ticker_delete
DELETE FROM
    ${"schema"}."ticker"
WHERE
    "ticker" = @id
RETURNING
    "ticker",
    "payload",
    "interval",
    "last_at";

-- pgqueue.ticker_list
SELECT
    "ticker",
    "payload",
    "interval",
    "last_at"
FROM
    ${"schema"}."ticker"
ORDER BY
    "ticker"

-- pgqueue.ticker_next
SELECT
    "ticker",
    "payload",
    "interval",
    "last_at"
FROM
    ${"schema"}.ticker_next();

-- pgqueue.stats
SELECT
    q."queue",
    q."status",
    q."count"
FROM
    ${"schema"}."queue_status" q
WHERE
    TRUE ${where}
ORDER BY
    q."queue",
    q."status";

-- pgqueue.partition
CREATE TABLE IF NOT EXISTS ${"schema"}."task${serial}" PARTITION OF ${"schema"}."task"
    FOR VALUES FROM (${start}) TO (${end});
