
-- pgqueue.partition
CREATE TABLE IF NOT EXISTS ${"schema"}."task${serial}" PARTITION OF ${"schema"}."task"
    FOR VALUES FROM (${start}) TO (${end});
