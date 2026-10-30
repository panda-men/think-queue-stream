CREATE TABLE IF NOT EXISTS "{{table}}" (
    scope TEXT NOT NULL,
    failure_id TEXT NOT NULL,
    sort_id TEXT NOT NULL,
    prefix TEXT NOT NULL,
    queue TEXT NOT NULL,
    job_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    error BLOB NOT NULL,
    failed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (scope, failure_id)
);
CREATE INDEX IF NOT EXISTS "{{table}}_scope_sort" ON "{{table}}" (scope, sort_id);
CREATE INDEX IF NOT EXISTS "{{table}}_scope_age" ON "{{table}}" (scope, failed_at_ms, sort_id);
