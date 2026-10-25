CREATE TABLE IF NOT EXISTS jobicy_jobs (
    id bigint PRIMARY KEY,
    url text NOT NULL,
    title text NOT NULL,
    company text NOT NULL,
    location text NOT NULL,
    published_at timestamptz,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'unknown' CHECK (status IN ('active', 'closed', 'unknown')),
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    status_checked_at timestamptz
);
CREATE INDEX IF NOT EXISTS jobicy_jobs_published_at_idx ON jobicy_jobs (published_at DESC);
CREATE TABLE IF NOT EXISTS jobicy_sync_state (
    key text PRIMARY KEY,
    completed_at timestamptz NOT NULL
);
