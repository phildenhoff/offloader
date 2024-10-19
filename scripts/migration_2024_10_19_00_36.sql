DROP TABLE IF EXISTS jobs;
CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    worker TEXT NOT NULL,
    queue TEXT NOT NULL,
    args JSONB NOT NULL,
    scheduled_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    state TEXT NOT NULL,
    attempts SMALLINT NOT NULL DEFAULT 0,
    max_attempts SMALLINT
);
