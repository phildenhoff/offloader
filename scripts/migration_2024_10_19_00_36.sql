DROP TABLE IF EXISTS jobs;
CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    worker TEXT NOT NULL,
    queue TEXT NOT NULL,
    args TEXT NOT NULL,
    scheduled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    inserted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    state TEXT NOT NULL,
    attempts SMALLINT NOT NULL DEFAULT 0,
    max_attempts SMALLINT
);
