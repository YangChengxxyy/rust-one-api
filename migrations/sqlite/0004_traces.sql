-- Phase 2: request trace persistence (axonhub requests + request_executions).

CREATE TABLE IF NOT EXISTS requests (
    id          TEXT PRIMARY KEY,
    api_key_id  TEXT,
    channel_id  TEXT,
    model       TEXT NOT NULL,
    stream      INTEGER NOT NULL DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'success',
    error       TEXT,
    ttft_ms     INTEGER,
    latency_ms  INTEGER NOT NULL DEFAULT 0,
    usage       TEXT NOT NULL DEFAULT '{}',
    cost        TEXT NOT NULL DEFAULT '0',
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_requests_created ON requests (created_at);
CREATE INDEX IF NOT EXISTS idx_requests_key_created ON requests (api_key_id, created_at);
CREATE INDEX IF NOT EXISTS idx_requests_channel_created ON requests (channel_id, created_at);
CREATE INDEX IF NOT EXISTS idx_requests_status_created ON requests (status, created_at);
CREATE INDEX IF NOT EXISTS idx_requests_model_created ON requests (model, created_at);

CREATE TABLE IF NOT EXISTS request_executions (
    id               TEXT PRIMARY KEY,
    request_id       TEXT NOT NULL REFERENCES requests (id) ON DELETE CASCADE,
    attempt          INTEGER NOT NULL DEFAULT 1,
    channel_id       TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'success',
    error            TEXT,
    latency_ms       INTEGER NOT NULL DEFAULT 0,
    request_headers  TEXT,
    request_body     TEXT,
    response_headers TEXT,
    response_body    TEXT,
    created_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_request_executions_request ON request_executions (request_id, attempt);
