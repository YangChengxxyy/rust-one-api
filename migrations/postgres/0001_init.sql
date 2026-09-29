CREATE TABLE IF NOT EXISTS channels (
    id               TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    channel_type     TEXT NOT NULL,
    base_url         TEXT NOT NULL,
    credentials      TEXT NOT NULL DEFAULT '{}',
    supported_models TEXT NOT NULL DEFAULT '[]',
    model_mapping    TEXT NOT NULL DEFAULT '{}',
    weight           INTEGER NOT NULL DEFAULT 1,
    status           TEXT NOT NULL DEFAULT 'enabled',
    settings         TEXT NOT NULL DEFAULT '{}',
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS api_keys (
    id         TEXT PRIMARY KEY,
    key        TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL DEFAULT '',
    status     TEXT NOT NULL DEFAULT 'enabled',
    quota      TEXT NOT NULL DEFAULT '{}',
    expired_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS model_prices (
    id           TEXT PRIMARY KEY,
    channel_id   TEXT,
    model        TEXT NOT NULL,
    price        TEXT NOT NULL,
    reference_id TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_model_prices_channel_model
    ON model_prices (COALESCE(channel_id, ''), model);

CREATE TABLE IF NOT EXISTS usage_logs (
    id                TEXT PRIMARY KEY,
    request_id        TEXT NOT NULL,
    api_key_id        TEXT,
    channel_id        TEXT,
    model             TEXT NOT NULL,
    stream            BOOLEAN NOT NULL DEFAULT FALSE,
    prompt_tokens     BIGINT NOT NULL DEFAULT 0,
    completion_tokens BIGINT NOT NULL DEFAULT 0,
    cached_tokens     BIGINT NOT NULL DEFAULT 0,
    reasoning_tokens  BIGINT NOT NULL DEFAULT 0,
    total_tokens      BIGINT NOT NULL DEFAULT 0,
    cost              TEXT NOT NULL DEFAULT '0',
    cost_items        TEXT NOT NULL DEFAULT '[]',
    status            TEXT NOT NULL DEFAULT 'success',
    latency_ms        BIGINT NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usage_logs_created ON usage_logs (created_at);
CREATE INDEX IF NOT EXISTS idx_usage_logs_key_created ON usage_logs (api_key_id, created_at);

CREATE TABLE IF NOT EXISTS provider_quota_status (
    id            TEXT PRIMARY KEY,
    channel_id    TEXT NOT NULL,
    provider_type TEXT NOT NULL,
    account_key   TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT 'unknown',
    quota_data    TEXT NOT NULL DEFAULT '{}',
    next_check_at TEXT,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    UNIQUE (channel_id, provider_type, account_key)
);
