CREATE TABLE IF NOT EXISTS notifications (
    id              TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    user_id         TEXT NOT NULL,
    template_id     TEXT NOT NULL DEFAULT '',
    channel         TEXT NOT NULL,
    priority        TEXT NOT NULL DEFAULT 'normal',
    status          TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    variables       JSONB NOT NULL DEFAULT '{}'::jsonb,
    locale          TEXT NOT NULL DEFAULT 'en',
    scheduled_at    TIMESTAMPTZ,
    sent_at         TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    opened_at       TIMESTAMPTZ,
    clicked_at      TIMESTAMPTZ,
    error           TEXT NOT NULL DEFAULT '',
    retry_count     INT NOT NULL DEFAULT 0,
    max_retries     INT NOT NULL DEFAULT 3,
    provider        TEXT NOT NULL DEFAULT '',
    provider_id     TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL DEFAULT '',
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_notifications_tenant_created ON notifications (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_status_created ON notifications (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_provider ON notifications (provider, created_at DESC);

CREATE TABLE IF NOT EXISTS templates (
    id          TEXT PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    name        TEXT NOT NULL,
    channel     TEXT NOT NULL,
    subject     TEXT NOT NULL DEFAULT '',
    body        TEXT NOT NULL,
    variables   JSONB NOT NULL DEFAULT '[]'::jsonb,
    locales     JSONB NOT NULL DEFAULT '{}'::jsonb,
    version     INT NOT NULL DEFAULT 1,
    is_active   BOOLEAN NOT NULL DEFAULT true,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_templates_tenant_name ON templates (tenant_id, name, channel);

CREATE TABLE IF NOT EXISTS user_preferences (
    user_id        TEXT NOT NULL,
    tenant_id      TEXT NOT NULL,
    channels       JSONB NOT NULL DEFAULT '{}'::jsonb,
    categories     JSONB NOT NULL DEFAULT '{}'::jsonb,
    quiet_hours    JSONB NOT NULL DEFAULT '{}'::jsonb,
    timezone       TEXT NOT NULL DEFAULT 'UTC',
    locale         TEXT NOT NULL DEFAULT 'en',
    frequency_caps JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, user_id)
);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id                TEXT PRIMARY KEY,
    tenant_id         TEXT NOT NULL,
    user_id           TEXT NOT NULL,
    notification_data JSONB NOT NULL,
    error             TEXT NOT NULL DEFAULT '',
    attempts          INT NOT NULL DEFAULT 0,
    failed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    provider          TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON dead_letter_queue (failed_at DESC);
