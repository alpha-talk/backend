CREATE TABLE IF NOT EXISTS idempotency_record (
    user_id    BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    idem_key   CHAR(26)    NOT NULL,
    action     TEXT        NOT NULL,
    response   JSONB       NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, idem_key)
);
