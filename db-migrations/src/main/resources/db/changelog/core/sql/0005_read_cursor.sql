CREATE TABLE IF NOT EXISTS read_cursor (
    user_id       BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    code          CHAR(6)     NOT NULL,
    last_event_id CHAR(26)    NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, code)
);
