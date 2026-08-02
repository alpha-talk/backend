CREATE TABLE IF NOT EXISTS post (
    id              CHAR(26)    PRIMARY KEY,
    code            CHAR(6)     NOT NULL,
    author_id       BIGINT      NOT NULL REFERENCES users (id),
    title           TEXT        NOT NULL,
    content         TEXT        NOT NULL,
    quoted_event_id CHAR(26)    NULL,
    like_count      INTEGER     NOT NULL DEFAULT 0,
    comment_count   INTEGER     NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NULL,
    deleted_at      TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_post_code_id ON post (code, id DESC);

CREATE TABLE IF NOT EXISTS comment (
    id         CHAR(26)    PRIMARY KEY,
    post_id    CHAR(26)    NOT NULL REFERENCES post (id),
    author_id  BIGINT      NOT NULL REFERENCES users (id),
    content    TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_comment_post_id ON comment (post_id, id);

CREATE TABLE IF NOT EXISTS post_like (
    post_id    CHAR(26)    NOT NULL REFERENCES post (id),
    user_id    BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (post_id, user_id)
);

CREATE TABLE IF NOT EXISTS report (
    id          CHAR(26)    PRIMARY KEY,
    target_type TEXT        NOT NULL,
    target_id   CHAR(26)    NOT NULL,
    reporter_id BIGINT      NOT NULL REFERENCES users (id),
    reason      TEXT        NOT NULL,
    detail      TEXT        NULL,
    status      TEXT        NOT NULL DEFAULT 'RECEIVED',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_report_target ON report (target_type, target_id);
