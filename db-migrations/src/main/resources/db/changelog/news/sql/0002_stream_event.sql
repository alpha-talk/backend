-- stream_event: core-api stream 모듈과 공유(core-api 명세 §11). worker-llm은 INSERT/UPDATE만.
-- 논리적 소유자는 core-api — core-api 착수 시 changelog 상 위치만 정리한다(뉴스 워커 명세 §5).
CREATE TABLE IF NOT EXISTS stream_event (
    event_id    CHAR(26)    PRIMARY KEY,
    code        CHAR(6)     NOT NULL,
    type        TEXT        NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    source      TEXT,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stream_event_code_event ON stream_event (code, event_id DESC);
CREATE INDEX IF NOT EXISTS idx_stream_event_code_type_event ON stream_event (code, type, event_id DESC);
