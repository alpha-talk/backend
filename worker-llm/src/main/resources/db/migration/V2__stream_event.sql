-- stream_event: core-api stream 모듈과 공유(core-api 명세 §11). worker-llm은 INSERT/UPDATE만.
-- core-api 소스가 별도 브랜치인 현 상태에서 뉴스 워커 단독 기동을 위해 IF NOT EXISTS로 관리하고,
-- 브랜치 병합 시 Flyway 소유를 core-api와 정합화한다.
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
