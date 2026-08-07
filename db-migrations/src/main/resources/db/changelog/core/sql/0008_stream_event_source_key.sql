-- 외부 생산자(worker-batch 투자의견) 멱등 키. 논리 소유자는 core-api stream 모듈(뉴스 워커 명세 §5, KIS 워커 명세 §3.3).
ALTER TABLE stream_event ADD COLUMN IF NOT EXISTS source_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS uq_stream_event_source_key
    ON stream_event (source_key)
    WHERE source_key IS NOT NULL;
