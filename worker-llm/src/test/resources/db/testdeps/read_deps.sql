-- worker-batch 소유 테이블(KIS 워커 명세 §4)의 테스트용 최소 대역 — worker-llm은 읽기 전용 소비자다.
-- 프로덕션 DB에는 db-migrations의 worker-batch 소유 changelog가 만든다.
CREATE TABLE stock_master (
    code        CHAR(6) PRIMARY KEY,
    name        TEXT    NOT NULL,
    market      TEXT,
    sector_code TEXT,
    is_active   BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE sector (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
