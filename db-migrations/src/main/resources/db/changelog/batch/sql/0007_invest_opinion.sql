-- 증권사 투자의견 observation (KIS 워커 명세 §3.3·§4). immutable — published_at·stream_event_id만 갱신한다.
CREATE TABLE IF NOT EXISTS invest_opinion (
    code            CHAR(6)     NOT NULL,
    business_date   CHAR(8)     NOT NULL,
    broker_code     TEXT        NOT NULL,
    broker_name     TEXT,
    rating          TEXT        NOT NULL,
    previous_rating TEXT,
    target_price    BIGINT,
    content_hash    CHAR(64)    NOT NULL,
    collected_at    TIMESTAMPTZ NOT NULL,
    stream_event_id CHAR(26),
    published_at    TIMESTAMPTZ,
    PRIMARY KEY (code, business_date, broker_code, content_hash)
);

-- 미통보 스캔(§3.3 ③)이 테이블 누적과 무관하게 좁게 돌도록 하는 부분 인덱스
CREATE INDEX IF NOT EXISTS idx_invest_opinion_unpublished
    ON invest_opinion (collected_at)
    WHERE published_at IS NULL;
