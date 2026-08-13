-- 투자의견 전역 읽음 커서 (core-api 명세 §6.1). 유저당 1행, DB 전용(Redis 미러 없음).
CREATE TABLE IF NOT EXISTS opinion_read_cursor (
    user_id       BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    last_event_id CHAR(26)    NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id)
);

-- 전역 피드의 최신순 조회·커서 이후 카운트가 테이블 누적과 무관하게 좁게 돌도록 하는 부분 인덱스
CREATE INDEX IF NOT EXISTS idx_invest_opinion_event_id
    ON invest_opinion (stream_event_id)
    WHERE stream_event_id IS NOT NULL;
