-- 커서 없는 유저의 최근 24시간 카운트(core-api 명세 §6.1)가 이력 전체를 훑지 않도록 하는 부분 인덱스
CREATE INDEX IF NOT EXISTS idx_invest_opinion_bound_collected
    ON invest_opinion (collected_at)
    WHERE stream_event_id IS NOT NULL;
