-- 로컬 E2E 테스트용 시드 — 운영에선 worker-batch가 스키마 적재·마스터 동기화를 담당한다(KIS 워커 명세 §4).
-- stock_master·sector는 worker-batch 소유 테이블이라 db-migrations에 아직 없으므로 여기서 임시 생성한다
-- (worker-llm 테스트의 db/testdeps/read_deps.sql과 같은 최소 대역).
-- 적용: docker exec -i alphatalk-postgres psql -U alphatalk alphatalk < test-front/seed-stocks.sql

CREATE TABLE IF NOT EXISTS stock_master (
    code        CHAR(6) PRIMARY KEY,
    name        TEXT    NOT NULL,
    market      TEXT,
    sector_code TEXT,
    is_active   BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS sector (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO sector (code, name) VALUES
    ('ELEC',  '전기·전자'),
    ('AUTO',  '자동차'),
    ('CHEM',  '화학·2차전지'),
    ('IT',    '인터넷·플랫폼'),
    ('STEEL', '철강'),
    ('FIN',   '금융')
ON CONFLICT (code) DO NOTHING;

INSERT INTO stock_master (code, name, market, sector_code, is_active) VALUES
    ('005930', '삼성전자',       'KOSPI', 'ELEC',  true),
    ('000660', 'SK하이닉스',     'KOSPI', 'ELEC',  true),
    ('373220', 'LG에너지솔루션', 'KOSPI', 'CHEM',  true),
    ('005380', '현대차',         'KOSPI', 'AUTO',  true),
    ('000270', '기아',           'KOSPI', 'AUTO',  true),
    ('035420', 'NAVER',          'KOSPI', 'IT',    true),
    ('035720', '카카오',         'KOSPI', 'IT',    true),
    ('051910', 'LG화학',         'KOSPI', 'CHEM',  true),
    ('005490', 'POSCO홀딩스',    'KOSPI', 'STEEL', true),
    ('105560', 'KB금융',         'KOSPI', 'FIN',   true)
ON CONFLICT (code) DO NOTHING;
