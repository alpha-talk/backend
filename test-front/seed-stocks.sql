-- 로컬 E2E 테스트용 시드. stock_master·sector 스키마는 db-migrations가 소유하고(KIS 워커 명세 §4),
-- 운영 데이터는 worker-batch의 stock_master_sync가 KIS 마스터 파일에서 적재한다(§3.1).
-- 이 파일은 워커를 돌리지 않고 화면을 확인할 때 쓰는 최소 표본이다.
-- 적용: docker exec -i alphatalk-postgres psql -U alphatalk alphatalk < test-front/seed-stocks.sql

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
