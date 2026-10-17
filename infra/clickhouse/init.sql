-- ClickHouse 초기 테이블
-- Kafka 엔진 테이블이 토픽을 구독 → 구체화 뷰가 저장 테이블로 옮김.
-- 재시작 시 같은 결과가 다시 들어올 수 있어 ReplacingMergeTree로 (분, 키) 기준 덮어쓴다 (설계 원칙 9).

CREATE DATABASE IF NOT EXISTS databrain;

-- ---------- 메인 경로 집계 (장바구니·조회, 샘플링 추정치)
CREATE TABLE IF NOT EXISTS databrain.agg_minute_q (
    key String, minute_ms UInt64, est_count Float64, raw_count UInt64, max_fresh_ms Int64
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'agg.minute',
  kafka_group_name = 'ch-agg-minute', kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS databrain.agg_minute (
    event_type LowCardinality(String), category_code String,
    minute DateTime, est_count Float64, raw_count UInt64, max_fresh_ms Int64,
    status LowCardinality(String) DEFAULT 'estimate',     -- 'estimate' → 밤 재계산 후 'confirmed'
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (event_type, category_code, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS databrain.agg_minute_mv TO databrain.agg_minute AS
SELECT splitByChar('|', key)[1] AS event_type, splitByChar('|', key)[2] AS category_code,
       toDateTime(intDiv(minute_ms, 1000)) AS minute, est_count, raw_count, max_fresh_ms
FROM databrain.agg_minute_q;

-- ---------- 구매 전용 경로 (항상 확정값)
CREATE TABLE IF NOT EXISTS databrain.purchase_minute_q (
    key String, minute_ms UInt64, orders UInt64, revenue Float64, max_fresh_ms Int64
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'agg.purchase',
  kafka_group_name = 'ch-agg-purchase', kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS databrain.purchase_minute (
    brand String, category_code String, minute DateTime, orders UInt64, revenue Float64,
    max_fresh_ms Int64, inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (brand, category_code, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS databrain.purchase_minute_mv TO databrain.purchase_minute AS
SELECT splitByChar('|', key)[1] AS brand, splitByChar('|', key)[2] AS category_code,
       toDateTime(intDiv(minute_ms, 1000)) AS minute, orders, revenue, max_fresh_ms
FROM databrain.purchase_minute_q;

-- ---------- 제어기 판단 기록 (꿈 일기·뇌 화면의 재료)
CREATE TABLE IF NOT EXISTS databrain.decisions_q (
    at_ms UInt64, in_cart_eps Float64, in_view_eps Float64, backlog_cart UInt64, backlog_view UInt64,
    raw_backlog_cart UInt64, raw_backlog_view UInt64, signal String, age_s Float64,
    cart Float64, view Float64, version UInt64
) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'log.decisions',
  kafka_group_name = 'ch-decisions', kafka_format = 'JSONEachRow';

CREATE TABLE IF NOT EXISTS databrain.decisions (
    at DateTime64(3), in_cart_eps Float64, in_view_eps Float64, backlog_cart UInt64, backlog_view UInt64,
    raw_backlog_cart UInt64, raw_backlog_view UInt64,   -- 추측 항법 전 측정값 (ADR-0008)
    signal LowCardinality(String), age_s Float64,       -- 처리 위치를 어디서 읽었나(flink/commit), 몇 초 묵었나
    cart Float64, view Float64, version UInt64
) ENGINE = MergeTree ORDER BY at;

CREATE MATERIALIZED VIEW IF NOT EXISTS databrain.decisions_mv TO databrain.decisions AS
SELECT fromUnixTimestamp64Milli(toInt64(at_ms)) AS at, in_cart_eps, in_view_eps, backlog_cart, backlog_view,
       raw_backlog_cart, raw_backlog_view, signal, age_s, cart, view, version
FROM databrain.decisions_q;
