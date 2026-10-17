"""
Flink 메인 경로 (장바구니·조회): 읽으면서 거르기 → 1분 집계 → Kafka 출력.

상태: 뼈대. API 이름은 PyFlink 2.2.1 패키지에서 확인했지만 실제 클러스터 실행은 11월에 검증한다.

    flink run -py src/databrain/flink_jobs/main_job.py --pyFiles src

구조 (기획서 '확정된 설계 원칙'):
  events.cart, events.view ──┐
                              ├─ 거르기 (머리말만 읽음, 혼합 샘플링, 이벤트에 비율 기록)
  control.rates ── 브로드캐스트┘        │
                                       └─ 본문 파싱 → 이벤트 시간 워터마크(유휴 시간 포함)
                                           → 2단계 집계 (카테고리+소금 → 카테고리) → agg.minute
"""
from __future__ import annotations

import calendar
import json
import time

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetsInitializer, KafkaRecordSerializationSchema,
                                                 KafkaSink, KafkaSource)
from pyflink.datastream.functions import BroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

from databrain import wire
from databrain.sampling.hashing import keep, weight, unit_hash
from databrain.sampling.heavy import HeavyUserTracker

BOOTSTRAP = "kafka:9092"
SALT_BUCKETS = 8                     # 인기 상품·카테고리 쏠림을 나누는 조각 수 (ADR-0004)
RATES = MapStateDescriptor("rates", Types.STRING(), Types.STRING())


class SheddingFilter(BroadcastProcessFunction):
    """입력: (lane, raw_value). 출력: (lane, raw_value, rate) — 통과한 것만."""

    def open(self, runtime_context):
        self.heavy = HeavyUserTracker()
        self.seen = 0

    def process_element(self, value, ctx):
        lane, raw = value
        state = ctx.get_broadcast_state(RATES)
        rates = json.loads(state.get("current")) if state.contains("current") else {"cart": 1.0, "view": 1.0}
        rate = float(rates[lane])
        h = wire.peek(raw)                       # 본문은 아직 파싱하지 않음
        self.heavy.add(h.user_id)
        self.seen += 1
        if self.seen % 100_000 == 0:
            self.heavy.decay()
        if keep(h.user_id, h.event_id, rate, self.heavy.is_heavy(h.user_id)):
            yield lane, raw, rate

    def process_broadcast_element(self, value, ctx):
        msg = json.loads(value)
        state = ctx.get_broadcast_state(RATES)
        cur = json.loads(state.get("current")) if state.contains("current") else {"version": -1}
        if msg["version"] > cur["version"]:          # 늦거나 순서가 바뀐 옛 신호는 무시 (운영 함정 #5)
            state.put("current", json.dumps(msg))


class EventTime(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[1]


def to_record(value):
    lane, raw, rate = value
    h, body = wire.decode(raw)
    ts = int(calendar.timegm(time.strptime(body["event_time"][:19], "%Y-%m-%dT%H:%M:%S")) * 1000)   # REES46 시각은 UTC
    salt = int(unit_hash(f"s:{h.user_id}") * SALT_BUCKETS)
    # (키, 이벤트시각ms, 가중 개수, 가중 매출, 원본 건수, 최대 신선도 지연ms)
    key = f"{body['event_type']}|{body.get('category_code', 'etc')}|{salt}"
    w = weight(rate)
    return key, ts, w, 0.0, 1, int(time.time() * 1000) - h.ingest_ms


def merge(a, b):
    return a[0], max(a[1], b[1]), a[2] + b[2], a[3] + b[3], a[4] + b[4], max(a[5], b[5])


def source(env, topic: str, group: str, name: str):
    src = (KafkaSource.builder().set_bootstrap_servers(BOOTSTRAP).set_topics(topic).set_group_id(group)
           .set_starting_offsets(KafkaOffsetsInitializer.earliest())
           .set_value_only_deserializer(SimpleStringSchema())
           .set_property("commit.offsets.on.checkpoint", "true").build())
    return env.from_source(src, WatermarkStrategy.no_watermarks(), name)


def build(env: StreamExecutionEnvironment, prefix: str = "") -> None:
    group = "flink-main" if not prefix else f"flink-{prefix.rstrip('.')}"
    view = source(env, f"{prefix}events.view", group, "view").map(lambda v: ("view", v))
    cart = source(env, f"{prefix}events.cart", group, "cart").map(lambda v: ("cart", v))
    control = source(env, "control.rates", group + "-control", "control").broadcast(RATES)

    kept = (view.union(cart).connect(control).process(SheddingFilter())
            .map(to_record)
            .assign_timestamps_and_watermarks(
                WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
                .with_idleness(Duration.of_seconds(10))                # 운영 함정 #2
                .with_timestamp_assigner(EventTime())))

    minute = Time.minutes(1)
    partial = kept.key_by(lambda r: r[0]).window(TumblingEventTimeWindows.of(minute)).reduce(merge)
    final = (partial.map(lambda r: (r[0].rsplit("|", 1)[0],) + tuple(r[1:]))      # 소금 제거
             .key_by(lambda r: r[0]).window(TumblingEventTimeWindows.of(minute)).reduce(merge))

    sink = (KafkaSink.builder().set_bootstrap_servers(BOOTSTRAP)
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                   .set_topic(f"{prefix}agg.minute")
                                   .set_value_serialization_schema(SimpleStringSchema()).build())
            .build())
    (final.map(lambda r: json.dumps({"key": r[0], "minute_ms": r[1] - r[1] % 60000, "est_count": r[2],
                                     "raw_count": r[4], "max_fresh_ms": r[5]}), output_type=Types.STRING())
     .sink_to(sink))


if __name__ == "__main__":
    import sys
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    build(env, prefix=sys.argv[1] if len(sys.argv) > 1 else "")
    env.execute("databrain-main")
