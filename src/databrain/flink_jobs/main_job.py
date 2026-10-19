"""
Flink 메인 경로 (장바구니·조회): 읽으면서 거르기 → 1분 집계 → Kafka 출력.

상태: 2026-10-17 가짜 데이터로 실행·정답 대조 확인 (docs/week1.md). 실제 데이터 검증은 11월.

    flink run -d -py /opt/databrain/src/databrain/flink_jobs/main_job.py   (jobmanager 컨테이너 안에서)

구조 (기획서 '확정된 설계 원칙'):
  events.cart, events.view ──┐  (워터마크는 소스에서 파티션별로, ADR-0007)
                              ├─ 거르기 (머리말만 읽음, 혼합 샘플링, 이벤트에 비율 기록)
  control.rates ── 브로드캐스트┘        │
                                       └─ 본문 파싱 → 2단계 집계 (카테고리+소금 → 카테고리) → agg.minute
                                          집계에는 오차 범위용 흔들림 합(var_sum)도 함께 더한다 (ADR-0015)
"""
from __future__ import annotations

import json
import os
import time

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink
from pyflink.datastream.functions import BroadcastProcessFunction, MapFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import StreamTableEnvironment

from databrain import wire
from databrain.flink_jobs.watermarks import control_source, event_source, setup
from databrain.sampling.hashing import keep, weight, unit_hash
from databrain.sampling.interval import ClusterIndex, variance_part
from databrain.sampling.heavy import HeavyUserTracker

BOOTSTRAP = "kafka:9092"
SALT_BUCKETS = 8                     # 인기 상품·카테고리 쏠림을 나누는 조각 수 (ADR-0004)
RATES = MapStateDescriptor("rates", Types.STRING(), Types.STRING())


class SheddingFilter(BroadcastProcessFunction):
    """입력: (lane, raw_value). 출력: (lane, raw_value, rate, heavy) — 통과한 것만.

    `heavy`를 함께 내보낸다. 오차 범위는 상위 사용자(이벤트 단위)와 나머지(사용자 단위)의
    흔들림이 다르므로 뒤 단계에서 그 구분이 필요하다 (ADR-0015).
    """

    def open(self, runtime_context):
        self.heavy = HeavyUserTracker()          # 사용자별 30분 감쇠 건수 50 이상 → 이벤트 단위 (ADR-0010, 0011)

    def process_element(self, value, ctx):
        lane, raw = value
        state = ctx.get_broadcast_state(RATES)
        rates = json.loads(state.get("current")) if state.contains("current") else {"cart": 1.0, "view": 1.0}
        rate = float(rates[lane])
        h = wire.peek(raw)                       # 본문은 아직 파싱하지 않음
        self.heavy.add(h.user_id, h.event_ms)
        hv = self.heavy.is_heavy(h.user_id)
        if keep(h.user_id, h.event_id, rate, hv):
            yield lane, raw, rate, hv

    def process_broadcast_element(self, value, ctx):
        msg = json.loads(value)
        state = ctx.get_broadcast_state(RATES)
        cur = json.loads(state.get("current")) if state.contains("current") else {"version": -1}
        if msg["version"] > cur["version"]:          # 늦거나 순서가 바뀐 옛 신호는 무시 (운영 함정 #5)
            state.put("current", json.dumps(msg))


class ToRecord(MapFunction):
    """통과한 이벤트를 집계용 튜플로 바꾸고, 오차 범위용 흔들림 조각을 붙인다 (ADR-0015).

    묶음 안 순번을 세야 하므로 상태가 필요하다 (`ClusterIndex`). Kafka 키가 user_id라서
    한 사용자의 같은 종류 이벤트는 늘 같은 작업으로 오므로 작업마다 따로 세어도 맞는다 (ADR-0013).
    """

    def open(self, runtime_context):
        self.ci = ClusterIndex()

    def map(self, value):
        lane, raw, rate, heavy = value
        h, body = wire.decode(raw)
        ts = h.event_ms
        salt = int(unit_hash(f"s:{h.user_id}") * SALT_BUCKETS)
        cat = body.get("category_code", "etc")
        key = f"{body['event_type']}|{cat}|{salt}"
        idx = self.ci.next(h.user_id, f"{body['event_type']}|{cat}", ts - ts % 60_000, rate, heavy)
        # (키, 이벤트시각ms, 가중 개수, 흔들림 합, 원본 건수, 최대 신선도 지연ms)
        return (key, ts, weight(rate), variance_part(rate, idx), 1,
                int(time.time() * 1000) - h.ingest_ms)


def merge(a, b):
    """두 칸 합치기. 가중 개수와 흔들림 합은 둘 다 그냥 더해진다 (2단계 집계에서도 같은 함수를 쓴다)."""
    return a[0], max(a[1], b[1]), a[2] + b[2], a[3] + b[3], a[4] + b[4], max(a[5], b[5])


def build(env: StreamExecutionEnvironment, prefix: str = "") -> None:
    group = "flink-main" if not prefix else f"flink-{prefix.rstrip('.')}"
    t_env = StreamTableEnvironment.create(env)
    setup(t_env)
    view = event_source(t_env, BOOTSTRAP, f"{prefix}events.view", group, "src_view").map(lambda v: ("view", v))
    cart = event_source(t_env, BOOTSTRAP, f"{prefix}events.cart", group, "src_cart").map(lambda v: ("cart", v))
    control = control_source(env, BOOTSTRAP, group + "-control").broadcast(RATES)

    kept = view.union(cart).connect(control).process(SheddingFilter()).map(ToRecord())

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
                                     "var_sum": r[3], "raw_count": r[4], "max_fresh_ms": r[5]}),
               output_type=Types.STRING())
     .sink_to(sink))


if __name__ == "__main__":
    import sys
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(int(os.environ.get("CHECKPOINT_MS", "10000")))
    build(env, prefix=sys.argv[1] if len(sys.argv) > 1 else "")
    env.execute("databrain-main")
