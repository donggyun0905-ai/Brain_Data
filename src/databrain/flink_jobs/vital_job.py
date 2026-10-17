"""
Flink 구매 전용 경로 (뇌간: 생명 유지). 거르지 않고 100% 처리해 분 단위 매출·주문 수를 낸다.

상태: 2026-10-17 가짜 데이터로 실행·정답 대조 확인 (docs/week1.md). 실제 데이터 검증은 11월.
메인 경로와 다른 작업(Job)으로 띄워, 조회 폭증이 와도 처리 자원을 나눠 쓰지 않게 한다.
"""
from __future__ import annotations

import json
import time

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import StreamTableEnvironment

from databrain import wire
from databrain.flink_jobs.watermarks import event_source, setup

BOOTSTRAP = "kafka:9092"


def to_record(raw: str):
    h, body = wire.decode(raw)
    ts = h.event_ms
    key = f"{body.get('brand', 'etc')}|{body.get('category_code', 'etc')}"
    return key, ts, 1, float(body.get("price") or 0.0), int(time.time() * 1000) - h.ingest_ms


def merge(a, b):
    return a[0], max(a[1], b[1]), a[2] + b[2], a[3] + b[3], max(a[4], b[4])


if __name__ == "__main__":
    import sys
    prefix = sys.argv[1] if len(sys.argv) > 1 else ""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    t_env = StreamTableEnvironment.create(env)
    setup(t_env)
    stream = event_source(t_env, BOOTSTRAP, f"{prefix}events.purchase", "flink-vital", "src_purchase").map(to_record)
    agg = stream.key_by(lambda r: r[0]).window(TumblingEventTimeWindows.of(Time.minutes(1))).reduce(merge)
    sink = (KafkaSink.builder().set_bootstrap_servers(BOOTSTRAP)
            .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic(f"{prefix}agg.purchase")
                                   .set_value_serialization_schema(SimpleStringSchema()).build()).build())
    (agg.map(lambda r: json.dumps({"key": r[0], "minute_ms": r[1] - r[1] % 60000, "orders": r[2],
                                   "revenue": round(r[3], 2), "max_fresh_ms": r[4]}), output_type=Types.STRING())
     .sink_to(sink))
    env.execute("databrain-vital")
