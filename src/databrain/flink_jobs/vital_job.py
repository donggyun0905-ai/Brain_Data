"""
Flink 구매 전용 경로 (뇌간: 생명 유지). 거르지 않고 100% 처리해 분 단위 매출·주문 수를 낸다.

상태: 뼈대. 11월에 실제 클러스터에서 검증.
메인 경로와 다른 작업(Job)으로 띄워, 조회 폭증이 와도 처리 자원을 나눠 쓰지 않게 한다.
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
from pyflink.datastream.window import TumblingEventTimeWindows

from databrain import wire

BOOTSTRAP = "kafka:9092"


class EventTime(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[1]


def to_record(raw: str):
    h, body = wire.decode(raw)
    ts = int(calendar.timegm(time.strptime(body["event_time"][:19], "%Y-%m-%dT%H:%M:%S")) * 1000)   # REES46 시각은 UTC
    key = f"{body.get('brand', 'etc')}|{body.get('category_code', 'etc')}"
    return key, ts, 1, float(body.get("price") or 0.0), int(time.time() * 1000) - h.ingest_ms


def merge(a, b):
    return a[0], max(a[1], b[1]), a[2] + b[2], a[3] + b[3], max(a[4], b[4])


if __name__ == "__main__":
    import sys
    prefix = sys.argv[1] if len(sys.argv) > 1 else ""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000)
    src = (KafkaSource.builder().set_bootstrap_servers(BOOTSTRAP).set_topics(f"{prefix}events.purchase")
           .set_group_id("flink-vital").set_starting_offsets(KafkaOffsetsInitializer.earliest())
           .set_value_only_deserializer(SimpleStringSchema()).build())
    stream = (env.from_source(src, WatermarkStrategy.no_watermarks(), "purchase")
              .map(to_record)
              .assign_timestamps_and_watermarks(
                  WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5))
                  .with_idleness(Duration.of_seconds(10)).with_timestamp_assigner(EventTime())))
    agg = stream.key_by(lambda r: r[0]).window(TumblingEventTimeWindows.of(Time.minutes(1))).reduce(merge)
    sink = (KafkaSink.builder().set_bootstrap_servers(BOOTSTRAP)
            .set_record_serializer(KafkaRecordSerializationSchema.builder().set_topic(f"{prefix}agg.purchase")
                                   .set_value_serialization_schema(SimpleStringSchema()).build()).build())
    (agg.map(lambda r: json.dumps({"key": r[0], "minute_ms": r[1] - r[1] % 60000, "orders": r[2],
                                   "revenue": round(r[3], 2), "max_fresh_ms": r[4]}), output_type=Types.STRING())
     .sink_to(sink))
    env.execute("databrain-vital")
