"""
Flink 작업 공용 소스와 워터마크 설정 (ADR-0007).

1. 이벤트 토픽: 소스에서 '파티션별로' 워터마크를 만든다.
   읽은 뒤 합친 흐름에 워터마크를 붙이면, 밀린 데이터를 따라잡을 때 Kafka 리더가
   파티션 하나를 몇 분치씩 몰아 읽으면서 시각이 뒤섞이고, 다른 파티션 이벤트가 '늦음'으로 버려진다
   (2026-10-17 가짜 데이터로 재현: 닫힌 창 70개 중 대부분이 틀림).

   PyFlink 2.2.1의 from_source는 Python TimestampAssigner를 조용히 무시하고 Kafka 기록 시각을 쓴다
   (2026-10-17 확인: 워터마크가 2019년이 아니라 전송 시각인 2026-10-17로 잡힘).
   그래서 Flink SQL Kafka 소스에서 머리말의 event_ms를 계산 열로 꺼내 WATERMARK를 선언하고,
   to_data_stream으로 DataStream으로 바꾼다. 워터마크 계산은 Java 쪽에서 파티션별로 하고,
   이벤트 시각과 워터마크는 DataStream으로 그대로 넘어온다.

2. 제어 토픽(control.rates): 시각 기준선을 내지 않고, 메시지가 올 때만 잠깐 활성 상태가 된다.
   Flink는 연결된 입력 중 가장 느린 기준선을 따른다.
   - 기준선을 계속 내지 않으면 → 집계 창이 영영 닫히지 않음
   - 현재 시각(2026년)을 내면 → 재생 중인 2019년 데이터가 전부 '늦음'으로 버려질 수 있음
   그래서 기준선 없음 + 짧은 유휴 시간: 메시지 직후 잠깐만 창 닫힘이 미뤄진다.
"""
from __future__ import annotations

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

OUT_OF_ORDER_S = 5        # 같은 파티션 안에서 허용하는 순서 뒤바뀜 (초)
IDLE = "10 s"             # 이 시간 동안 데이터가 없는 파티션은 기준선 계산에서 뺀다 (설계 원칙 9)
CONTROL_IDLE = Duration.of_millis(500)
TAB = "\t"


def setup(t_env) -> None:
    t_env.get_config().set("table.exec.source.idle-timeout", IDLE)


def event_source(t_env, bootstrap: str, topic: str, group: str, name: str):
    """머리말 형식(wire.py) 메시지를 읽어, 이벤트 시각과 워터마크가 붙은 문자열 DataStream으로 돌려준다."""
    event_ms = f"CAST(SPLIT_INDEX(`v`, '{TAB}', 2) AS BIGINT)"
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE `{name}` (
            `v` STRING,
            `ts` AS TO_TIMESTAMP_LTZ({event_ms}, 3),
            WATERMARK FOR `ts` AS `ts` - INTERVAL '{OUT_OF_ORDER_S}' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap}',
            'properties.group.id' = '{group}',
            'scan.startup.mode' = 'earliest-offset',
            'value.format' = 'raw'
        )""")
    return t_env.to_data_stream(t_env.from_path(name)).map(lambda r: r[0], output_type=Types.STRING())


def control_source(env, bootstrap: str, group: str):
    src = (KafkaSource.builder().set_bootstrap_servers(bootstrap).set_topics("control.rates").set_group_id(group)
           .set_starting_offsets(KafkaOffsetsInitializer.earliest())
           .set_value_only_deserializer(SimpleStringSchema()).build())
    return env.from_source(src, WatermarkStrategy.no_watermarks().with_idleness(CONTROL_IDLE), "control")
