"""
Kafka에서 밀림(backlog)과 유입 속도를 직접 계산한다 (운영 함정 #1).

Prometheus 기본 수집 주기(15초)는 1초마다 판단하는 제어기에 너무 느리다.
그래서 제어기는 Kafka 관리 API로 '토픽 끝 위치'와 '소비자 그룹이 읽은 위치'를 직접 읽는다.

  밀림 = 끝 위치 - 읽은 위치          (모든 파티션 합)
  유입 속도 = (이번 끝 위치 - 지난 끝 위치) / 경과 시간
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class Snapshot:
    at_s: float
    end: Mapping[int, int]        # 파티션 -> 토픽 끝 위치
    committed: Mapping[int, int]  # 파티션 -> 소비자 그룹이 읽은 위치


def backlog(s: Snapshot) -> int:
    return sum(max(0, s.end[p] - s.committed.get(p, 0)) for p in s.end)


def in_rate(prev: Snapshot | None, cur: Snapshot) -> float:
    if prev is None or cur.at_s <= prev.at_s:
        return 0.0
    produced = sum(max(0, cur.end[p] - prev.end.get(p, cur.end[p])) for p in cur.end)
    return produced / (cur.at_s - prev.at_s)


class KafkaLagReader:  # pragma: no cover - 실제 브로커가 필요해 단위 테스트 대상 아님
    """confluent-kafka로 스냅샷을 만든다. 11월 1주차에 실제 브로커로 확인할 것."""

    def __init__(self, bootstrap: str, group_id: str):
        from confluent_kafka import Consumer
        self._c = Consumer({"bootstrap.servers": bootstrap, "group.id": group_id,
                            "enable.auto.commit": False})
        self.group_id = group_id

    def snapshot(self, topic: str, now_s: float) -> Snapshot:
        from confluent_kafka import TopicPartition
        md = self._c.list_topics(topic, timeout=5)
        parts = [TopicPartition(topic, p) for p in md.topics[topic].partitions]
        end = {}
        for tp in parts:
            _low, high = self._c.get_watermark_offsets(tp, timeout=5, cached=False)
            end[tp.partition] = high
        committed = {tp.partition: max(tp.offset, 0) for tp in self._c.committed(parts, timeout=5)}
        return Snapshot(now_s, end, committed)
