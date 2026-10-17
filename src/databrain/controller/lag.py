"""
Kafka에서 밀림(backlog)과 유입 속도를 직접 계산한다 (운영 함정 #1, ADR-0002, ADR-0008).

Prometheus 기본 수집 주기(15초)는 1초마다 판단하는 제어기에 너무 느리다.
그래서 제어기는 '토픽 끝 위치'를 Kafka 관리 API로 직접 읽는다.

  밀림 = 끝 위치 - 처리 위치          (모든 파티션 합)
  유입 속도 = (이번 끝 위치 - 지난 끝 위치) / 경과 시간

처리 위치는 어디서 읽나 (ADR-0008):
  1순위  Flink REST의 소스 currentOffset (작업 관리자가 1초마다 모음) + 이동 중인 양
  대비책 소비자 그룹 커밋 위치. Flink는 체크포인트가 끝나야 커밋하므로 실측 5~9초 묵어 있다
         (2026-10-18 PC 실측: 커밋 기준 밀림이 실제보다 중앙값 7,000~11,000건 큼)
  두 경우 모두 '측정한 뒤 지난 시간 동안 Flink가 처리했을 양'을 처리 능력 모델로 빼 준다 (추측 항법).
"""
from __future__ import annotations

import json
import re
import urllib.request
from dataclasses import dataclass
from typing import Mapping

from databrain.controller.rate_model import CapacityModel, Rates


@dataclass(frozen=True)
class Snapshot:
    at_s: float
    end: Mapping[int, int]                       # 파티션 -> 토픽 끝 위치
    committed: Mapping[int, int]                 # 파티션 -> 소비자 그룹이 커밋한 위치
    processed: Mapping[int, int] | None = None   # 파티션 -> Flink가 다음에 읽을 위치 (REST, 없으면 None)


def backlog(s: Snapshot, use_processed: bool = True) -> int:
    """처리 위치가 있으면 그것으로, 없으면 커밋 위치로 계산. 처리 위치가 없는 파티션은 커밋 위치를 쓴다."""
    pos = s.processed if (use_processed and s.processed is not None) else {}
    return sum(max(0, s.end[p] - pos.get(p, s.committed.get(p, 0))) for p in s.end)


def in_rate(prev: Snapshot | None, cur: Snapshot) -> float:
    if prev is None or cur.at_s <= prev.at_s:
        return 0.0
    produced = sum(max(0, cur.end[p] - prev.end.get(p, cur.end[p])) for p in cur.end)
    return produced / (cur.at_s - prev.at_s)


def drain_rate(m: CapacityModel, rates: Rates, backlog_cart: float, backlog_view: float) -> float:
    """지금 비율로 Flink가 1초에 읽어 나갈 수 있는 이벤트 수 (처리 능력 모델).
    읽은 이벤트마다 거르기 비용, 남긴 이벤트마다 무거운 처리 비용 1이 든다."""
    total = backlog_cart + backlog_view
    if total <= 0:
        return m.capacity_eps
    kept = (backlog_cart * rates.cart + backlog_view * rates.view) / total
    return m.capacity_eps / (m.filter_cost + kept)


def dead_reckon(m: CapacityModel, rates: Rates, backlog_cart: float, backlog_view: float,
                age_s: float, max_age_s: float = 15.0) -> tuple[float, float]:
    """추측 항법: age_s초 전에 잰 밀림에서, 그동안 처리했을 양을 뺀다.
    두 경로는 Kafka에 쌓인 비중대로 함께 줄어든다고 본다.
    Flink가 멈췄는데 측정도 끊긴 경우 밀림을 0으로 착각하지 않도록 경과 시간에 상한을 둔다."""
    total = backlog_cart + backlog_view
    if total <= 0 or age_s <= 0:
        return backlog_cart, backlog_view
    done = min(total, drain_rate(m, rates, backlog_cart, backlog_view) * min(age_s, max_age_s))
    left = 1.0 - done / total
    return backlog_cart * left, backlog_view * left


# ---------------- Flink REST에서 처리 위치 읽기 ----------------
_OFFSET = re.compile(r"KafkaSourceReader\.topic\.(?P<topic>.+)\.partition\.(?P<p>\d+)\.currentOffset$")


def parse_offsets(metrics: list[dict], topics: list[str]) -> dict[str, dict[int, int]]:
    """REST 지표 [{"id": ..., "value": ...}] → {토픽: {파티션: 다음에 읽을 위치}}.
    Flink는 지표 이름에서 토픽의 '.'을 '_'로 바꾸고, currentOffset은 '마지막으로 읽은' 위치라 +1 한다.
    아직 읽지 않은 파티션은 음수로 나오므로 뺀다."""
    by_name = {t.replace(".", "_"): t for t in topics}
    out: dict[str, dict[int, int]] = {t: {} for t in topics}
    for m in metrics:
        hit = _OFFSET.search(m["id"])
        if not hit or hit["topic"] not in by_name:
            continue
        v = int(float(m["value"]))
        if v >= 0:
            out[by_name[hit["topic"]]][int(hit["p"])] = v + 1
    return out


def in_flight(source_out: Mapping[str, int], control_out: int, filter_in: int) -> dict[str, int]:
    """소스는 읽었지만 거르기 단계에 아직 안 닿은 양 (Python 묶음 처리, 실측 약 1,000건).
    거르기 단계 입력에는 제어 메시지도 섞여 있어 뺀다. 경로별 몫은 소스 출력 비중으로 나눈다."""
    total_out = sum(source_out.values())
    gap = max(0, total_out + control_out - filter_in)
    if total_out <= 0 or gap == 0:
        return {t: 0 for t in source_out}
    return {t: round(gap * n / total_out) for t, n in source_out.items()}


class FlinkProgressReader:  # pragma: no cover - 실제 Flink가 필요
    """Flink REST에서 소스가 읽은 위치와 이동 중인 양을 읽는다.
    작업 관리자 설정 metrics.fetcher.update-interval(1초)마다 값이 새로 모인다."""

    def __init__(self, rest_url: str, topics: list[str], timeout_s: float = 0.5):
        self.url, self.topics, self.timeout = rest_url.rstrip("/"), topics, timeout_s
        self._plan = None   # (job_id, {vertex_id: [지표 이름]}, filter_vertex, control_vertex)

    def _get(self, path: str):
        with urllib.request.urlopen(self.url + path, timeout=self.timeout) as r:
            return json.load(r)

    def _discover(self):
        self._plan = None
        for job in self._get("/jobs/overview")["jobs"]:
            if job["state"] != "RUNNING":
                continue
            jid = job["jid"]
            sources, flt, ctl = {}, None, None
            for v in self._get(f"/jobs/{jid}")["vertices"]:
                ids = [m["id"] for m in self._get(f"/jobs/{jid}/vertices/{v['id']}/metrics")]
                offs = [i for i in ids if _OFFSET.search(i) and any(
                    f".topic.{t.replace('.', '_')}." in i for t in self.topics)]
                if offs:
                    op_out = [i for i in ids if i.startswith("0.Source__") and i.endswith(".numRecordsOut")]
                    sources[v["id"]] = offs + op_out[:1]
                elif "Co-Process" in v["name"]:
                    flt = v["id"]
                elif v["name"].startswith("Source: control"):
                    ctl = v["id"]
            if sources:
                self._plan = (jid, sources, flt, ctl)
                return

    def read(self) -> tuple[dict[str, dict[int, int]], dict[str, int]] | None:
        """(토픽별 처리 위치, 토픽별 이동 중인 양). 작업이 없거나 응답이 없으면 None → 대비책 사용."""
        try:
            if self._plan is None:
                self._discover()
            if self._plan is None:
                return None
            jid, sources, flt, ctl = self._plan
            metrics, src_out = [], {}
            for vid, names in sources.items():
                vals = self._get(f"/jobs/{jid}/vertices/{vid}/metrics?get={','.join(names)}")
                metrics += vals
                topic = next((t for t in self.topics for m in vals
                              if f".topic.{t.replace('.', '_')}." in m["id"]), None)
                outs = [int(float(m["value"])) for m in vals if m["id"].endswith(".numRecordsOut")]
                if topic and outs:
                    src_out[topic] = outs[0]
            pos = parse_offsets(metrics, self.topics)
            if any(not pos[t] for t in self.topics):   # 파티션 지표가 아직 없음 → 다음에 다시 찾기
                self._plan = None
                return None
            moving = {t: 0 for t in self.topics}
            if flt and ctl and len(src_out) == len(self.topics):
                f_in = self._get(f"/jobs/{jid}/vertices/{flt}/metrics?get=0.numRecordsIn")
                c_out = self._get(f"/jobs/{jid}/vertices/{ctl}/metrics?get=0.numRecordsOut")
                if f_in and c_out:
                    moving = in_flight(src_out, int(float(c_out[0]["value"])), int(float(f_in[0]["value"])))
            return pos, moving
        except Exception:   # 작업 재시작, 네트워크 문제 등 → 다음 판단 때 다시 찾음
            self._plan = None
            return None


class KafkaLagReader:  # pragma: no cover - 실제 브로커가 필요
    """관리 API로 여러 파티션의 끝 위치와 커밋 위치를 한 번에 읽는다.
    파티션마다 따로 묻던 방식(실측 15~210ms)보다 빠르다 (실측 2~3ms, 2026-10-17)."""

    def __init__(self, bootstrap: str, group_id: str, timeout_s: float = 2.0):
        from confluent_kafka.admin import AdminClient
        self._a = AdminClient({"bootstrap.servers": bootstrap})
        self.group_id, self.timeout = group_id, timeout_s
        self._parts: dict[str, list[int]] = {}

    def snapshot(self, topics: list[str], now_s: float) -> dict[str, Snapshot]:
        from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
        from confluent_kafka.admin import OffsetSpec
        for t in topics:
            if t not in self._parts:
                md = self._a.list_topics(t, timeout=self.timeout)
                self._parts[t] = sorted(md.topics[t].partitions)
        tps = [TopicPartition(t, p) for t in topics for p in self._parts[t]]
        ends = self._a.list_offsets({tp: OffsetSpec.latest() for tp in tps},
                                    request_timeout=self.timeout)
        com = self._a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(self.group_id, tps)],
                                                  request_timeout=self.timeout)
        end = {t: {} for t in topics}
        for tp, fut in ends.items():
            end[tp.topic][tp.partition] = fut.result(timeout=self.timeout).offset
        committed = {t: {} for t in topics}
        for tp in list(com.values())[0].result(timeout=self.timeout).topic_partitions:
            committed[tp.topic][tp.partition] = max(tp.offset, 0)
        return {t: Snapshot(now_s, end[t], committed[t]) for t in topics}
