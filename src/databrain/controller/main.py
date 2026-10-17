"""
제어기 실행 루프: 1초마다 밀림을 읽고, 통과 비율을 계산해 제어 토픽에 발행한다.

    python -m databrain.controller.main --bootstrap localhost:29092 --capacity 1250

제어 토픽 메시지는 버전 번호를 가진다 (운영 함정 #5).
Flink는 더 높은 버전만 받아들이고, 마지막 값은 체크포인트에 저장된다.
모든 결정은 decisions 토픽에도 남겨 꿈 일기와 뇌 화면의 재료로 쓴다.

처리 위치 신호 (ADR-0008, --signal):
  auto       Flink REST 처리 위치 + 이동 중인 양 + 추측 항법. REST가 안 되면 커밋 위치 + 추측 항법 (기본)
  commit     커밋 위치 그대로 (2026-10-17까지의 방식. 비교 실험용)
  commit-dr  커밋 위치 + 추측 항법 (REST 없이 쓰는 경우)
"""
from __future__ import annotations

import argparse
import json
import time
from dataclasses import replace

from databrain.controller.lag import FlinkProgressReader, KafkaLagReader, backlog, dead_reckon, in_rate
from databrain.controller.rate_model import CapacityModel, Rates, compute_rates

TOPIC_CART, TOPIC_VIEW = "events.cart", "events.view"
TOPIC_CONTROL, TOPIC_DECISIONS = "control.rates", "log.decisions"


class CommitAge:
    """커밋 위치가 마지막으로 바뀐 시각을 기억해, 커밋 값이 몇 초 묵었는지 추정한다.
    커밋에 담긴 위치는 체크포인트 시작 시점 것이라, 바뀐 순간에도 이미 commit_delay초쯤 묵어 있다."""

    def __init__(self, commit_delay_s: float):
        self.delay, self.last, self.changed_at = commit_delay_s, None, None

    def age(self, committed_total: int, now_s: float) -> float:
        if committed_total != self.last:
            self.last, self.changed_at = committed_total, now_s
        return now_s - self.changed_at + self.delay


def main() -> None:  # pragma: no cover - 실제 브로커 필요
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:29092",
                    help="PC에서는 localhost:29092, 컨테이너 안에서는 kafka:9092")
    ap.add_argument("--group", default="flink-main", help="Flink 메인 작업의 소비자 그룹")
    ap.add_argument("--capacity", type=float, required=True, help="E1에서 측정한 처리 능력 (이벤트/초)")
    ap.add_argument("--period", type=float, default=1.0)
    ap.add_argument("--signal", choices=["auto", "commit", "commit-dr"], default="auto")
    ap.add_argument("--flink-rest", default="http://localhost:8081",
                    help="PC에서는 http://localhost:8081, 컨테이너 안에서는 http://jobmanager:8081")
    ap.add_argument("--flink-age", type=float, default=0.5,
                    help="REST 값이 평균 몇 초 묵었나 (metrics.fetcher.update-interval 1초의 절반)")
    ap.add_argument("--commit-delay", type=float, default=2.0,
                    help="커밋이 보일 때 이미 묵은 시간 (PC 실측 체크포인트 평균 0.8~2.7초)")
    args = ap.parse_args()

    from confluent_kafka import Producer
    topics = [TOPIC_CART, TOPIC_VIEW]
    reader = KafkaLagReader(args.bootstrap, args.group)
    flink = FlinkProgressReader(args.flink_rest, topics) if args.signal == "auto" else None
    producer = Producer({"bootstrap.servers": args.bootstrap})
    model = CapacityModel(capacity_eps=args.capacity)
    # 버전은 시작 시각(ms)부터 센다. 0부터 세면 제어기를 다시 켰을 때 Flink가 이미 본 버전보다 작아서
    # 새 비율을 전부 무시한다 (운영 함정 #5의 반대 경우, 2026-10-18 발견)
    rates = Rates(version=int(time.time() * 1000))
    prev = {t: None for t in topics}
    commit_age = CommitAge(args.commit_delay)

    while True:
        now = time.time()
        try:
            snaps = reader.snapshot(topics, now)
        except Exception as e:   # Kafka 일시 오류: 이번 판단은 건너뛴다 (지난 비율 유지)
            print("Kafka 읽기 실패, 건너뜀:", e, flush=True)
            time.sleep(args.period)
            continue
        prog = flink.read() if flink else None
        c_age = commit_age.age(sum(sum(s.committed.values()) for s in snaps.values()), now)
        if prog is not None:
            pos, moving = prog
            raw = {t: backlog(replace(snaps[t], processed=pos[t])) + moving[t] for t in topics}
            source, age = "flink", args.flink_age
        else:
            raw = {t: backlog(snaps[t], use_processed=False) for t in topics}
            source, age = "commit", (c_age if args.signal != "commit" else 0.0)
        bc, bv = dead_reckon(model, rates, raw[TOPIC_CART], raw[TOPIC_VIEW], age)
        rc, rv = (in_rate(prev[t], snaps[t]) for t in topics)
        new = compute_rates(model, rc, rv, bc, bv, rates)
        if new.version != rates.version:
            msg = {"version": new.version, "cart": new.cart, "view": new.view, "at_ms": int(now * 1000)}
            producer.produce(TOPIC_CONTROL, key=b"rates", value=json.dumps(msg).encode())
        producer.produce(TOPIC_DECISIONS, value=json.dumps({
            "at_ms": int(now * 1000), "in_cart_eps": rc, "in_view_eps": rv,
            "backlog_cart": round(bc), "backlog_view": round(bv),
            "raw_backlog_cart": raw[TOPIC_CART], "raw_backlog_view": raw[TOPIC_VIEW],
            "signal": source, "age_s": round(age, 2),
            "cart": new.cart, "view": new.view, "version": new.version}).encode())
        producer.poll(0)
        rates, prev = new, snaps
        time.sleep(max(0.0, args.period - (time.time() - now)))


if __name__ == "__main__":
    main()
