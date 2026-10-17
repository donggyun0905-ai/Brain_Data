"""
제어기 실행 루프: 1초마다 밀림을 읽고, 통과 비율을 계산해 제어 토픽에 발행한다.

    python -m databrain.controller.main --bootstrap localhost:9092 --capacity 10000

제어 토픽 메시지는 버전 번호를 가진다 (운영 함정 #5).
Flink는 더 높은 버전만 받아들이고, 마지막 값은 체크포인트에 저장된다.
모든 결정은 decisions 토픽에도 남겨 꿈 일기와 뇌 화면의 재료로 쓴다.
"""
from __future__ import annotations

import argparse
import json
import time

from databrain.controller.lag import KafkaLagReader, backlog, in_rate
from databrain.controller.rate_model import CapacityModel, Rates, compute_rates

TOPIC_CART, TOPIC_VIEW = "events.cart", "events.view"
TOPIC_CONTROL, TOPIC_DECISIONS = "control.rates", "log.decisions"


def main() -> None:  # pragma: no cover - 실제 브로커 필요
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--group", default="flink-main", help="Flink 메인 작업의 소비자 그룹")
    ap.add_argument("--capacity", type=float, required=True, help="E1에서 측정한 처리 능력 (이벤트/초)")
    ap.add_argument("--period", type=float, default=1.0)
    args = ap.parse_args()

    from confluent_kafka import Producer
    reader = KafkaLagReader(args.bootstrap, args.group)
    producer = Producer({"bootstrap.servers": args.bootstrap})
    model = CapacityModel(capacity_eps=args.capacity)
    rates = Rates()
    prev = {TOPIC_CART: None, TOPIC_VIEW: None}

    while True:
        now = time.time()
        snaps = {t: reader.snapshot(t, now) for t in (TOPIC_CART, TOPIC_VIEW)}
        rc, rv = (in_rate(prev[t], snaps[t]) for t in (TOPIC_CART, TOPIC_VIEW))
        bc, bv = (backlog(snaps[t]) for t in (TOPIC_CART, TOPIC_VIEW))
        new = compute_rates(model, rc, rv, bc, bv, rates)
        if new.version != rates.version:
            msg = {"version": new.version, "cart": new.cart, "view": new.view, "at_ms": int(now * 1000)}
            producer.produce(TOPIC_CONTROL, key=b"rates", value=json.dumps(msg).encode())
        producer.produce(TOPIC_DECISIONS, value=json.dumps({
            "at_ms": int(now * 1000), "in_cart_eps": rc, "in_view_eps": rv,
            "backlog_cart": bc, "backlog_view": bv, "cart": new.cart, "view": new.view,
            "version": new.version}).encode())
        producer.poll(0)
        rates, prev = new, snaps
        time.sleep(max(0.0, args.period - (time.time() - now)))


if __name__ == "__main__":
    main()
