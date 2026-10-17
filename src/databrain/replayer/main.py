"""
재생기: 시간순 Parquet를 읽어 원래 도착 간격(× 배속)대로 Kafka에 보낸다.

    python -m databrain.replayer.main data/parquet/2019-10/day=2019-10-01 --speed 100 \
        --burst 3600,180,10          # 데이터 시각 1시간 지점부터 3분 동안 10배 폭증

- 이벤트 종류별 토픽: events.purchase / events.cart / events.view (설계 원칙 1, 2)
- Kafka 키 = user_id (ADR-0004: 세션 순서 유지, 고른 분산)
- 값 = wire.encode 머리말 형식 (ADR-0005)
- 수면 재생 때는 --topic-prefix replay. 로 별도 토픽에 보낸다 (설계 원칙 8)
- 처리량이 부족하면 --workers N으로 사용자 해시 기준 여러 프로세스로 나눠 보낸다
"""
from __future__ import annotations

import argparse
import time

from databrain import wire
from databrain.replayer.schedule import build_wall_clock
from databrain.sampling.hashing import unit_hash


def parse_burst(s: str) -> tuple[float, float, float]:
    a, b, c = (float(x) for x in s.split(","))
    return a, b, c


def run(path: str, bootstrap: str, speed: float, bursts, prefix: str,
        worker: int, workers: int) -> None:  # pragma: no cover - 실제 브로커 필요
    import duckdb
    from confluent_kafka import Producer

    prod = Producer({"bootstrap.servers": bootstrap, "linger.ms": 5, "batch.num.messages": 10000,
                     "compression.type": "lz4"})
    wall = build_wall_clock(speed, bursts)
    con = duckdb.connect()
    cur = con.execute(f"SELECT * FROM read_parquet('{path}/*.parquet') ORDER BY event_time")
    cols = [d[0] for d in cur.description]
    t0_data, t0_wall, sent = None, time.time(), 0
    while True:
        rows = cur.fetchmany(50_000)
        if not rows:
            break
        for r in rows:
            ev = dict(zip(cols, r))
            uid = str(ev["user_id"])
            if workers > 1 and int(unit_hash(f"w:{uid}") * workers) != worker:
                continue
            t = ev["event_time"].timestamp()
            if t0_data is None:
                t0_data = t
            due = t0_wall + wall(t - t0_data)
            lag = due - time.time()
            if lag > 0:
                time.sleep(lag)
            payload = {k: (v.isoformat() if hasattr(v, "isoformat") else v)
                       for k, v in ev.items() if k not in ("user_id", "event_id", "lane", "day")}
            value = wire.encode(uid, ev["event_id"], int(time.time() * 1000), payload)
            prod.produce(f"{prefix}events.{ev['lane']}", key=uid.encode(), value=value)
            sent += 1
            if sent % 10_000 == 0:
                prod.poll(0)
    prod.flush()
    print(f"sent={sent} wall={time.time() - t0_wall:.1f}s")


def main() -> None:  # pragma: no cover
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--bootstrap", default="localhost:29092",
                    help="PC에서는 localhost:29092, 컨테이너 안에서는 kafka:9092")
    ap.add_argument("--speed", type=float, default=100.0)
    ap.add_argument("--burst", action="append", default=[], type=parse_burst)
    ap.add_argument("--topic-prefix", default="")
    ap.add_argument("--worker", type=int, default=0)
    ap.add_argument("--workers", type=int, default=1)
    a = ap.parse_args()
    run(a.path, a.bootstrap, a.speed, a.burst, a.topic_prefix, a.worker, a.workers)


if __name__ == "__main__":
    main()
