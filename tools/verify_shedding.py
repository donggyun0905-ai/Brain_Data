"""Flink 거르기 확인 (ADR-0011): 비율을 고정해 두고 Flink가 남긴 건수를 같은 규칙으로 계산한 정답과 칸마다 대조한다.
tools/verify_flink.py는 전부 통과(비율 1) 상태라 상위 사용자 기준을 시험하지 못한다.

    python tools/verify_shedding.py make <출력 폴더>        # 11/17 13:50~14:10 실제 데이터 + 가짜 봇 5개(분당 20건)
    python tools/verify_shedding.py run <출력 폴더> 5       # 조회 10%로 고정, 5배속 실시간 재생 후 대조

정답: 같은 이벤트를 이벤트 시각 순으로 HeavyUserTracker(기본값) + keep()에 넣어 센 (범주, 1분)별 남은 건수와 1/비율 합.
Flink는 병렬 1이라 필터 하나가 조회·장바구니를 모두 본다. 마지막 1분 창은 대조에서 뺀다 (ADR-0007).
"""
import json
import sys
import time

import numpy as np

sys.path.insert(0, "src")
sys.path.insert(0, "tools")
from databrain.sampling.hashing import keep, weight  # noqa: E402
from databrain.sampling.heavy import HeavyUserTracker  # noqa: E402

SRC = "data/parquet/2019-11/day=2019-11-17"
VIEW_RATE, N_BOTS, BOT_PER_MIN = 0.1, 5, 20


def make(out: str) -> None:
    import duckdb
    import os
    os.makedirs(f"{out}/day=2019-11-17", exist_ok=True)
    con = duckdb.connect()
    df = con.sql(f"""SELECT * FROM read_parquet('{SRC}/*.parquet')
        WHERE event_time >= '2019-11-17 13:50:00' AND event_time < '2019-11-17 14:10:00'""").df()
    g = np.random.default_rng(0)
    v = df[df.lane == "view"]
    cats = v.category_code.value_counts()
    bots = []
    for i in range(N_BOTS):                  # 봇마다 범주 하나를 골라 그 범주 상품을 일정 간격으로 본다
        cat = g.choice(cats.index.values, p=(cats / cats.sum()).values)
        pool = v[v.category_code == cat]
        step = 60 / BOT_PER_MIN
        ts = df.event_time.min() + np.arange(g.random() * step, 20 * 60, step) * np.timedelta64(1, "s")
        b = pool.sample(len(ts), replace=True, random_state=i).copy()
        b["event_time"], b["user_id"], b["user_session"] = ts, 10**12 + i, f"bot-session-{i}"
        b["event_id"] = [f"bot{i}-{k}" for k in range(len(ts))]
        bots.append(b)
    import pandas as pd
    all_ = pd.concat([df] + bots, ignore_index=True).sort_values("event_time", kind="stable")
    con.register("t", all_)
    con.sql(f"COPY t TO '{out}/day=2019-11-17/data_0.parquet' (FORMAT parquet)")
    print(json.dumps({"rows": len(all_), "bot_rows": sum(len(b) for b in bots)}))


def reference(day: str):
    import duckdb
    rows = duckdb.sql(f"""SELECT user_id, event_id, epoch_ms(event_time) ms, lane, event_type, category_code
        FROM read_parquet('{day}/*.parquet') WHERE lane <> 'purchase' ORDER BY event_time""").fetchall()
    t = HeavyUserTracker()
    kept, est, true = {}, {}, {}
    heavy_users, heavy_views, views = set(), 0, 0
    for uid, eid, ms, lane, et, cat in rows:
        uid = str(uid)
        t.add(uid, ms)
        h = t.is_heavy(uid)
        rate = VIEW_RATE if lane == "view" else 1.0
        k = (f"{et}|{cat or 'etc'}", ms - ms % 60000)
        true[k] = true.get(k, 0) + 1
        if lane == "view":
            views += 1; heavy_views += h
            if h:
                heavy_users.add(uid)
        if keep(uid, eid, rate, h):
            kept[k] = kept.get(k, 0) + 1
            est[k] = est.get(k, 0.0) + weight(rate)
    return kept, est, true, {"heavy_users": sorted(heavy_users), "heavy_view_pct": round(heavy_views / views * 100, 2)}


def run(out: str, speed: str) -> None:
    import verify_flink as vf
    from confluent_kafka import Producer
    day = f"{out}/day=2019-11-17"
    vf.reset()
    p = Producer({"bootstrap.servers": vf.B})
    p.produce("control.rates", key=b"rates", value=json.dumps(
        {"version": int(time.time() * 1000), "cart": 1.0, "view": VIEW_RATE, "at_ms": int(time.time() * 1000)}).encode())
    p.flush()
    vf.submit("main_job", 1); time.sleep(10)
    vf.replay(day, speed)
    agg, _ = vf.collect("agg.minute")
    kept, est, true, info = reference(day)
    got = {(r["key"], r["minute_ms"]): (r["raw_count"], r["est_count"]) for r in agg}
    last = max(m for _, m in kept)
    closed = [k for k in kept if k[1] != last]
    bad = [(k, kept[k], got.get(k)) for k in closed if got.get(k, (None,))[0] != kept[k]]
    est_bad = [k for k in closed if k in got and abs(got[k][1] - est[k]) > 1e-6]
    extra = [k for k in got if k[1] != last and k not in kept]
    bots = [u for u in info["heavy_users"] if u.startswith("1000000000")]
    tot_true = sum(v for k, v in true.items() if k[1] != last and k[0].startswith("view"))
    tot_est = sum(got[k][1] for k in closed if k in got and k[0].startswith("view"))
    print(json.dumps({"speed": speed, "closed": len(closed), "match": len(closed) - len(bad), "est_mismatch": len(est_bad),
                      "extra_keys": len(extra), "bad_sample": bad[:3], "heavy_users": len(info["heavy_users"]),
                      "heavy_bots": len(bots), "heavy_view_pct": info["heavy_view_pct"],
                      "view_total_err_pct": round((tot_est - tot_true) / tot_true * 100, 2)}, ensure_ascii=False))


if __name__ == "__main__":
    {"make": lambda: make(sys.argv[2]), "run": lambda: run(sys.argv[2], sys.argv[3])}[sys.argv[1]]()
