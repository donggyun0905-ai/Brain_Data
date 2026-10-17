"""
Data Brain 사전 검증 시뮬레이션 v2 (최종 아키텍처 기준)

v1에서 발견한 것:
 - 도착 즉시 거르는 모델에서는 단계적 조절이 느려 구매 지연 목표(3초)를 못 지킴
 - 거르는 단계가 무거우면(본문 파싱) 오히려 붕괴 (p99 184초)

v2 가정 (실제 구조에 맞춤):
 - 구매는 별도 토픽 + 별도 처리 경로(뇌간: 생명 유지 경로)로 분리 → 조회 폭증과 무관
 - 장바구니·조회는 Kafka에 쌓인 채로 대기하고, Flink가 '읽을 때' 현재 통과 비율로 거름
   → 이미 밀린 데이터도 비율이 낮아지면 빠르게 소화됨
 - 거르는 판단은 헤더/토픽만 보고 하므로 비용이 작음 (filter_cost)
 - 목표: 대시보드 신선도(조회·장바구니 처리 지연) p99 5초 이하, 버리는 데이터 최소화
"""
import numpy as np
from collections import deque
import json, itertools, sys

DT = 0.1
CAP = 10_000                 # 공유 처리 경로 능력 (이벤트/초, 무거운 처리 기준)
PURCHASE_CAP = 1_000         # 구매 전용 경로 능력
FRESH_SLO = 5.0              # 대시보드 신선도 목표 (p99, 초)
TARGET_LAG = 2.0             # 제어기 목표 밀림 (초)
BASE_MIX = np.array([0.015, 0.065, 0.92])   # purchase, cart, view (REES46과 비슷하다고 가정)
VIEW_LEVELS = [1.0, 0.5, 0.25, 0.1, 0.05, 0.02]
CART_LEVELS = [1.0, 0.5, 0.25]


def make_segment(rng, base_rate, bursts, seconds=1200):
    n = int(seconds / DT)
    t = np.arange(n) * DT
    rate = np.tile(base_rate * BASE_MIX, (n, 1))
    rate *= (1 + 0.1 * np.sin(2 * np.pi * t / 600))[:, None]
    for start, dur, mag in bursts:
        ramp = 20.0
        shape = np.minimum(np.clip((t - start) / ramp, 0, 1), np.clip((start + dur - t) / ramp, 0, 1))
        mult = np.array([max(mag / 3, 1), max(mag / 2, 1), mag])
        rate *= 1 + shape[:, None] * (mult - 1)
    return rate


def weighted_p99(vals, ws):
    v = np.asarray(vals); w = np.asarray(ws)
    if len(v) == 0 or w.sum() == 0:
        return 0.0
    o = np.argsort(v); v, w = v[o], w[o]
    return float(v[np.searchsorted(np.cumsum(w) / w.sum(), 0.99)])


def simulate(rate, rng, params=None, mode="adaptive", filter_cost=0.05, record=False):
    p = {"up_thr": 0.75, "step": 1, "recover_s": 30, "period_s": 5}
    if params:
        p.update(params)
    n = len(rate)
    backlog = deque()               # Kafka에 쌓인 (도착시각, 장바구니 수, 조회 수)
    pq = deque(); pq_units = 0.0    # 구매 전용 경로
    vi = ci = 0
    calm_since = None
    lat, lw = [], []                # 조회·장바구니 신선도
    plat, plw = [], []
    m = n // int(60 / DT)
    true_v = np.zeros(m + 1); est_v = np.zeros(m + 1)
    read_in = np.zeros(2); kept_tot = np.zeros(2); arrived = np.zeros(2)
    changes = 0
    trace = []
    period = int(p["period_s"] / DT)
    for k in range(n):
        now = k * DT
        a = rng.poisson(rate[k] * DT).astype(float)
        arrived += a[1:]
        true_v[int(now // 60)] += a[2]
        if a[0] > 0:
            pq.append([now, a[0]]); pq_units += a[0]
        if a[1] + a[2] > 0:
            backlog.append([now, a[1], a[2]])
        # 구매 전용 경로
        b = PURCHASE_CAP * DT
        while b > 1e-9 and pq:
            take = min(b, pq[0][1]); plat.append(now + DT - pq[0][0]); plw.append(take)
            pq[0][1] -= take; b -= take; pq_units -= take
            if pq[0][1] <= 1e-9: pq.popleft()
        # 공유 경로: 읽으면서 거르기
        kc = 1.0 if mode == "none" else CART_LEVELS[ci]
        kv = 1.0 if mode == "none" else VIEW_LEVELS[vi]
        fc = 0.0 if mode == "none" else filter_cost
        budget = CAP * DT
        while budget > 1e-9 and backlog:
            t0, c, v = backlog[0]
            tot = c + v
            cost_per = fc + (c * kc + v * kv) / tot
            can = budget / cost_per
            take = min(can, tot)
            frac = take / tot
            rc, rv = c * frac, v * frac
            kept_c = rng.binomial(int(round(rc)), kc) if rc >= 1 else rc * kc
            kept_v = rng.binomial(int(round(rv)), kv) if rv >= 1 else rv * kv
            read_in += [rc, rv]; kept_tot += [kept_c, kept_v]
            est_v[int(t0 // 60)] += kept_v / kv
            if kept_c + kept_v > 0:
                lat.append(now + DT - t0); lw.append(kept_c + kept_v)
            budget -= take * cost_per
            backlog[0][1] -= rc; backlog[0][2] -= rv
            if take >= tot - 1e-9:
                backlog.popleft()
        # 제어기
        if mode != "none" and k % period == 0:
            q_c = sum(x[1] for x in backlog); q_v = sum(x[2] for x in backlog)
            q = q_c + q_v
            eff = CAP / (fc + (q_c * kc + q_v * kv) / q) if q > 0 else CAP
            lag = q / eff
            old = (vi, ci)
            if mode == "fixed":
                vi = len(VIEW_LEVELS) - 1 if lag > TARGET_LAG else 0
                ci = len(CART_LEVELS) - 1 if lag > TARGET_LAG * 1.5 else 0
            else:
                if lag > TARGET_LAG * p["up_thr"]:
                    calm_since = None
                    for _ in range(p["step"]):
                        if vi < len(VIEW_LEVELS) - 1: vi += 1
                        elif ci < len(CART_LEVELS) - 1: ci += 1
                elif lag < TARGET_LAG * 0.25:
                    calm_since = now if calm_since is None else calm_since
                    if now - calm_since >= p["recover_s"]:
                        if ci > 0: ci -= 1
                        elif vi > 0: vi -= 1
                        calm_since = now
                else:
                    calm_since = None
            changes += (vi, ci) != old
            if record:
                trace.append([round(now, 1), round(lag, 3), VIEW_LEVELS[vi], CART_LEVELS[ci], round(float(rate[k].sum()))])
    full = int(n * DT // 60)
    err = np.abs(est_v[:full] - true_v[:full]) / np.maximum(true_v[:full], 1)
    res = {
        "fresh_p99": weighted_p99(lat, lw),
        "purchase_p99": weighted_p99(plat, plw),
        "view_drop": float(1 - kept_tot[1] / max(read_in[1], 1)),
        "cart_drop": float(1 - kept_tot[0] / max(read_in[0], 1)),
        "unread_left": float(sum(x[1] + x[2] for x in backlog)),
        "view_err_total": float(abs(est_v[:full].sum() - true_v[:full].sum()) / true_v[:full].sum()),
        "view_err_minute_max": float(err.max()),
        "changes": int(changes),
    }
    if record:
        res["trace"] = trace
    return res


def score(r):
    pen = max(0.0, r["fresh_p99"] - FRESH_SLO) / FRESH_SLO * 5
    return r["view_drop"] + 0.3 * r["cart_drop"] + pen + 0.002 * r["changes"]


GRID = {"up_thr": [0.5, 0.75, 1.0], "step": [1, 2], "recover_s": [10, 30, 60], "period_s": [1, 2, 5]}


def neighbors(cur):
    keys = list(GRID); idx = {k: GRID[k].index(cur[k]) for k in keys}
    out = [dict(cur)]                    # 하룻밤에 설정 하나만, 한 칸만 바꿀 수 있음 (변경 폭 상한)
    for key in keys:
        for dk in (-1, 1):
            j = idx[key] + dk
            if 0 <= j < len(GRID[key]):
                cand = dict(cur); cand[key] = GRID[key][j]; out.append(cand)
    return out


def random_day(rng, big=False):
    if big:
        return [(120, 600, 10)]
    bursts, t = [], 120
    for _ in range(rng.integers(1, 3)):
        mag, dur = rng.uniform(4, 8), rng.uniform(60, 240)
        bursts.append((t, dur, mag)); t += dur + rng.uniform(150, 300)
    return bursts


def run(seed):
    rng = np.random.default_rng(seed)
    base = 6000
    out = {}
    seg = make_segment(rng, base, [(120, 180, 10)])
    for mode in ["none", "fixed", "adaptive"]:
        out[f"burst_{mode}"] = simulate(seg, np.random.default_rng(seed + 1), mode=mode)
    out["burst_adaptive_heavyfilter"] = simulate(seg, np.random.default_rng(seed + 1), mode="adaptive", filter_cost=0.4)
    out["trace"] = simulate(seg, np.random.default_rng(seed + 1), mode="adaptive", record=True)["trace"]
    oct_days = [make_segment(rng, base, random_day(rng)) for _ in range(14)]
    nov_days = [make_segment(rng, base, random_day(rng)) for _ in range(6)] + [make_segment(rng, base, random_day(rng, True))]
    cur = {"up_thr": 1.0, "step": 1, "recover_s": 10, "period_s": 5}   # 첫날 설정 (의도적으로 평범하게)
    hist, memory, nightly = [dict(cur)], [], []
    for d, day in enumerate(oct_days):
        memory = (memory + [day])[-3:]
        best, best_s = cur, None
        for cand in neighbors(cur):
            s = float(np.mean([score(simulate(mm, np.random.default_rng(100 + i), cand)) for i, mm in enumerate(memory)]))
            if best_s is None or s < best_s - 1e-6:
                best, best_s = cand, s
        cur = best; hist.append(dict(cur)); nightly.append(best_s)

    def evaluate(params):
        rs = [simulate(mm, np.random.default_rng(500 + i), params) for i, mm in enumerate(nov_days)]
        return {
            "fresh_p99_mean": float(np.mean([r["fresh_p99"] for r in rs])),
            "fresh_p99_max": float(max(r["fresh_p99"] for r in rs)),
            "slo_violations": int(sum(r["fresh_p99"] > FRESH_SLO for r in rs)),
            "view_drop_mean": float(np.mean([r["view_drop"] for r in rs])),
            "cart_drop_mean": float(np.mean([r["cart_drop"] for r in rs])),
            "changes_mean": float(np.mean([r["changes"] for r in rs])),
            "view_err_total_max": float(max(r["view_err_total"] for r in rs)),
            "view_err_minute_max": float(max(r["view_err_minute_max"] for r in rs)),
            "purchase_p99_max": float(max(r["purchase_p99"] for r in rs)),
            "score_mean": float(np.mean([score(r) for r in rs])),
        }
    out["tuning_history"] = hist
    out["nightly_score"] = nightly
    out["nov_day1"] = evaluate(hist[0])
    out["nov_tuned"] = evaluate(cur)
    nn = [simulate(mm, np.random.default_rng(500 + i), mode="none") for i, mm in enumerate(nov_days)]
    out["nov_none_fresh_p99_max"] = float(max(r["fresh_p99"] for r in nn))
    return out


if __name__ == "__main__":
    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    res = run(seed)
    json.dump(res, open(f"v2_seed{seed}.json", "w"), ensure_ascii=False, default=float)
    res.pop("trace")
    print(json.dumps(res, ensure_ascii=False, indent=1, default=float))
