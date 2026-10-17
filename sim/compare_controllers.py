"""
제어 알고리즘 비교: 단계형(현재) vs AIMD vs 예측형(피드포워드+피드백)
v2와 같은 구조(구매 분리, Kafka에 쌓인 채로 읽을 때 거름, 헤더 판단)를 사용.
"""
import sys, json
import numpy as np
from collections import deque
sys.path.insert(0, sys.argv[2] if len(sys.argv) > 2 else ".")
from databrain_sim_v2 import make_segment, random_day, weighted_p99, DT, CAP, FRESH_SLO, TARGET_LAG, VIEW_LEVELS, CART_LEVELS

FC = 0.05


def sim(rate, rng, algo, period_s=2, cap_est=1.0):
    n = len(rate)
    backlog = deque()
    kc, kv = 1.0, 1.0
    vi = ci = 0
    calm = None
    lat, lw = [], []
    read_in = np.zeros(2); kept = np.zeros(2)
    m = n // int(60 / DT)
    true_v = np.zeros(m + 1); est_v = np.zeros(m + 1)
    changes = 0
    period = int(period_s / DT)
    arr_hist = deque(maxlen=int(1 / DT))
    for k in range(n):
        now = k * DT
        a = rng.poisson(rate[k] * DT).astype(float)
        arr_hist.append(a[1:])
        true_v[int(now // 60)] += a[2]
        if a[1] + a[2] > 0:
            backlog.append([now, a[1], a[2]])
        budget = CAP * DT
        while budget > 1e-9 and backlog:
            t0, c, v = backlog[0]
            tot = c + v
            cost = FC + (c * kc + v * kv) / tot
            take = min(budget / cost, tot)
            f = take / tot
            rc, rv = c * f, v * f
            kcc = rng.binomial(int(round(rc)), kc) if rc >= 1 else rc * kc
            kvv = rng.binomial(int(round(rv)), kv) if rv >= 1 else rv * kv
            read_in += [rc, rv]; kept += [kcc, kvv]
            est_v[int(t0 // 60)] += kvv / kv
            if kcc + kvv > 0:
                lat.append(now + DT - t0); lw.append(kcc + kvv)
            budget -= take * cost
            backlog[0][1] -= rc; backlog[0][2] -= rv
            if take >= tot - 1e-9:
                backlog.popleft()
        if k % period == 0:
            qc = sum(x[1] for x in backlog); qv = sum(x[2] for x in backlog); q = qc + qv
            eff = CAP / (FC + (qc * kc + qv * kv) / q) if q > 0 else CAP
            lag = q / eff
            old = (round(kc, 3), round(kv, 3))
            if algo == "step":
                if lag > TARGET_LAG:
                    calm = None
                    if vi < len(VIEW_LEVELS) - 1: vi += 1
                    elif ci < len(CART_LEVELS) - 1: ci += 1
                elif lag < TARGET_LAG * 0.25:
                    calm = now if calm is None else calm
                    if now - calm >= 10:
                        if ci > 0: ci -= 1
                        elif vi > 0: vi -= 1
                        calm = now
                else:
                    calm = None
                kc, kv = CART_LEVELS[ci], VIEW_LEVELS[vi]
            elif algo == "aimd":           # TCP 혼잡 제어 방식: 곱셈으로 줄이고 덧셈으로 늘림
                if lag > TARGET_LAG:
                    if kv > 0.02: kv = max(0.02, kv * 0.5)
                    else: kc = max(0.25, kc * 0.5)
                elif lag < TARGET_LAG * 0.25:
                    if kc < 1: kc = min(1.0, kc + 0.05)
                    else: kv = min(1.0, kv + 0.05)
            elif algo == "ff":             # 예측형: 지금 유입률과 밀린 양으로 필요한 비율을 직접 계산
                arr = np.sum(arr_hist, axis=0) / (len(arr_hist) * DT)
                rc_in, rv_in = arr
                R = rc_in + rv_in + q / TARGET_LAG             # 밀린 것을 목표 시간 안에 비우려면 필요한 읽기 속도
                if R <= 0:
                    kc, kv = 1.0, 1.0
                else:
                    sc = (rc_in + qc / TARGET_LAG) / R; sv = 1 - sc
                    per = 0.9 * CAP * cap_est / R - FC                      # 읽은 이벤트 하나당 쓸 수 있는 처리량 (여유 10%)
                    want_v = (per - sc * 1.0) / max(sv, 1e-9)
                    if want_v >= 0.02:
                        kc, kv = 1.0, float(np.clip(want_v, 0.02, 1.0))
                    else:
                        kv = 0.02
                        kc = float(np.clip((per - sv * 0.02) / max(sc, 1e-9), 0.25, 1.0))
                    # 급하게 올리지 않도록 상승은 천천히 (흔들림 방지)
                    kv = min(kv, old[1] + 0.1) if kv > old[1] else kv
            if abs(kv - old[1]) > 0.01 or abs(kc - old[0]) > 0.01:
                changes += 1
    full = int(n * DT // 60)
    err = np.abs(est_v[:full] - true_v[:full]) / np.maximum(true_v[:full], 1)
    return {"fresh_p99": weighted_p99(lat, lw), "view_drop": float(1 - kept[1] / max(read_in[1], 1)),
            "cart_drop": float(1 - kept[0] / max(read_in[0], 1)), "changes": changes,
            "err_minute_max": float(err.max())}


if __name__ == "__main__":
    seed = int(sys.argv[1])
    rng = np.random.default_rng(seed)
    base = 6000
    burst = make_segment(rng, base, [(120, 180, 10)])
    days = [make_segment(rng, base, random_day(rng)) for _ in range(6)] + [make_segment(rng, base, random_day(rng, True))]
    out = {}
    if len(sys.argv) > 3:
        for ce in [0.8, 1.2, 1.5]:
            ds = [sim(d, np.random.default_rng(900 + i), "ff", cap_est=ce) for i, d in enumerate(days)]
            b = sim(burst, np.random.default_rng(seed + 1), "ff", cap_est=ce)
            out[f"ff_cap{ce}"] = {"burst": b, "days_fresh_p99_max": float(max(d["fresh_p99"] for d in ds)),
                                  "days_view_drop_mean": float(np.mean([d["view_drop"] for d in ds]))}
        print(json.dumps(out, indent=1)); sys.exit()
    for algo in ["step", "aimd", "ff"]:
        b = sim(burst, np.random.default_rng(seed + 1), algo)
        ds = [sim(d, np.random.default_rng(900 + i), algo) for i, d in enumerate(days)]
        out[algo] = {"burst": b,
                     "days_fresh_p99_mean": float(np.mean([d["fresh_p99"] for d in ds])),
                     "days_fresh_p99_max": float(max(d["fresh_p99"] for d in ds)),
                     "days_view_drop_mean": float(np.mean([d["view_drop"] for d in ds])),
                     "days_cart_drop_mean": float(np.mean([d["cart_drop"] for d in ds])),
                     "days_changes_mean": float(np.mean([d["changes"] for d in ds])),
                     "days_err_max": float(max(d["err_minute_max"] for d in ds))}
    print(json.dumps(out, indent=1))
