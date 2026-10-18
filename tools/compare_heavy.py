"""혼합 샘플링 상위 사용자 기준 비교 (ADR-0010). 실제 데이터 1시간을 여러 기준으로 거른 뒤 정답과 비교한다.

    python tools/compare_heavy.py data/parquet/2019-11/day=2019-11-17 14     # 11/17 14시 (폭증)

- 조회만 거르고 장바구니는 모두 남긴다고 본다. 시드 10개씩 3묶음 → 중앙값(범위)
- 분 총량 p95     : 1분 조회 수 추정 오차의 95번째 백분위
- 범주·분 p95     : (범주, 1분) 칸 중 실제 100건 이상인 칸의 오차 95번째 백분위 (대시보드 단위)
- 퍼널 p95        : 세션 전환율(조회 세션 중 장바구니 세션) 추정 오차. 세션이 남을 확률의 역수로 가중
- 쪼개진 세션     : 조회 일부만 남은 세션 비율 (경로·체류 같은 세션 분석이 깨지는 양)
세션 지표 (조회가 하나라도 남은 세션만 본다. 분석가가 실제로 보게 되는 세션)
- 세션 통째 보존  : 조회가 전부 남은 세션 비율
- 경로 보존       : 실제로 연달아 본 조회 두 개(A→B)가 둘 다 남아 그대로 보이는 비율
- 체류 시간 틀림  : (남은 마지막 조회 - 남은 첫 조회)가 실제 체류 시간과 10% 넘게 다른 세션 비율 (조회 2개 이상 세션)
- 담기 전 조회    : 장바구니에 담은 상품을 담기 전에 본 기록이 실제로 있을 때, 그 조회가 남아 보이는 비율
"""
from __future__ import annotations

import json
import sys

import numpy as np

sys.path.insert(0, "src")
from databrain.sampling.heavy import HeavyUserTracker  # noqa: E402

RATES = (0.5, 0.2, 0.1, 0.05)


def load(day: str, hour: int):
    import duckdb
    df = duckdb.sql(f"""SELECT user_id, user_session, epoch_ms(event_time) ms, lane, category_code, product_id
        FROM read_parquet('{day}/*.parquet') WHERE hour(event_time) = {hour} AND lane <> 'purchase'
        ORDER BY event_time""").df()
    return df


def criteria(df):
    N = len(df)
    uid, u = np.unique(df.user_id.values, return_inverse=True)
    ms = df.ms.values.astype(np.int64)
    mi = ms // 60000 - ms.min() // 60000
    view = df.lane.values == "view"

    def prefix(group):                      # 같은 묶음 안에서 몇 번째 이벤트인지 (자기 포함)
        o = np.lexsort((np.arange(N), group)); g = group[o]
        st = np.r_[0, np.flatnonzero(np.diff(g)) + 1]
        out = np.empty(N, np.int64); out[o] = np.arange(N) - np.repeat(st, np.diff(np.r_[st, N])) + 1
        return out

    def tracker(tau_min, k):                # 실제 코드 (sampling/heavy.py) 그대로
        t = HeavyUserTracker(tau_ms=tau_min * 60_000, threshold=k); out = np.empty(N, bool)
        for i, (uu, m) in enumerate(zip(df.user_id.values, ms)):
            t.add(str(uu), int(m)); out[i] = t.is_heavy(str(uu))
        return out

    in_min, in_hour = prefix(u * (mi.max() + 1) + mi), prefix(u)
    hv = np.bincount(u, weights=view)
    c = {"사용자 해시만": np.zeros(N, bool), "이벤트 무작위만": np.ones(N, bool),
         "시간 상위 1% (미래를 앎)": (hv >= np.quantile(hv[hv > 0], 0.99))[u],
         "이번 분 3건째부터": in_min >= 3, "이번 분 5건째부터": in_min >= 5,
         "시간 누적 20건째부터": in_hour >= 20, "시간 누적 40건째부터": in_hour >= 40}
    for tau, k in ((10, 5), (10, 10), (30, 10), (30, 20), (30, 30), (30, 40), (30, 50), (30, 75), (30, 100), (60, 40)):
        c[f"감쇠 {tau}분 {k}건 이상"] = tracker(tau, k)
    return u, mi, view, c


def evaluate(df, u, mi, view, heavy, seeds):
    s = np.unique(df.user_session.astype(str).values, return_inverse=True)[1]
    cat = np.unique(df.category_code.values, return_inverse=True)[1]
    NM, NS, NU = mi.max() + 1, s.max() + 1, u.max() + 1
    vi = np.flatnonzero(view); h = heavy[vi]
    cell = (cat * NM + mi)[vi]; tc = np.bincount(cell); big = np.flatnonzero(tc >= 100)
    tm = np.bincount(mi[vi], minlength=NM)
    sv = np.bincount(s[vi], minlength=NS); s_cart = np.bincount(s[~view], minlength=NS) > 0
    true_f = s_cart[sv > 0].mean()
    nh = np.bincount(s[vi], weights=h, minlength=NS); nl = sv - nh
    # 세션 지표 준비: 세션·시각 순으로 줄 세운 조회
    so = vi[np.lexsort((df.ms.values[vi], s[vi]))]
    pos_in_vi = np.empty(len(df), np.int64); pos_in_vi[vi] = np.arange(len(vi))
    pair = s[so][1:] == s[so][:-1]                       # 같은 세션 안에서 연달아 본 두 조회
    a, b = pos_in_vi[so][:-1][pair], pos_in_vi[so][1:][pair]
    pair_s = s[so][1:][pair]
    msv = df.ms.values[vi].astype(np.int64)
    t_first = np.full(NS, np.iinfo(np.int64).max); np.minimum.at(t_first, s[vi], msv)
    t_last = np.full(NS, -1); np.maximum.at(t_last, s[vi], msv)
    import pandas as pd
    ev = pd.DataFrame({"s": s, "p": df.product_id.values, "ms": df.ms.values, "i": np.arange(len(df))})
    cj = ev[~view].merge(ev.iloc[vi], on=["s", "p"], suffixes=("_c", "_v"))
    cj = cj[cj.ms_v <= cj.ms_c]
    cart_id = np.unique(cj.i_c.values, return_inverse=True)[1]; cj_v = pos_in_vi[cj.i_v.values]
    cj_s = s[cj.i_c.values]
    out = {}
    for r in RATES:
        pi = 1 - (1 - r) ** nh * (1 - r * (nl > 0))          # 세션이 하나라도 남을 확률
        em, ec, ef, part, whole, path, dwell, prior = [], [], [], [], [], [], [], []
        for sd in seeds:
            g = np.random.default_rng(sd)
            k = np.where(h, g.random(len(vi)) < r, g.random(NU)[u[vi]] < r)
            em.append(np.abs(np.bincount(mi[vi][k], minlength=NM) / r - tm) / tm)
            ec.append((np.abs(np.bincount(cell[k], minlength=len(tc)) / r - tc) / np.maximum(tc, 1))[big])
            ks = np.bincount(s[vi][k], minlength=NS); seen = ks > 0; w = 1 / pi[seen]
            ef.append(abs((w * s_cart[seen]).sum() / w.sum() - true_f))
            part.append(((ks > 0) & (ks < sv))[sv > 0].mean())
            whole.append((ks == sv)[seen].mean())
            ps = seen[pair_s]; path.append((k[a] & k[b])[ps].mean())
            kf = np.full(NS, np.iinfo(np.int64).max); np.minimum.at(kf, s[vi][k], msv[k])
            kl = np.full(NS, -1); np.maximum.at(kl, s[vi][k], msv[k])
            td = (t_last - t_first).astype(float); m = seen & (sv >= 2) & (td > 0)
            dwell.append((np.abs((kl - kf)[m] - td[m]) / td[m] > 0.1).mean())
            hit = np.zeros(cart_id.max() + 1, bool); np.logical_or.at(hit, cart_id, k[cj_v])
            cs = np.zeros(cart_id.max() + 1, bool); cs[cart_id] = seen[cj_s]
            prior.append(hit[cs].mean())
        out[r] = (np.percentile(np.concatenate(em), 95) * 100, np.percentile(np.concatenate(ec), 95) * 100,
                  np.percentile(ef, 95) * 100, np.mean(part) * 100, np.mean(whole) * 100,
                  np.mean(path) * 100, np.mean(dwell) * 100, np.mean(prior) * 100)
    return out


def main(day: str, hour: int) -> None:
    df = load(day, hour)
    u, mi, view, cs = criteria(df)
    groups = [range(o, o + 10) for o in (0, 100, 200)]
    report = {}
    for name, heavy in cs.items():
        runs = [evaluate(df, u, mi, view, heavy, g) for g in groups]
        report[name] = {"상위 판정 조회%": round(heavy[view].mean() * 100, 1)}
        for r in RATES:
            cols = zip(*(run[r] for run in runs))
            med = [sorted(x) for x in cols]
            report[name][str(r)] = {m: [round(v[1], 1), round(v[0], 1), round(v[2], 1)] for m, v in
                                    zip(("분 총량 p95%", "범주·분 p95%", "퍼널 p95%p", "쪼개진 세션%",
                                                  "세션 통째 보존%", "경로 보존%", "체류 시간 틀림%", "담기 전 조회%"), med)}
    print(json.dumps({"day": day, "hour": hour, "events": len(df), "views": int(view.sum()), "res": report},
                     ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]))
