"""혼합 샘플링 상위 사용자 기준 비교 (ADR-0010). 실제 데이터 1시간을 여러 기준으로 거른 뒤 정답과 비교한다.

    python tools/compare_heavy.py data/parquet/2019-11/day=2019-11-17 14     # 11/17 14시 (폭증)
    python tools/compare_heavy.py data/parquet/2019-11/day=2019-11-17 14 5 10   # 가짜 봇 5개, 각 분당 10건 추가

- 가짜 봇: 실제 데이터에는 봇처럼 몰아 보는 사용자가 없어서 안전장치를 시험하려고 넣는다 (ADR-0011).
  봇마다 조회 많은 범주 하나를 골라(조회 수 비례) 1시간 내내 일정 간격으로 그 범주 상품을 본다. 세션은 봇당 하나
- 봇 범주·분 p95 : 봇이 본 (범주, 1분) 칸만 따로 본 오차 (봇을 넣었을 때만)

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


def add_bots(df, n_bots: int, per_min: float, seed: int = 0):
    """가짜 봇 조회를 넣는다. 반환: (새 df, 봇 행 표시)."""
    import pandas as pd
    g = np.random.default_rng(seed)
    v = df[df.lane == "view"]
    cats = v.category_code.value_counts()
    t0, t1 = int(df.ms.min()), int(df.ms.max())
    rows = []
    for i in range(n_bots):
        cat = g.choice(cats.index.values, p=(cats / cats.sum()).values)
        prods = v.product_id[v.category_code == cat].unique()
        step = 60_000 / per_min
        ts = np.arange(t0 + g.random() * step, t1, step).astype(np.int64)
        rows.append(pd.DataFrame({"user_id": df.user_id.dtype.type(10**12 + i), "user_session": f"bot-session-{i}", "ms": ts, "lane": "view",
                                  "category_code": cat, "product_id": g.choice(prods, len(ts))}))
    out = pd.concat([df.assign(bot=False)] + [r.assign(bot=True) for r in rows], ignore_index=True)
    out = out.sort_values("ms", kind="stable", ignore_index=True)
    return out.drop(columns="bot"), out.bot.values


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


def evaluate(df, u, mi, view, heavy, seeds, bot=None):
    s = np.unique(df.user_session.astype(str).values, return_inverse=True)[1]
    cat = np.unique(df.category_code.values, return_inverse=True)[1]
    NM, NS, NU = mi.max() + 1, s.max() + 1, u.max() + 1
    vi = np.flatnonzero(view); h = heavy[vi]
    cell = (cat * NM + mi)[vi]; tc = np.bincount(cell); big = np.flatnonzero(tc >= 100)
    bcell = np.unique(cell[bot[vi]]) if bot is not None else None
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
        em, ec, ef, part, whole, path, dwell, prior, eb = [], [], [], [], [], [], [], [], []
        for sd in seeds:
            g = np.random.default_rng(sd)
            k = np.where(h, g.random(len(vi)) < r, g.random(NU)[u[vi]] < r)
            em.append(np.abs(np.bincount(mi[vi][k], minlength=NM) / r - tm) / tm)
            ce = np.abs(np.bincount(cell[k], minlength=len(tc)) / r - tc) / np.maximum(tc, 1)
            ec.append(ce[big])
            if bcell is not None:
                eb.append(ce[bcell])
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
                  np.mean(path) * 100, np.mean(dwell) * 100, np.mean(prior) * 100,
                  np.percentile(np.concatenate(eb), 95) * 100 if eb else float("nan"))
    return out


def main(day: str, hour: int, n_bots: int = 0, per_min: float = 0) -> None:
    df, bot = load(day, hour), None
    if n_bots:
        df, bot = add_bots(df, n_bots, per_min)
    u, mi, view, cs = criteria(df)
    groups = [range(o, o + 10) for o in (0, 100, 200)]
    report = {}
    for name, heavy in cs.items():
        runs = [evaluate(df, u, mi, view, heavy, g, bot) for g in groups]
        report[name] = {"상위 판정 조회%": round(heavy[view].mean() * 100, 1)}
        if bot is not None:                  # 봇 조회 중 이벤트 단위로 넘어간 비율
            report[name]["봇 판정%"] = round(heavy[bot].mean() * 100, 1)
        for r in RATES:
            cols = zip(*(run[r] for run in runs))
            med = [sorted(x) for x in cols]
            report[name][str(r)] = {m: [round(v[1], 1), round(v[0], 1), round(v[2], 1)] for m, v in
                                    zip(("분 총량 p95%", "범주·분 p95%", "퍼널 p95%p", "쪼개진 세션%",
                                                  "세션 통째 보존%", "경로 보존%", "체류 시간 틀림%", "담기 전 조회%",
                                                  "봇 범주·분 p95%"), med) if not np.isnan(v[1])}
    print(json.dumps({"day": day, "hour": hour, "bots": [n_bots, per_min], "events": len(df), "views": int(view.sum()),
                      "res": report}, ensure_ascii=False))


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]), *(int(sys.argv[3]), float(sys.argv[4])) if len(sys.argv) > 4 else ())
