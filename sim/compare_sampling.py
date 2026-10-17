"""
샘플링 방식 비교: 이벤트 단위 무작위 vs 사용자 해시(중첩 임계값)
- 사용자 활동량은 실제 쇼핑몰처럼 소수가 매우 많이 보는 긴 꼬리 분포로 가정
- 지표 1: 1분 조회수 추정 오차 (총량)
- 지표 2: 세션 퍼널 '조회한 세션 중 장바구니까지 간 비율' 추정 오차 (세션 단위 지표)
"""
import numpy as np, json, sys

rng = np.random.default_rng(int(sys.argv[1]) if len(sys.argv) > 1 else 3)
N_USERS = 40_000
act = rng.pareto(1.3, N_USERS) + 1               # 사용자별 활동량 (긴 꼬리)
act = act / act.sum()
user_hash = rng.random(N_USERS)                  # 사용자 ID 해시를 0~1로 본 값
HEAVY_CUT = np.quantile(act, float(sys.argv[2]) if len(sys.argv) > 2 else 0.99)                # 활동량 상위 1% (실제로는 지난 기록이나 근사 집계로 판별)
print("heavy share of views", round(float(act[act >= HEAVY_CUT].sum()), 3))


def one_minute(views_per_min=60_000, cart_prob=0.07):
    # 세션: 사용자별 이번 분 조회 수
    v = rng.multinomial(views_per_min, act)
    active = np.nonzero(v)[0]
    views = v[active]
    carted = rng.random(len(active)) < 1 - (1 - cart_prob) ** np.minimum(views, 20)
    return active, views, carted


def estimate(rate, trials=300):
    res = {"random": {"count": [], "funnel": []}, "user_hash": {"count": [], "funnel": []}, "mixed": {"count": [], "funnel": []}}
    for _ in range(trials):
        active, views, carted = one_minute()
        true_count = views.sum(); true_funnel = carted.mean()
        # 무작위: 조회 이벤트를 각각 rate 확률로 남김, 장바구니는 100%
        kept_views = rng.binomial(views, rate)
        est_count_r = kept_views.sum() / rate
        seen = kept_views > 0
        # '조회한 세션' = 남은 조회가 1개 이상인 세션. 장바구니 세션 중 조회가 다 사라진 것은 분모에서 빠짐
        funnel_r = (carted & seen).sum() / max(seen.sum(), 1)
        # 사용자 해시: 해시 < rate 인 사용자의 모든 조회를 남김
        keep_u = user_hash[active] < rate
        est_count_h = views[keep_u].sum() / rate
        funnel_h = carted[keep_u].mean() if keep_u.any() else 0
        # 혼합: 활동량 상위 1% 사용자는 이벤트 단위, 나머지는 사용자 해시
        heavy = act[active] >= HEAVY_CUT
        kv_h = np.where(heavy, rng.binomial(views, rate), np.where(keep_u, views, 0))
        est_count_m = kv_h[heavy].sum() / rate + kv_h[~heavy].sum() / rate
        m_keep = (~heavy & keep_u) | (heavy & (kv_h > 0))
        # 가중 추정: 해시로 뽑힌 가벼운 사용자는 1/rate배, 무거운 사용자는 전원 관측되므로 1배
        lk = ~heavy & keep_u; hv = heavy & (kv_h > 0)
        num = carted[hv].sum() + carted[lk].sum() / rate
        den = hv.sum() + lk.sum() / rate
        funnel_m = num / den if den > 0 else 0
        res["mixed"]["count"].append(abs(est_count_m - true_count) / true_count)
        res["mixed"]["funnel"].append(funnel_m - true_funnel)
        res["random"]["count"].append(abs(est_count_r - true_count) / true_count)
        res["random"]["funnel"].append(funnel_r - true_funnel)
        res["user_hash"]["count"].append(abs(est_count_h - true_count) / true_count)
        res["user_hash"]["funnel"].append(funnel_h - true_funnel)
    out = {}
    for k, d in res.items():
        out[k] = {"count_err_p95": float(np.percentile(d["count"], 95)),
                  "funnel_bias_pp": float(np.mean(d["funnel"]) * 100),
                  "funnel_err_p95_pp": float(np.percentile(np.abs(d["funnel"]), 95) * 100)}
    return out


print(json.dumps({str(r): estimate(r) for r in [0.5, 0.1, 0.05, 0.02]}, indent=1))
