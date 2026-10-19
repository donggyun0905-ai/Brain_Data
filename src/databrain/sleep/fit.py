"""
수면 재생 2단계: 그날의 판단 기록에서 처리 능력 모형을 되찾는다 (ADR-0014).

제어기는 1초마다 `log.decisions`에 그 순간의 유입 속도·밀림·통과 비율을 남긴다
(`controller/main.py`). 그 기록만으로 다음 두 숫자를 되찾을 수 있다.

    capacity_eps : 무거운 처리 단계가 1초에 할 수 있는 작업량
    filter_cost  : 머리말만 읽고 거르는 판단 하나의 비용 (무거운 처리 1 대비)

ADR-0005는 `filter_cost`를 'E1에서 측정해 기획서 가정(5%)과 비교'하기로 미뤄 두었다.
이 방식이 맞으면 따로 측정하지 않아도 매일 밤 저절로 다시 맞춰진다.

## 어떻게 되찾는가
비용 모형은 시뮬레이터와 같다 (`sim/compare_controllers.py`의 `cost`).

    작업량 = filter_cost × 읽은 수 + 남긴 수

읽은 수는 기록에서 뺄셈으로 나온다. 밀림은 '들어온 것 − 읽은 것'이 쌓인 양이므로

    읽은 수(구간) = 그 구간에 들어온 수 − (구간 끝 밀림 − 구간 시작 밀림)

밀림이 쌓여 있는 동안에는 처리 단계가 늘 꽉 차 있으므로 작업량 = capacity다. 즉

    남긴 수 = capacity − filter_cost × 읽은 수

꽉 찬 구간의 점들을 (읽은 수, 남긴 수) 평면에 놓으면 **기울기 −filter_cost,
세로 절편 capacity인 직선** 위에 모인다. 그 직선을 찾으면 두 숫자가 한 번에 나온다.
통과 비율이 낮을 때는 같은 작업량으로 더 많이 읽으므로 점들이 오른쪽 아래로 퍼진다.
그 퍼짐이 바로 직선을 정할 수 있게 해 주는 정보다.

## 왜 그냥 최소제곱을 쓰지 않는가
작업량은 capacity를 넘을 수 없으므로 모든 점은 직선의 **아래쪽**에 있다.
꽉 차지 않은 순간이 섞이면 직선이 아래로 끌려 내려가 capacity를 낮게 본다.
그래서 읽은 수로 구간을 나눠 각 구간의 위쪽 분위수만 모아 직선을 맞춘다 (위쪽 외피).

## 믿을 수 없을 때는 믿지 않는다
점이 너무 적거나, 읽은 수가 거의 변하지 않아 직선을 정할 수 없거나, 기울기가 양수로
나오면(모형과 어긋남) `ok=False`로 돌려준다. 호출하는 쪽은 그 밤의 교정을 건너뛴다.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Decision:
    """`log.decisions` 한 줄. 측정값인 raw_backlog_*를 쓴다 (추측 항법으로 보정한 값이 아니라)."""
    at_s: float
    in_cart_eps: float
    in_view_eps: float
    backlog_cart: float
    backlog_view: float
    cart: float
    view: float


@dataclass(frozen=True)
class Step:
    """기록 두 줄 사이의 한 구간. 모두 '초당'이 아니라 '그 구간 동안의 수'다."""
    dt_s: float
    read: float
    kept: float
    backlog_start: float
    arrivals: float


@dataclass(frozen=True)
class Fit:
    capacity_eps: float
    filter_cost: float
    n_steps: int          # 쓸 수 있었던 구간 수
    n_saturated: int      # 그중 꽉 찼다고 본 구간 수
    read_spread: float    # 꽉 찬 구간의 읽은 수 최대/최소 (1에 가까우면 직선을 정할 수 없다)
    r2: float
    ok: bool
    reason: str


def steps(rows: list[Decision]) -> list[Step]:
    """연속한 두 기록에서 구간별 (읽은 수, 남긴 수)를 만든다.

    구간 i는 rows[i]와 rows[i+1] 사이다.
    - 들어온 수는 rows[i+1]의 유입 속도를 쓴다 (그 값이 '지난 구간'을 잰 것이기 때문)
    - 적용된 비율은 rows[i]의 것을 쓴다 (제어기가 그 시각에 발행한 비율)
    """
    out: list[Step] = []
    for a, b in zip(rows, rows[1:]):
        dt = b.at_s - a.at_s
        if dt <= 0:
            continue
        for in_eps, bl_a, bl_b, rate in (
                (b.in_cart_eps, a.backlog_cart, b.backlog_cart, a.cart),
                (b.in_view_eps, a.backlog_view, b.backlog_view, a.view)):
            arrivals = max(in_eps, 0.0) * dt
            read = arrivals - (bl_b - bl_a)
            if read < 0:                       # 밀림이 줄어든 양보다 더 줄었다 = 측정 잡음. 버린다
                read = 0.0
            out.append(Step(dt, read, read * rate, bl_a, arrivals))
    # 경로별로 쪼갠 것을 구간별로 합친다 (작업량은 두 경로를 함께 쓴다)
    merged: list[Step] = []
    for i in range(0, len(out), 2):
        c, v = out[i], out[i + 1]
        merged.append(Step(c.dt_s, c.read + v.read, c.kept + v.kept,
                           c.backlog_start + v.backlog_start, c.arrivals + v.arrivals))
    return merged


def _line(pts: list[tuple[float, float]]) -> tuple[float, float, float]:
    """(기울기, 절편, r2). 점이 2개 미만이거나 x가 모두 같으면 기울기 0으로 돌려준다."""
    n = len(pts)
    if n < 2:
        return 0.0, (pts[0][1] if pts else 0.0), 0.0
    mx = sum(x for x, _ in pts) / n
    my = sum(y for _, y in pts) / n
    sxx = sum((x - mx) ** 2 for x, _ in pts)
    if sxx <= 0:
        return 0.0, my, 0.0
    slope = sum((x - mx) * (y - my) for x, y in pts) / sxx
    intercept = my - slope * mx
    sst = sum((y - my) ** 2 for _, y in pts)
    sse = sum((y - (slope * x + intercept)) ** 2 for x, y in pts)
    r2 = 1.0 - sse / sst if sst > 0 else 1.0
    return slope, intercept, r2


def _quantile(xs: list[float], q: float) -> float:
    s = sorted(xs)
    if len(s) == 1:
        return s[0]
    pos = q * (len(s) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (pos - lo)


def fit_capacity(rows: list[Decision], min_backlog_s: float = 1.0, bins: int = 8,
                 q: float = 0.9, min_saturated: int = 30, min_spread: float = 1.2) -> Fit:
    """판단 기록에서 (capacity_eps, filter_cost)를 되찾는다.

    min_backlog_s: 밀림이 '초당 유입의 이 배수' 이상이면 처리 단계가 꽉 찼다고 본다
    bins, q      : 읽은 수로 bins개 구간을 나눠 각 구간의 q분위수로 위쪽 외피를 만든다
    min_saturated, min_spread: 이보다 모자라면 믿지 않는다 (ok=False)
    """
    st = steps(rows)
    # 꽉 찼는지는 '이미 쌓여 있던 것 중 못 읽고 남은 양'으로 본다.
    #   남은 양 = 구간 시작 밀림 - 읽은 수
    # 밀림 값 자체와 유입량을 비교하면 안 된다. 구간 시작 밀림에는 그 구간의 유입이 이미 들어 있어서,
    # 큐를 다 비운 한가한 순간도 '밀림 = 유입'이 되어 꽉 찬 것처럼 보인다 (2026-10-19 테스트로 발견)
    sat = [s for s in st
           if s.read > 0 and (s.backlog_start - s.read) >= min_backlog_s * (s.read / s.dt_s)]
    if len(sat) < min_saturated:
        return Fit(0.0, 0.0, len(st), len(sat), 0.0, 0.0, False,
                   f"꽉 찬 구간이 {len(sat)}개뿐 (필요 {min_saturated})")

    reads = [s.read / s.dt_s for s in sat]
    spread = max(reads) / min(reads) if min(reads) > 0 else float("inf")
    if spread < min_spread:
        return Fit(0.0, 0.0, len(st), len(sat), spread, 0.0, False,
                   f"읽은 수가 거의 일정 (최대/최소 {spread:.2f}) → 직선을 정할 수 없음")

    # 읽은 수 기준 같은 개수씩 구간을 나누고, 각 구간에서 '남긴 수가 위쪽 q번째인 구간 하나'를 집는다.
    # x는 평균, y는 분위수로 따로 집으면 안 된다. 남긴 수는 읽은 수가 적을 때 크므로,
    # 분위수로 집힌 y는 평균보다 작은 x의 것이어서 기울기가 완만해지고 filter_cost가 낮게 나온다
    # (2026-10-19 측정: 정답 0.05 → 0.0456, 0.20 → 0.179). 그래서 짝을 깨지 않고 통째로 집는다
    order = sorted(sat, key=lambda s: s.read / s.dt_s)
    size = max(1, len(order) // bins)
    pts: list[tuple[float, float]] = []
    for i in range(0, len(order), size):
        chunk = order[i:i + size]
        if len(chunk) < 2:
            continue
        by_kept = sorted(chunk, key=lambda s: s.kept / s.dt_s)
        pick = by_kept[min(len(by_kept) - 1, int(q * (len(by_kept) - 1) + 0.5))]
        pts.append((pick.read / pick.dt_s, pick.kept / pick.dt_s))
    slope, intercept, r2 = _line(pts)

    if slope >= 0:
        return Fit(0.0, 0.0, len(st), len(sat), spread, r2, False,
                   f"기울기가 음수여야 하는데 {slope:.4f} → 비용 모형과 어긋남")
    filter_cost = -slope
    if not (0.0 < filter_cost < 1.0) or intercept <= 0:
        return Fit(intercept, filter_cost, len(st), len(sat), spread, r2, False,
                   f"값이 범위를 벗어남 (filter_cost {filter_cost:.3f}, capacity {intercept:.0f})")
    return Fit(intercept, filter_cost, len(st), len(sat), spread, r2, True, "ok")
