"""
예측형 + 되먹임 제어기의 핵심 계산 (ADR-0002).

사전 검증에서 단계형(신선도 p99 1.9초)·AIMD(흔들림 2배)보다 예측형(0.1초)이 나았고,
처리 능력을 ±50% 잘못 추정해도 목표(5초) 안이었다 (sim/compare_controllers.py).

계산:
  필요한 읽기 속도 R = (장바구니 유입 + 조회 유입) + (밀린 양 / 목표 밀림 시간)
  읽은 이벤트 하나에 쓸 수 있는 처리량 per = 여유율 × 처리 능력 / R - 거르기 비용
  per = 장바구니 비중 × 장바구니 비율 + 조회 비중 × 조회 비율 이 되도록 비율을 정한다.
  조회부터 줄이고, 조회가 최소에 닿으면 장바구니를 줄인다.

수면 재생이 밤마다 고치는 값은 CapacityModel의 숫자들이다 (처리 능력, 여유율, 올리는 속도).
"""
from __future__ import annotations

from dataclasses import dataclass, replace


@dataclass(frozen=True)
class CapacityModel:
    capacity_eps: float            # 무거운 처리 단계가 1초에 처리할 수 있는 이벤트 수 (E1에서 측정)
    filter_cost: float = 0.05      # 거르는 판단 하나의 비용 (무거운 처리 1 대비)
    margin: float = 0.9            # 처리 능력 중 실제로 쓸 비율 (여유 10%)
    target_lag_s: float = 2.0      # 밀린 양을 이 시간 안에 비우는 것이 목표
    min_view: float = 0.02
    min_cart: float = 0.25
    max_rise: float = 0.10         # 한 번에 올릴 수 있는 최대 폭 (흔들림 방지)


@dataclass(frozen=True)
class Rates:
    cart: float = 1.0
    view: float = 1.0
    version: int = 0


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def compute_rates(m: CapacityModel, in_cart_eps: float, in_view_eps: float,
                  backlog_cart: float, backlog_view: float, prev: Rates) -> Rates:
    need_cart = max(in_cart_eps, 0.0) + max(backlog_cart, 0.0) / m.target_lag_s
    need_view = max(in_view_eps, 0.0) + max(backlog_view, 0.0) / m.target_lag_s
    R = need_cart + need_view
    if R <= 0:
        cart, view = 1.0, 1.0
    else:
        sc = need_cart / R
        sv = 1.0 - sc
        per = m.margin * m.capacity_eps / R - m.filter_cost
        want_view = (per - sc * 1.0) / sv if sv > 0 else 1.0
        if want_view >= m.min_view:
            cart, view = 1.0, _clip(want_view, m.min_view, 1.0)
        else:
            view = m.min_view
            want_cart = (per - sv * m.min_view) / sc if sc > 0 else 1.0
            cart = _clip(want_cart, m.min_cart, 1.0)
    # 내릴 때는 즉시, 올릴 때는 천천히
    if view > prev.view:
        view = min(view, prev.view + m.max_rise)
    if cart > prev.cart:
        cart = min(cart, prev.cart + m.max_rise)
    changed = abs(view - prev.view) > 1e-3 or abs(cart - prev.cart) > 1e-3
    return replace(prev, cart=round(cart, 4), view=round(view, 4),
                   version=prev.version + 1 if changed else prev.version)
