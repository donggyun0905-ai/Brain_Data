"""
재생 시간표: 데이터 속 시간(이벤트 발생 시각)을 실제 보낼 시각으로 바꾼다.

- speed: 기본 배속 (100이면 데이터 100초가 실제 1초)
- bursts: [(데이터 시각 시작초, 데이터 시각 길이초, 배수)] 이 구간은 배속 × 배수로 더 빨리 보낸다
  → 같은 데이터가 짧은 시간에 몰려 들어와 '폭증'이 된다 (타임딜 재현)

수면 재생에서도 같은 함수를 써서 '원래 도착 간격 그대로' 다시 보낸다 (설계 원칙 8).
"""
from __future__ import annotations

from bisect import bisect_right
from typing import Sequence


def build_wall_clock(speed: float, bursts: Sequence[tuple[float, float, float]] = ()):
    """데이터 시각(첫 이벤트 기준 초) → 실제 보낼 시각(초) 함수를 돌려준다. 단조 증가."""
    if speed <= 0:
        raise ValueError("speed는 0보다 커야 함")
    edges = sorted({0.0} | {s for s, _, _ in bursts} | {s + d for s, d, _ in bursts})

    def factor_at(t: float) -> float:
        f = 1.0
        for s, d, m in bursts:
            if s <= t < s + d:
                f = max(f, m)
        return f

    # 구간별 누적 실제 시간
    cum = [0.0]
    for a, b in zip(edges, edges[1:]):
        cum.append(cum[-1] + (b - a) / (speed * factor_at(a)))

    def wall(t: float) -> float:
        if t <= 0:
            return 0.0
        i = bisect_right(edges, t) - 1
        base = edges[i]
        return cum[i] + (t - base) / (speed * factor_at(base))

    return wall
