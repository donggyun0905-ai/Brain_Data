"""
문제 주입: 중복, 지연 도착. 무엇을 주입했는지 '정답'을 함께 돌려준다 (Q1·Q2 실험).

events는 (보낼 시각초, 이벤트 dict) 목록이다. 이벤트 dict에는 'event_id'가 있어야 한다.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass
class InjectionLog:
    duplicated: list[str] = field(default_factory=list)
    delayed: list[tuple[str, float]] = field(default_factory=list)   # (event_id, 늦춘 초)


def inject(events: list[tuple[float, dict]], dup_rate: float = 0.0, delay_rate: float = 0.0,
           delay_range_s: tuple[float, float] = (60.0, 600.0), seed: int = 0):
    rng = random.Random(seed)
    log = InjectionLog()
    out: list[tuple[float, dict]] = []
    for t, ev in events:
        eid = str(ev["event_id"])
        if delay_rate and rng.random() < delay_rate:
            d = rng.uniform(*delay_range_s)
            out.append((t + d, ev)); log.delayed.append((eid, d))
        else:
            out.append((t, ev))
        if dup_rate and rng.random() < dup_rate:
            out.append((t + rng.uniform(0.0, 1.0), ev)); log.duplicated.append(eid)
    out.sort(key=lambda x: x[0])
    return out, log
