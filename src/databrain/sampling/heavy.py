"""
활동량 상위 사용자 판별 (ADR-0010, 기준값은 ADR-0011).

사용자마다 '시간이 지나면 줄어드는 건수'를 정확히 센다 (지수 감쇠, 이벤트 시각 기준).
- 한 건이 들어오면 +1, 시간이 tau만큼 지나면 약 37%(1/e)로 줄어든다
- 감쇠 건수가 threshold 이상이면 상위 사용자 → 이벤트 단위로 거름
- 기본값 tau 30분, 기준 50건 = 꾸준히 시간당 약 100건 이상 보는 사용자 (실측: 시간 상위 1%가 32~49건, 최대 195건)
  봇처럼 극단적인 사용자만 잡는 안전장치. 기준이 낮으면 세션 경로가 끊기고 정확도 이득은 거의 없다

이전 방식(Count-Min Sketch, '전체의 0.1%', 10만 건마다 절반)은 버렸다.
기준이 필터 병렬도와 처리량에 따라 바뀌었기 때문 (병렬 4에서 상위 판정이 4~6배로 늘어남).
이벤트 시각을 쓰므로 배속·따라잡기와 무관하게 같은 판정이 나온다.
"""
from __future__ import annotations

import math


class HeavyUserTracker:
    def __init__(self, tau_ms: float = 30 * 60_000, threshold: float = 50.0,
                 prune_every_ms: float = 5 * 60_000, prune_below: float = 0.05):
        """
        tau_ms: 감쇠 시간 상수. threshold: 상위로 보는 감쇠 건수.
        prune_below: 이 값보다 작아진 사용자는 지운다 (1건이 0.05가 되려면 tau의 약 3배, 90분)
        """
        self.tau, self.threshold = tau_ms, threshold
        self.prune_every, self.prune_below = prune_every_ms, prune_below
        self.users: dict[str, tuple[float, int]] = {}     # user_id → (감쇠 건수, 마지막 이벤트 시각 ms)
        self._last_prune: int | None = None

    def _value(self, user_id: str, now_ms: int) -> float:
        v, t = self.users.get(user_id, (0.0, now_ms))
        return v * math.exp(-max(0, now_ms - t) / self.tau)   # 늦게 온 이벤트(시각이 과거)는 감쇠 없이

    def add(self, user_id: str, event_ms: int) -> float:
        _, t = self.users.get(user_id, (0.0, event_ms))
        v = self._value(user_id, event_ms) + 1.0
        self.users[user_id] = (v, max(t, event_ms))
        if self._last_prune is None:
            self._last_prune = event_ms
        elif event_ms - self._last_prune >= self.prune_every:
            self.prune(event_ms)
        return v

    def is_heavy(self, user_id: str) -> bool:
        """add 직후에 부른다 (마지막 이벤트 시각 기준 값으로 판정)."""
        v = self.users.get(user_id)
        return v is not None and v[0] >= self.threshold

    def prune(self, now_ms: int) -> None:
        self.users = {u: vt for u, vt in self.users.items() if self._value(u, now_ms) >= self.prune_below}
        self._last_prune = now_ms
