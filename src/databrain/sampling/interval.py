"""
추정치에 붙일 오차 범위 (H2 성공 기준 ①: "표시한 95% 오차 범위가 실제값을 95% 안팎으로 포함").

혼합 샘플링은 두 방식이 섞여 있다 (ADR-0003, 0011).
- 상위 사용자: 이벤트마다 따로 뽑는다 → 서로 독립
- 나머지 사용자: 사용자 해시로 통째로 뽑는다 → 한 사람의 이벤트는 다 들어오거나 다 빠진다

그래서 단순 이항 분산(`n(1-r)/r`)을 쓰면 **실제보다 좁은 구간**이 나온다. 한 사람이 그 칸에
10건을 만들었다면 그 10건은 같이 움직이므로, 흔들림은 1건 10개가 아니라 10건 1개짜리다.

## 묶음 추출로 보면 더해지는 식이 된다
'묶음(cluster)'을 이렇게 정하면 두 방식을 한 식으로 쓸 수 있다.

    상위 사용자의 이벤트 하나 = 묶음 하나 (크기 1)
    나머지 사용자의 (사용자, 통과 비율)별 이벤트 묶음 = 묶음 하나 (크기 A)

묶음 c가 크기 A_c, 뽑힐 확률 r_c로 통째로 뽑힌다고 보면 호비츠-톰슨 추정치와 그 분산은

    추정치 Y = Σ_c (A_c / r_c) × [뽑혔는가]
    분산  V = Σ_c A_c² (1 − r_c) / r_c

V는 뽑히지 않은 묶음도 알아야 해서 그대로는 못 센다. 그런데 뽑힌 묶음만으로

    V^ = Σ_{뽑힌 c} A_c² (1 − r_c) / r_c²

를 재면 기대값이 정확히 V가 된다 (뽑힐 확률이 r_c이므로 r_c로 한 번 더 나눈다).

## 왜 집계 코드를 거의 안 바꿔도 되는가
A² 은 하나씩 더해 만들 수 있다. 묶음의 i번째 이벤트에서 `2i − 1`을 더하면 합이 A² 이다
(1 + 3 + 5 + … + (2A−1) = A²). 따라서 남긴 이벤트마다

    흔들림 조각 = (2·묶음_안_순번 − 1) × (1 − 비율) / 비율²

을 실어 보내고, 칸마다 그냥 **더하면** 그 칸의 V^ 가 된다. 지금 `est_count`를 더하는 것과 똑같은
모양이라 집계 단계(`flink_jobs/main_job.py`의 `merge`)에 숫자 하나만 늘리면 된다.

## 한계 (측정으로 확인할 것)
- 통과 비율이 창 안에서 바뀌면 한 사용자가 어떤 초에는 들어오고 어떤 초에는 빠진다.
  겹치는 임계값(hash < rate) 때문에 이 선택들은 서로 **독립이 아니라 포개져 있다**.
  (사용자, 비율)별로 묶음을 나누고 묶음끼리 독립이라고 보면 분산을 **조금 작게** 본다
  → `tests/test_interval.py`에서 적중률로 확인한다
- 남은 건수가 아주 적으면 정규 근사가 나빠진다. 적중률 측정으로 쓸 수 있는 하한을 정한다
"""
from __future__ import annotations

import math
from dataclasses import dataclass

Z95 = 1.959963984540054        # 표준정규 양쪽 95%


def variance_part(rate: float, cluster_index: int) -> float:
    """남긴 이벤트 하나가 그 칸의 흔들림에 더하는 양.

    rate          : 그 이벤트를 남길 때 쓴 통과 비율 (머리말과 함께 실어 보낸 값)
    cluster_index : 그 이벤트가 자기 묶음에서 몇 번째인가 (1부터).
                    상위 사용자는 이벤트마다 묶음이 따로이므로 항상 1
    """
    if not 0.0 < rate <= 1.0:
        raise ValueError("rate는 0보다 크고 1 이하여야 함")
    if cluster_index < 1:
        raise ValueError("cluster_index는 1부터")
    return (2 * cluster_index - 1) * (1.0 - rate) / (rate * rate)


@dataclass(frozen=True)
class Interval:
    est: float
    lo: float
    hi: float
    half_width: float

    @property
    def rel_half_width(self) -> float:
        """추정치 대비 오차 범위 반폭. 화면에 '±N%'로 띄울 값."""
        return self.half_width / self.est if self.est > 0 else 0.0


def interval(est: float, var: float, z: float = Z95) -> Interval:
    """추정치와 모아 둔 흔들림에서 오차 범위를 만든다. 아래쪽은 0에서 자른다 (건수는 음수가 없다)."""
    if var < 0:
        raise ValueError("var는 음수가 될 수 없음")
    half = z * math.sqrt(var)
    return Interval(est, max(0.0, est - half), est + half, half)


class ClusterIndex:
    """묶음 안 순번을 센다. 거르기 단계가 아니라 본문을 파싱한 뒤에 쓴다 (칸을 알아야 하므로).

    Kafka 키가 user_id라서 한 사용자는 늘 같은 작업으로 간다 → 작업마다 따로 세어도 정확하다
    (단, 조회와 장바구니는 다른 작업으로 갈라진다. ADR-0013. 칸이 이벤트 종류로 나뉘어 있어 문제되지 않는다).

    비율은 자릿수를 줄여 묶는다. 1초마다 바뀌는 값을 그대로 쓰면 묶음이 잘게 쪼개져
    분산을 작게 보기 때문이다 (쪼개질수록 '한 사람이 같이 움직인다'는 성질이 사라진다).

    창(분)은 열쇠에 넣고, 오래된 분만 치운다. '분이 바뀌면 다 비우기'로 하면 안 된다.
    조회·장바구니 토픽을 합친 흐름에서는 분이 뒤섞여 들어와 상태가 계속 날아간다
    (2026-10-19: Flink에 넣기 전에 발견).
    """

    def __init__(self, rate_digits: int = 2, keep_windows: int = 3):
        self.rate_digits = rate_digits
        self.keep_windows = keep_windows
        self._n: dict[tuple, int] = {}
        self._max_minute: int | None = None

    def next(self, user_id: str, cell: str, minute_ms: int, rate: float, heavy: bool) -> int:
        if heavy:
            return 1                                  # 이벤트마다 묶음이 따로
        if self._max_minute is None or minute_ms > self._max_minute:
            self._max_minute = minute_ms
            self._prune()
        key = (minute_ms, user_id, cell, round(rate, self.rate_digits))
        i = self._n.get(key, 0) + 1
        self._n[key] = i
        return i

    def _prune(self) -> None:
        cut = self._max_minute - self.keep_windows * 60_000
        if any(k[0] < cut for k in self._n):
            self._n = {k: v for k, v in self._n.items() if k[0] >= cut}
