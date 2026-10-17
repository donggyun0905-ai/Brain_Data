"""
활동량 상위 사용자 판별 (근사 집계, Count-Min Sketch).

사용자 수가 많아도 고정된 메모리로 '최근 구간에 이벤트를 많이 만든 사용자'를 찾는다.
구간이 끝나면 절반으로 줄여(감쇠) 오래된 활동의 영향을 낮춘다.
Count-Min Sketch는 실제보다 크게 셀 수는 있어도 작게 세지는 않는다.
"""
from __future__ import annotations

import hashlib


class HeavyUserTracker:
    def __init__(self, width: int = 2048, depth: int = 4, share: float = 0.001):
        """
        share: 구간 전체 이벤트 중 이 비율 이상을 만든 사용자를 '상위'로 본다.
               기본 0.1% (사용자 1천 명 중 1명꼴이 상위 1%라면 대략 이 수준. E3에서 조정)
        """
        self.width, self.depth, self.share = width, depth, share
        self.table = [[0.0] * width for _ in range(depth)]
        self.total = 0.0

    def _cols(self, key: str):
        for i in range(self.depth):
            d = hashlib.blake2b(key.encode("utf-8"), digest_size=8, salt=i.to_bytes(8, "big")).digest()
            yield i, int.from_bytes(d, "big") % self.width

    def add(self, user_id: str, n: float = 1.0) -> None:
        for i, c in self._cols(user_id):
            self.table[i][c] += n
        self.total += n

    def estimate(self, user_id: str) -> float:
        return min(self.table[i][c] for i, c in self._cols(user_id))

    def is_heavy(self, user_id: str) -> bool:
        if self.total <= 0:
            return False
        return self.estimate(user_id) >= self.share * self.total

    def decay(self, factor: float = 0.5) -> None:
        for row in self.table:
            for j in range(self.width):
                row[j] *= factor
        self.total *= factor
