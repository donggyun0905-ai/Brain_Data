"""
혼합 샘플링 (ADR-0003).

- 일반 사용자: 사용자 ID 해시로 거름 → 한 사람의 세션이 통째로 남거나 통째로 빠짐 (퍼널 왜곡 방지)
- 활동량 상위 사용자: 이벤트 단위로 거름 → 많이 보는 소수 때문에 총량이 흔들리는 문제 방지
- 겹치는 임계값: hash < rate 로 판단하므로 5%에 뽑힌 사용자는 10%에도 항상 포함된다.
  비율이 바뀌어도 이미 보던 사용자의 세션이 끊기지 않는다.
- 남긴 이벤트에는 그 순간의 비율을 함께 실어 보내, 집계 때 1/비율을 하나씩 더한다.
"""
from __future__ import annotations

import hashlib

_MAX = float(1 << 64)


def unit_hash(key: str) -> float:
    """문자열을 [0, 1) 구간의 고정된 값으로 바꾼다. 프로세스·재시작과 무관하게 항상 같다."""
    d = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(d, "big") / _MAX


def keep(user_id: str, event_id: str, rate: float, heavy: bool) -> bool:
    if rate >= 1.0:
        return True
    if rate <= 0.0:
        return False
    if heavy:
        return unit_hash(f"e:{user_id}:{event_id}") < rate
    return unit_hash(f"u:{user_id}") < rate


def weight(rate: float) -> float:
    """호비츠-톰슨 가중치. 남긴 이벤트 하나가 대표하는 원래 이벤트 수."""
    if rate <= 0:
        raise ValueError("rate는 0보다 커야 함")
    return 1.0 / rate
