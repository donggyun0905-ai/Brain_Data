"""
Kafka 메시지 형식.

PyFlink의 KafkaSource는 메시지의 값(value)만 읽을 수 있고, 키와 헤더는 읽지 못한다
(PyFlink 2.2.1 패키지에서 set_value_only_deserializer만 제공됨을 확인. ADR-0005).
그래서 거르는 판단에 필요한 정보를 값 맨 앞의 짧은 머리말에 넣는다.

    user_id \t event_id \t ingest_ms \t {본문 JSON}

거르는 단계는 split('\t', 3)로 머리말만 읽고, 본문 JSON은 통과한 이벤트만 파싱한다.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

SEP = "\t"


@dataclass(frozen=True)
class Head:
    user_id: str
    event_id: str
    ingest_ms: int
    body: str          # 아직 파싱하지 않은 본문 JSON 문자열


def encode(user_id: str | int, event_id: str | int, ingest_ms: int, payload: dict[str, Any]) -> bytes:
    u, e = str(user_id), str(event_id)
    if SEP in u or SEP in e:
        raise ValueError("user_id/event_id에 탭 문자가 들어갈 수 없음")
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return SEP.join((u, e, str(int(ingest_ms)), body)).encode("utf-8")


def peek(value: str | bytes) -> Head:
    """본문을 파싱하지 않고 머리말만 읽는다 (거르는 단계용, 저비용)."""
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    u, e, ts, body = value.split(SEP, 3)
    return Head(u, e, int(ts), body)


def decode(value: str | bytes) -> tuple[Head, dict[str, Any]]:
    """머리말과 본문을 모두 읽는다 (통과한 이벤트만)."""
    h = peek(value)
    return h, json.loads(h.body)
