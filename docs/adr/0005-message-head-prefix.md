# ADR-0005: 메시지 값 앞에 머리말을 둔다 (헤더 대신)

- 날짜: 2026-10-16 / 상태: 채택

## 상황
기획서는 '헤더만 보고 거르기'를 전제했다. 그런데 PyFlink 2.2.1 패키지를 열어보니
KafkaSource에 `set_value_only_deserializer`만 있어 Python에서는 키와 헤더를 읽을 수 없다.

## 후보
| 방식 | 판정 |
| --- | --- |
| 값 앞에 탭으로 구분한 머리말 (user_id, event_id, ingest_ms) | **채택**. split 한 번으로 읽어 비용이 작음 |
| Table API의 Kafka 메타데이터 열로 헤더 읽은 뒤 DataStream으로 변환 | 가능하지만 구조가 복잡해짐. 처리량 문제가 생기면 재검토 |
| Java로 거르기 단계만 작성 | 가장 빠르지만 언어가 두 개가 됨 |

## 결과
- 형식: `user_id \t event_id \t ingest_ms \t {본문 JSON}` (src/databrain/wire.py)
  → 2026-10-17 `event_ms`를 추가해 `user_id \t event_id \t event_ms \t ingest_ms \t {본문 JSON}`으로 변경 (ADR-0007)
- 이벤트 종류는 토픽으로 구분하므로 머리말에 넣지 않음
- E1에서 거르기 단계 비용을 측정해 기획서 가정(무거운 처리의 5%)과 비교
