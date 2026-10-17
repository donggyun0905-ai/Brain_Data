# ADR-0001: 기본 구성 Kafka + PyFlink + ClickHouse + Iceberg

- 날짜: 2026-10-16 / 상태: 채택

## 상황
과부하 시 데이터를 골라 덜 보고, 걸러진 데이터도 나중에 정확히 재계산해야 한다.

## 후보와 선택
| 역할 | 선택 | 비교 대상 | 이유 |
| --- | --- | --- | --- |
| 이벤트 통로 | Kafka | Redpanda | 종류별 토픽, 보관, 시각 기준 재읽기. 업계 표준 |
| 실시간 처리 | PyFlink 2.2 | Spark Structured Streaming | 브로드캐스트 상태로 재시작 없이 비율 변경, 이벤트 시간 처리 |
| 실시간 저장 | ClickHouse | PostgreSQL | 대량 입력과 초 단위 집계 조회, Kafka 엔진 |
| 장기 저장 | Iceberg + MinIO | 일반 Parquet | 원본 전체 보관, 과거 시점 조회 |

## 포기한 것
- Java Flink의 성능과 풍부한 커넥터 대신 Python의 학습 속도를 택함. 처리량 실험은 '상대 비교'로 해석한다
- 관리형 서비스(MSK, EMR)를 쓰지 않아 직접 운영 부담이 있음 (대신 비용과 학습 효과)
