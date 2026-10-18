# Data Brain — Claude 작업 안내

새 세션을 시작하면 이 파일을 먼저 읽는다. 결정의 근거는 `docs/adr/`, 전체 계획은 최종 기획서에 있다.

- 최종 기획서: https://claude.ai/code/artifact/e8034996-53e3-4d2d-ba6e-f7d6e84507d6
- 목표: 대기업 데이터 엔지니어 취업용 포트폴리오. 2026-10-19 ~ 2027-03-28, 1인 진행
- 사용자는 실무 도구를 익히는 단계. 설명은 한국어로, 처음 보는 사람도 이해할 수 있게 한다

## 한 줄 요약
트래픽이 폭증하면 덜 중요한 데이터를 스스로 골라 덜 보고(예측형 제어기 + 혼합 샘플링),
매일 밤 그날 폭증 구간을 원래 속도로 재생해 처리 능력 모델을 고치는(수면 재생) 실시간 파이프라인.

## 반드시 지킬 설계 원칙 (근거: 사전 검증 시뮬레이션, sim/)
1. 구매는 별도 토픽 + 별도 Flink 작업(`vital_job.py`). 절대 거르지 않는다
2. 거르는 판단은 메시지 머리말만 읽는다 (`wire.peek`). 본문 JSON은 통과한 것만 파싱
   머리말: `user_id ⇥ event_id ⇥ event_ms ⇥ ingest_ms ⇥ JSON` (ADR-0005, 0007)
3. Kafka에 쌓인 데이터를 **읽을 때** 현재 비율로 거른다
4. 제어기는 1초마다 밀림을 직접 읽는다: 끝 위치는 Kafka, 처리 위치는 Flink REST + 추측 항법, 커밋 위치는 대비책 (ADR-0008).
   Prometheus(15초 주기)를 제어에 쓰지 않는다
5. 혼합 샘플링: 상위 사용자는 이벤트 단위, 나머지는 겹치는 사용자 해시. 남긴 이벤트에 비율을 실어 1/비율로 집계.
   상위 = 사용자별 30분 감쇠 건수 50 이상(봇 안전장치), 이벤트 시각 기준 정확 집계 (ADR-0010, 0011)
6. Kafka 키는 user_id. 상품·카테고리 집계는 소금(salt) 2단계 집계
7. 걸러진 데이터도 버리지 않는다 (Kafka 7일 + Parquet/Iceberg 보관 → 밤에 재계산)
8. 수면 재생은 Parquet 보관본을 재생기로 원래 간격대로 `replay.` 토픽에 다시 보낸다
9. ClickHouse는 ReplacingMergeTree, 워터마크에는 유휴 시간. 워터마크는 SQL Kafka 소스에서 파티션별로 (ADR-0007)
10. 관리형 클라우드 서비스(MSK, EMR, Managed Flink)와 NAT Gateway는 쓰지 않는다 (예산 약 20만 원)

## 코드로 확인한 사실 (2026-10-16)
- PyFlink 2.2.1은 Python 3.11/3.12용 설치 파일만 있다 → **Python 3.12 고정**
- PyFlink 2.2.1 KafkaSource는 `set_value_only_deserializer`만 있다 → 키·헤더를 못 읽음 → 머리말 형식 (ADR-0005)
- PyFlink에 `BroadcastProcessFunction`, `KafkaOffsetsInitializer.timestamp()`, `set_bounded()`, `WatermarkStrategy.with_idleness` 있음
- 시뮬레이션: 조절 없음 신선도 p99 810초 / 단계형 1.9초 / 예측형 0.1초. 버린 양은 셋 다 약 58% (처리 능력이 결정)
- 샘플링: 이벤트 무작위는 퍼널을 최대 +16%p 왜곡, 사용자 해시는 총량 오차 최대 18%, 혼합 방식이 둘 다 해결

## 개발 PC에서 확인한 사실 (2026-10-17, 자세한 내용은 docs/week1.md)
- Windows 11, PC 메모리 15.4GB, Docker(WSL2) 할당 8GB → core 전체(한도 합 약 11GB)는 그대로 못 띄움
- make 없음 → PyCharm 실행 구성(.idea/runConfigurations) 사용. PC에서 Kafka 주소는 `localhost:29092`
- 이미지 태그 전부 존재 확인. Flink 커넥터는 `5.0.0-2.2`, Flink 이미지 안 Python 3.12.3
- 재생기 단일 프로세스 최대 약 39,000건/초 (3회 중앙값, 범위 36,000~48,000)
- PC에서는 `docker compose -f docker-compose.yml -f docker-compose.local.yml --profile core up -d kafka kafka-init jobmanager taskmanager` (ADR-0006)
- **PyFlink `from_source`는 Python TimestampAssigner를 오류 없이 무시한다** → 워터마크는 `flink_jobs/watermarks.py`의 SQL 소스로 (ADR-0007)
- vital_job·main_job 가짜 데이터로 실행·정답 대조 통과 (따라잡기 포함, 조회 30% 거르기 추정 오차 -0.9%)

## 실제 데이터 (2026-10-18, REES46 2019-Nov, 자세한 내용은 docs/week1.md)
- 10월 대신 **11월** 사용: 67,501,979행, 30일. 11/15~17은 하루 620만~650만 건으로 평소의 약 3.7배 (실제 폭증)
- 이벤트 비율 조회 94.15% / 장바구니 4.49% / 구매 1.36%. 완전 중복 0.149%. 상위 1% 사용자가 조회의 17.5%
- 실제 시간 평균 26건/초 → 100배속 약 2,600건/초. PC에서는 10~30배속으로 재생
- `prepare`는 2단계(split → sort). 한 번에 정렬하면 PC 메모리 부족
- 실제 데이터로 Flink 정답 대조 6회(실시간 3, 따라잡기 3) 모두 일치, 제어기 비교 3회씩 (ADR-0007, 0008)
- 1분 안에 몰아 보는 사용자는 거의 없다 (사용자·분당 조회 최대 13). 상위 사용자 처리 이득은 작고, 기준이 낮으면 세션 경로가 끊긴다 (ADR-0010, 0011)
- 가짜 봇(시간당 1,800건) 20개를 넣으면 사용자 해시만으로는 범주·분 오차 54% → 71%, 50건 기준이 53%로 되돌림. Flink 거르기도 정답과 일치 (ADR-0011, `tools/verify_shedding.py`)
- Flink 출력은 Kafka 기록 시각이 이벤트 시각(2019년) → 집계 토픽은 `message.timestamp.type=LogAppendTime`

## 제어기 밀림 신호 (2026-10-18, ADR-0008)
- Flink 커밋 위치는 실측 7~9초 늦다. 체크포인트를 1초로 줄여도 같다 → 처리 위치는 Flink REST currentOffset(+1)
- Flink REST 지표는 기본 10초마다 갱신 → `metrics.fetcher.update-interval: 1s`
- `FLINK_PROPERTIES` 블록 안에 `#` 주석 금지 (Flink가 값으로 읽어 시작 실패)
- 제어기 버전은 시작 시각(ms)부터 센다 (0부터 세면 재시작 후 Flink가 새 비율을 무시)
- 실제 확인: 밀림 착각 8초 → 0초, 비율 회복 17초 → 5초. 비교 실험은 `--signal commit`, 도구는 `tools/`

## 폴더
```
src/databrain/
  wire.py                 메시지 머리말 형식
  sampling/hashing.py     혼합 샘플링 판단, 가중치
  sampling/heavy.py       상위 사용자 판별 (사용자별 시간 감쇠 건수)
  controller/rate_model.py 예측형 비율 계산 (순수 함수, 테스트 있음)
  controller/lag.py       밀림·유입 속도 계산, Flink REST 처리 위치, 추측 항법
  controller/main.py      1초 제어 루프
  replayer/prepare.py     REES46 CSV → 시간순 Parquet (DuckDB)
  replayer/schedule.py    배속·폭증 시간표
  replayer/faults.py      중복·지연 주입 + 정답 기록
  replayer/main.py        Kafka로 재생
  flink_jobs/main_job.py  장바구니·조회 경로 (뼈대, 11월 실행 검증)
  flink_jobs/vital_job.py 구매 경로 (뼈대)
sim/                      사전 검증 시뮬레이션 (제어기 신호 비교 compare_lag_signals.py 포함)
tools/                    실제 스택 측정 도구 (밀림 신호 실측, 제어기 비교, 상위 사용자 기준 비교)
infra/                    Docker, Kafka 토픽, ClickHouse, Prometheus
docs/adr/                 기술 결정 기록
docs/week1.md             첫 주 체크리스트
docs/explainer.html       처음 보는 사람용 설명 페이지 (움직이는 모형 포함, https://claude.ai/artifact/ACFZMmuQmAq37w99JFPhWG)
docs/sampling-cctv.html   상위 사용자 기준 비교 관제실 페이지 (https://claude.ai/artifact/NyZpqvgoGM1rBPfNsxXX93)
```

## 명령
- 테스트: `make test` (브로커 없이 실행. 새 순수 로직에는 테스트를 같이 추가)
- 시뮬레이션 재현: `make sim`
- 기본 구성 실행: `make up-core` / 종료: `make down`

## 현재 단계와 다음 할 일
- 단계: 1단계 (환경·재생기·데이터 준비, ~11/8)
- 다음: ClickHouse까지 실제 데이터 흐름, 11월 EC2·Terraform 준비 (ADR-0006)
- 검문소: 12/6 E1 기준선 → 1/31 H1·H2 검증 → 2/28 기능 동결 → 3/28 완성

## 작업 규칙
- 코드 주석과 문서는 한국어
- 추측한 값은 추측이라고 적는다 (예: 이미지 태그, 메모리 추정치)
- 결정을 바꾸면 `docs/adr/`에 새 기록을 추가하고 기획서도 갱신 요청
- 실험 결과 수치는 3회 반복 중앙값과 범위로 기록
