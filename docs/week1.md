# 1주차 체크리스트 (2026-10-19 ~ 10-25)

목표: 노트북에서 '재생기 → Kafka' 까지 실제 데이터가 흐르는 것을 눈으로 확인한다.

## 환경
- [ ] Python 3.12 가상환경 만들고 `pip install -r requirements.txt`
- [ ] `make test` 통과 확인 (17개)
- [ ] Docker Desktop 메모리 할당 확인 (core만 켜도 약 9GB 필요)
- [ ] GitHub에 이 저장소 올리기 (data/ 폴더는 .gitignore에 있음)

## 이미지·빌드 확인 (이 저장소를 만든 환경에서는 직접 받아보지 못한 것들)
- [ ] `docker compose --profile core pull`로 이미지 태그 존재 확인. 없으면 가까운 버전으로 바꾸고 기록
  - apache/kafka:4.1.0, clickhouse/clickhouse-server:25.8, prom/prometheus:v3.5.0, grafana/grafana:12.1.0, danielqsj/kafka-exporter:v1.9.0
- [ ] `infra/flink/Dockerfile` 빌드
  - flink:2.2.1-java17 태그 존재 여부
  - 기반 OS의 python3가 3.11 또는 3.12인지 (아니면 Dockerfile의 확인 단계가 실패하도록 해둠)
  - Kafka 커넥터 JAR 버전이 Flink 2.2와 맞는지 (Maven Central에서 flink-sql-connector-kafka 최신 2.x용 확인)
- [ ] MinIO 공식 이미지 배포 정책 확인 (batch 단계 전까지만 확인하면 됨)

## 데이터
- [ ] Kaggle 계정으로 REES46 2019-Oct.csv 다운로드 → `data/raw/`
- [ ] `make prepare`로 날짜별 Parquet 생성, 행 수와 날짜 범위 확인
- [ ] 이벤트 종류 비율 실측 (기획서 가정: 조회 92%, 장바구니 6.5%, 구매 1.5%) → 다르면 시뮬레이션 다시 돌리기
- [ ] 완전히 같은 중복 행이 몇 개인지 세기 (입구 정리 단계의 기준값)
- [ ] 사용자별 활동량 분포 확인 (상위 1%가 조회의 몇 %를 만드는지) → 혼합 샘플링 기준 정하기

## 첫 흐름
- [ ] `make up-core` 후 kafka-init 로그에서 토픽 목록 확인
- [ ] 하루치 일부를 10배속으로 재생 → `kafka-console-consumer`로 메시지 머리말 형식 확인
- [ ] 재생기 단일 프로세스 최대 전송 속도 측정 (부족하면 --workers 사용)

## 기록
- [ ] 발견한 것과 바꾼 것을 `docs/adr/`에 남기기
