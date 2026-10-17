# 1주차 체크리스트 (2026-10-19 ~ 10-25)

목표: 노트북에서 '재생기 → Kafka' 까지 실제 데이터가 흐르는 것을 눈으로 확인한다.

## 환경
- [x] Python 3.12 가상환경 만들고 `pip install -r requirements.txt` (3.12.7, PyCharm 인터프리터 연결)
- [x] `make test` 통과 확인 (17개). Windows에는 make가 없어 PyCharm 실행 구성 "테스트 (pytest)" 사용
- [x] Docker Desktop 메모리 할당 확인 → **문제 있음, 아래 '발견한 것' 참고**
- [ ] GitHub에 이 저장소 올리기 (data/ 폴더는 .gitignore에 있음)

## 이미지·빌드 확인
- [x] 이미지 태그 존재 확인 (2026-10-17, `docker manifest inspect`): 아래 모두 있음
  - apache/kafka:4.1.0, clickhouse/clickhouse-server:25.8, prom/prometheus:v3.5.0, grafana/grafana:12.1.0, danielqsj/kafka-exporter:v1.9.0, python:3.12-slim
- [x] `infra/flink/Dockerfile` 빌드
  - flink:2.2.1-java17 태그 있음. 기반 OS Ubuntu 24.04, python3 = 3.12.3
  - Kafka 커넥터: 4.0.1-2.0(Flink 2.0용) → **5.0.0-2.2(Flink 2.2용)로 변경**
  - 빌드 실패 2건 수정: 기본 사용자가 flink라 apt 권한 없음 → 설치 동안만 `USER root` / `/usr/bin/python` 링크로는 가상환경이 안 잡힘 → `PATH`에 `/opt/venv/bin`
- [ ] MinIO 공식 이미지 배포 정책 확인 (batch 단계 전까지만 확인하면 됨)

## 데이터
- [ ] Kaggle 계정으로 REES46 2019-Oct.csv 다운로드 → `data/raw/`
- [ ] `make prepare`로 날짜별 Parquet 생성, 행 수와 날짜 범위 확인 (가짜 데이터로는 동작 확인함)
- [ ] 이벤트 종류 비율 실측 (기획서 가정: 조회 92%, 장바구니 6.5%, 구매 1.5%) → 다르면 시뮬레이션 다시 돌리기
- [ ] 완전히 같은 중복 행이 몇 개인지 세기 (입구 정리 단계의 기준값)
- [ ] 사용자별 활동량 분포 확인 (상위 1%가 조회의 몇 %를 만드는지) → 혼합 샘플링 기준 정하기

## 첫 흐름 (REES46 형식의 가짜 데이터로 확인, 실제 데이터로 다시 할 것)
- [x] Kafka + kafka-init만 실행 → 토픽 12개 생성 확인
- [x] 재생 → `kafka-console-consumer`로 머리말 형식 확인 (키 = user_id, 값 = `user_id⇥event_id⇥ingest_ms⇥JSON`)
- [x] 재생기 단일 프로세스 최대 전송 속도: **3회 중앙값 약 39,000건/초 (범위 36,000~48,000)**, 30만 건 기준
  - 10월 데이터 평균은 실제 시간으로 약 16건/초 → 100배속 × 10배 폭증이어도 약 16,000건/초라 `--workers` 불필요

## 발견한 것
- **메모리**: PC 전체 15.4GB, Docker(WSL2) 할당 8GB. core 전체의 메모리 한도 합은 약 11GB라 그대로는 못 띄움.
  Kafka만 띄우면 약 0.5GB 사용. 해결 방법을 정해야 함 (Flink 작업자 수 줄이기 / WSL 할당 늘리기 / 일부 서비스만 켜기)
- **PC에서 Kafka 접속 주소는 `localhost:29092`** (9092는 컨테이너 내부용). 재생기·제어기 기본값을 29092로 바꿈
- **Git Bash에서 `docker exec`에 `/opt/...` 경로를 주면 Windows 경로로 바뀜** → `MSYS_NO_PATHCONV=1`을 붙이거나 PowerShell 사용
- **줄 끝 문자**: `core.autocrlf=true`라 새로 받으면 `.sh`가 CRLF가 되어 컨테이너에서 실패할 수 있음 → `.gitattributes`로 LF 고정
- DuckDB 시간대가 Asia/Seoul이지만 `prepare` 결과의 event_time은 UTC 그대로 유지됨을 확인 (00:00 시작)
- Kafka에 볼륨이 없어서 `make down`(컨테이너 삭제)을 하면 토픽 데이터도 사라짐. 가짜 데이터 정리에는 편하지만 7일 보관 원칙과 맞추려면 나중에 볼륨 추가 필요

## 기록
- [ ] 발견한 것과 바꾼 것을 `docs/adr/`에 남기기 (메모리 대응 방법이 정해지면 ADR-0006으로)
