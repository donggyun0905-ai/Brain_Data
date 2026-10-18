# Data Brain

> 트래픽이 폭증하면 덜 중요한 데이터를 스스로 골라 덜 보고, 매일 밤 그날을 다시 재생하며 스스로 나아지는 실시간 데이터 파이프라인.

- 기획서: [Data Brain 최종 기획서](https://claude.ai/code/artifact/e8034996-53e3-4d2d-ba6e-f7d6e84507d6)
- 데이터: [REES46 다중 카테고리 쇼핑몰 행동 로그](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store), [GA4 구글 굿즈 스토어 샘플](https://developers.google.com/analytics/bigquery/web-ecommerce-demo-dataset)

## 구조

```
재생기 ─▶ Kafka ─▶ PyFlink (구매 경로 / 거르기 + 집계 경로) ─▶ ClickHouse ─▶ 화면 3개
              ▲              ▲
              │      제어기 (1초마다 통과 비율 계산)
              │
     수면 재생: 밤마다 폭증 구간을 원래 속도로 다시 보내 처리 능력 모델을 고침
```

## 빠른 시작

```bash
# 1. Python 3.12 가상환경 (PyFlink 2.2.1이 3.13을 지원하지 않음)
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && pip install -e .

# 2. 단위 테스트 (브로커 불필요)
make test

# 3. 사전 검증 시뮬레이션 재현
make sim

# 4. 데이터 준비: Kaggle에서 2019-Nov.csv를 data/raw/에 받은 뒤 (10월 대신 11월 사용, 2026-10-18)
make prepare SRC=data/raw/2019-Nov.csv OUT=data/parquet/2019-11

# 5. 기본 구성 실행 (메모리 약 9GB)
make up-core

# 6. 하루치 재생 (100배속)
make replay DAY=data/parquet/2019-11/day=2019-11-01 SPEED=100
```

## 현재 상태 (2026-10-18)

| 구성 요소 | 상태 |
| --- | --- |
| 메시지 형식, 혼합 샘플링, 예측형 제어 계산, 재생 시간표, 문제 주입 | 구현 + 단위 테스트 24개 통과 |
| 실제 데이터 | REES46 2019-11, 67,501,979행 준비 완료 (ADR-0009) |
| 재생기 Kafka 전송 | 최대 약 39,000건/초 |
| 제어기 루프 | 실제 Flink로 확인. 처리 위치는 Flink REST + 추측 항법 (ADR-0008) |
| Flink 메인·구매 작업 | 실제 데이터로 정답 대조 통과, 10% 거르기도 칸마다 일치 (ADR-0007, 0011) |
| 혼합 샘플링 상위 사용자 기준 | 30분 감쇠 건수 50건, 가짜 봇으로 안전장치 확인 (ADR-0010, 0011) |
| ClickHouse 적재 | 가짜 데이터로만 확인, 실제 데이터는 다음 작업 |
| 수면 재생, 화면 3개, AWS | 일정상 11월~2월 |

자세한 진행 상황과 다음 할 일은 `docs/devlog.md`.

## 문서

- `CLAUDE.md`: 설계 원칙과 작업 규칙 (Claude 세션용이지만 사람이 읽어도 됨)
- `docs/adr/`: 기술 결정 기록
- `docs/devlog.md`: 개발 일지 (현재 상황, 다음 할 일, 다른 PC에서 시작하기)
- `docs/week1.md`: 첫 주 체크리스트
- `sim/`: 사전 검증 시뮬레이션
- `tools/`: 실제 스택 측정 도구
