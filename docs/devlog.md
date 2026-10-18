# 개발 일지

다른 PC나 새 세션에서 이어서 작업할 때 이 파일부터 읽는다. 결정의 근거는 `docs/adr/`, 설계 원칙은 `CLAUDE.md`,
1주차 세부 체크리스트는 `docs/week1.md`에 있다.

- 마지막 갱신: 2026-10-18
- 작업 브랜치: `chore/dev-setup` (아직 main에 합치지 않음)
- 일정상 위치: 1단계 (환경·재생기·데이터 준비, ~11/8). 앞당겨 Flink·제어기·샘플링 검증까지 끝냄

---

## 1. 현재 상황 한눈에

| 구성 요소 | 상태 | 근거 |
| --- | --- | --- |
| 메시지 머리말 형식 (`wire.py`) | 완료, 테스트 있음 | ADR-0005, 0007 |
| 혼합 샘플링 (`sampling/`) | 완료. 상위 사용자 = 30분 감쇠 건수 50 이상 (봇 안전장치) | ADR-0003, 0010, 0011 |
| 예측형 제어기 (`controller/`) | 완료. 밀림 = Kafka 끝 위치 − Flink REST 처리 위치 + 추측 항법 | ADR-0002, 0008 |
| 재생기 (`replayer/`) | 완료. 단일 프로세스 최대 약 39,000건/초 | week1.md |
| 데이터 준비 (`replayer/prepare.py`) | 완료. REES46 2019-11, 67,501,979행 → 날짜별 Parquet | ADR-0009 |
| Flink 메인·구매 작업 | 실제 데이터로 정답 대조 통과 (전부 통과 6회, 10% 거르기 3회) | ADR-0007, 0011 |
| ClickHouse 적재 | 가짜 데이터로만 확인. **실제 데이터로는 아직** | week1.md |
| 수면 재생, 화면 3개, 배치(MinIO·Spark·Airflow) | 시작 전 (일정상 12월~2월) | 기획서 |
| AWS (EC2·Terraform) | 시작 전 (11월 예정) | ADR-0006 |

- 단위 테스트 24개 통과 (`pytest -q`, 브로커 없이)
- 공개 페이지(비공개 링크, 소유자만 열림)
  - 설명 페이지: https://claude.ai/artifact/ACFZMmuQmAq37w99JFPhWG (`docs/explainer.html`)
  - 샘플링 관제실: https://claude.ai/artifact/NyZpqvgoGM1rBPfNsxXX93 (`docs/sampling-cctv.html`)
  - 최종 기획서: https://claude.ai/code/artifact/e8034996-53e3-4d2d-ba6e-f7d6e84507d6

---

## 2. 다음에 할 일 (순서대로)

### 2-1. ClickHouse까지 실제 데이터 흐름 확인 ← 바로 다음
목표: 재생기 → Kafka → Flink → ClickHouse를 실제 11월 데이터로 돌리고, ClickHouse 결과가 DuckDB 정답과 같은지 본다.
- [ ] PC 가벼운 설정에 ClickHouse 추가해서 실행
  `docker compose -f docker-compose.yml -f docker-compose.local.yml --profile core up -d kafka kafka-init jobmanager taskmanager clickhouse`
  (메모리: 이전 가짜 데이터 때 Kafka·Flink·ClickHouse·제어기 합계 약 2.5GB)
- [ ] `infra/clickhouse/init.sql` 확인: Kafka 엔진 테이블(agg.minute, agg.purchase) → 구체화 뷰 → ReplacingMergeTree
- [ ] 11/17 피크 5분치(`tools/verify_flink.py`에서 쓰던 크기)를 재생한 뒤 ClickHouse에서 `SELECT ... FINAL`로 (키, 분)별 값 조회
- [ ] DuckDB 정답과 대조하는 도구 추가 (`tools/verify_flink.py`의 `check()`를 재사용하면 됨)
- [ ] 따라잡기(먼저 쌓고 나중에 처리)에서 같은 창이 두 번 들어와도 ReplacingMergeTree로 한 번만 세어지는지 확인
- [ ] 3회 반복, 중앙값과 범위를 week1.md·ADR에 기록

### 2-2. 1단계 마무리 (~11/8)
- [ ] Kafka에 볼륨 추가 (지금은 `down`하면 토픽 데이터가 사라짐 → 7일 보관 원칙과 안 맞음)
- [ ] MinIO 공식 이미지 배포 정책 확인 (배치 단계 전까지)
- [ ] README의 "현재 상태" 표 갱신
- [ ] `chore/dev-setup` 브랜치를 main에 합치기 (PR 만들어서 직접 해 보기)

### 2-3. 11월: AWS 준비 (ADR-0006)
- [ ] Terraform으로 EC2 구성 (관리형 서비스·NAT Gateway 금지, 예산 약 20만 원)
- [ ] 100배속 처리량 측정 (PC로는 못 따라감: PC 약 1,250건/초, 100배속 평균 약 2,600건/초)
- [ ] **Flink 병렬 2 이상에서 `tools/verify_shedding.py` 다시 돌리기** (추측, 미확인 문제)
  - 조회 토픽 12칸, 장바구니 토픽 6칸이라 한 사용자의 조회와 장바구니가 다른 필터 작업으로 갈 수 있음
  - 그러면 상위 사용자 건수가 작업마다 나뉘어 세어짐. 조회가 94%라 영향은 작을 것으로 추측
  - 차이가 크면: 두 토픽의 칸 수를 맞추거나, 상위 판정을 조회만으로 세기
- [ ] 정확 집계(dict) 메모리 확인: 작업 하나에 사용자 약 10만 명이면 수 MB로 추측 (ADR-0010)

### 2-4. 검문소
- 12/6 E1 기준선 → 1/31 H1·H2 검증 → 2/28 기능 동결 → 3/28 완성
- E1 재생 구간 후보: 평소 날 11/5, 폭증 날 11/15~17. 튜닝은 11/1~14, 평가는 11/15~30

---

## 3. 다른 PC에서 시작하기

> **먼저 꼭 알아 둘 것 3가지**
> - **데이터는 Git에 올라가지 않는다.** 새 PC에서 Kaggle의 `2019-Nov.csv`(9GB)를 다시 받아 `prepare`를 돌려야 한다 (약 10분)
> - **설치 명령이 하나 더 있다.** `pip install -r requirements.txt` 다음에 `pip install -e .`도 실행한다.
>   이게 없으면 `python -m databrain...`과 `tools/`의 도구들이 실행되지 않는다
> - **처음 받으면 브랜치를 바꿔야 한다.** clone 직후에는 `main`이 열린다. 지금 작업은 모두 `chore/dev-setup`에 있으므로
>   `git checkout chore/dev-setup`을 실행한다 (main에 합친 뒤에는 필요 없음)

```bash
git clone https://github.com/donggyun0905-ai/Brain_Data.git data-brain
cd data-brain
git checkout chore/dev-setup
```

1. **Python 3.12** 가상환경 (PyFlink 2.2.1은 3.13 미지원). Windows:
   `py -3.12 -m venv .venv` → `.venv\Scripts\activate` → `pip install -r requirements.txt` → `pip install -e .`
   (마지막 줄이 있어야 `python -m databrain...`과 도구들이 동작한다)
2. PyCharm에서 열면 실행 구성(`.idea/runConfigurations`)이 따라온다: 테스트, prepare, 시뮬레이션
3. 테스트: `python -m pytest -q` → 24개 통과 확인
4. **데이터는 Git에 없다** (`data/`는 .gitignore). Kaggle REES46에서 `2019-Nov.csv`(9.0GB)를 `data/raw/`에 받는다
   - https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
   - `python -m databrain.replayer.prepare data/raw/2019-Nov.csv data/parquet/2019-11` (또는 실행 구성 "prepare")
   - 2단계(날짜별 나누기 약 7분 + 하루씩 정렬 약 3분). 결과 67,501,979행이 맞는지 확인
5. Docker Desktop(WSL2) 메모리 할당 확인. 8GB면 core 전체는 못 띄우므로 가벼운 설정 사용 (ADR-0006)
   `docker compose -f docker-compose.yml -f docker-compose.local.yml --profile core up -d kafka kafka-init jobmanager taskmanager`
   - PC에서 Kafka 주소는 `localhost:29092`, Flink 화면은 http://localhost:8081
6. Windows 주의
   - Git Bash에서 `docker exec`에 `/opt/...` 경로를 주면 Windows 경로로 바뀜 → `MSYS_NO_PATHCONV=1`
   - 도구가 출력하는 JSON은 콘솔 인코딩(cp949)일 수 있음 → `PYTHONIOENCODING=utf-8` 붙여서 실행
   - 측정 도구를 여러 개 동시에 돌리면 메모리가 모자라 중간에 꺼진다 (10/18 실제로 발생) → 하나씩, 컨테이너는 필요할 때만
7. 이전 결과 재현 확인용 (선택)
   - `python tools/compare_heavy.py data/parquet/2019-11/day=2019-11-17 14` → ADR-0010·0011 표와 같은지
   - Flink 띄운 뒤 `python tools/verify_shedding.py make <폴더>` → `python tools/verify_shedding.py run <폴더> 5`

---

## 4. 날짜별 기록

### 2026-10-16 — 설계 원칙 확정과 코드로 확인
- 사전 검증 시뮬레이션(`sim/`)으로 설계 원칙 확정: 예측형 제어기 신선도 p99 0.1초 (단계형 1.9초, 조절 없음 810초)
- PyFlink 2.2.1 제약 확인 → Python 3.12 고정, KafkaSource가 키·헤더를 못 읽어 메시지 머리말 형식 도입 (ADR-0005)

### 2026-10-17 — 개발 PC 환경과 Flink 첫 실행
- PC 메모리 부족(Docker 8GB) → PC용 가벼운 설정 `docker-compose.local.yml` (ADR-0006)
- Flink 이미지 수정(커넥터 5.0.0-2.2, apt 권한, venv PATH), 재생기 → Kafka 흐름 확인
- **PyFlink `from_source`가 Python 워터마크를 조용히 무시**하는 것을 발견 → SQL Kafka 소스에서 파티션별 워터마크 (ADR-0007)
- 가짜 데이터로 Flink 정답 대조 통과 (메인 70/70, 구매 19/19), 전체 흐름(→ ClickHouse) 동작 확인

### 2026-10-18 — 실제 데이터, 제어기 신호, 샘플링 기준
- **제어기 밀림 신호 교체** (ADR-0008): Flink 커밋 위치가 7~9초 늦어 제어기가 밀림을 착각
  → Flink REST 처리 위치 + 추측 항법 + 커밋 대비책. 밀림 착각 8초 → 0초, 통과율 12.2% → 25.6%
- **실제 데이터 11월로 변경** (ADR-0009): 11/15~17 실제 폭증(평소의 3.7배). prepare를 2단계로
- 실제 데이터로 Flink 정답 대조 6회 모두 일치. 집계 토픽 Kafka 기록 시각이 2019년으로 붙는 문제 → LogAppendTime
- **상위 사용자 기준 재설계** (ADR-0010): 기존 CMS "전체의 0.1%"는 병렬도에 따라 판정이 4~6배 달라짐
  → 사용자별 30분 감쇠 건수, 이벤트 시각 기준, 정확 집계(dict)
- 세션 지표(경로·체류·담기 전 조회) 추가 측정: 기준이 낮으면 세션 경로가 크게 끊김 (20건에서 폭증 날 36%만 남음)
- **기준 20건 → 50건** (ADR-0011): 30건부터 정확도는 "사용자 해시만"과 같고 세션 보존만 좋아짐. 50건에서 경로 92.5%
  - 가짜 봇 확인: 봇 20개(분당 30건)일 때 사용자 해시만은 범주·분 오차 70.5%, 50건 기준은 53.2%로 되돌림
  - Flink 거르기 확인 (`tools/verify_shedding.py`, 조회 10% 고정): 3회 모두 2,161/2,161칸 일치, 봇 5개 모두 상위 판정
- 샘플링 관제실 페이지(CCTV 형식) 만들고, 설명 페이지·기획서 갱신

### 배운 것 (다음에 같은 실수 안 하기)
- 측정 결과가 늘 0이면 지표가 잘못된 것 (체류 시간 중앙값 → "10% 넘게 틀린 세션 비율"로 바꿈)
- "전부 통과" 검증은 거르기 규칙을 시험하지 못한다 → 비율을 고정해 둔 검증을 따로 만든다
- 실제 데이터에 없는 상황(봇)은 가짜로 넣어서 시험해야 안전장치가 작동하는지 알 수 있다
