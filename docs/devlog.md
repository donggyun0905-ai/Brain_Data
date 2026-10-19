# 개발 일지

다른 PC나 새 세션에서 이어서 작업할 때 이 파일부터 읽는다. 결정의 근거는 `docs/adr/`, 설계 원칙은 `CLAUDE.md`,
1주차 세부 체크리스트는 `docs/week1.md`에 있다.

- 마지막 갱신: 2026-10-19
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
| ClickHouse 적재 | **실제 데이터로 확인 완료.** 정답 일치 777/777·548/548, 중복 제거도 통과 | ADR-0016 |
| 수면 재생 | **설계 완료, 모형 교정 구현·검증 완료** (1·3단계는 12월·2월) | ADR-0014 |
| 오차 범위 (H2 기준 ①) | **분산 식 구현·검증 완료.** 1분 창 적중률 91.9~94.7% | ADR-0015 |
| 화면 3개, 배치(MinIO·Spark·Airflow) | 시작 전 (일정상 12월~2월) | 기획서 |
| AWS (EC2·Terraform) | 시작 전 (11월 예정) | ADR-0006 |

- 단위 테스트 45개 통과 (`pytest -q`, 브로커 없이)
- 2026-10-19: `requirements.txt`에 pandas가 빠져 있어 새 PC에서 `tools/`가 실행 실패 → 추가함
- 공개 페이지(비공개 링크, 소유자만 열림)
  - 설명 페이지: https://claude.ai/artifact/ACFZMmuQmAq37w99JFPhWG (`docs/explainer.html`)
  - 샘플링 관제실: https://claude.ai/artifact/NyZpqvgoGM1rBPfNsxXX93 (`docs/sampling-cctv.html`)
  - 최종 기획서: https://claude.ai/code/artifact/e8034996-53e3-4d2d-ba6e-f7d6e84507d6

---

## 2. 다음에 할 일 (순서대로)

### 2-1. ClickHouse까지 실제 데이터 흐름 확인 — **2026-10-19 완료 (ADR-0016)**
- [x] 가벼운 설정에 ClickHouse 추가해서 실행 (학교 PC 실제 사용량 약 1.8GB)
- [x] `infra/clickhouse/init.sql` 확인: Kafka 엔진 → 구체화 뷰 → ReplacingMergeTree
- [x] 11/17 14:00~14:06 46,564건 재생 → `SELECT ... FINAL`로 (키, 분)별 조회
- [x] DuckDB 정답 대조 도구 추가 (`tools/verify_clickhouse.py`, `verify_flink.check()` 재사용)
  - 실시간 5배속: main 777/777, vital 548/548 일치, 군더더기 0
- [x] 같은 창이 두 번 들어와도 한 번만 세는지 확인
  - `agg_minute` 777→1,554행 / `purchase_minute` 548→1,096행인데 `FINAL`은 777·548로 정답 일치
- [ ] 3회 반복 기록 (지금은 각 1회. 값이 '전부 일치/불일치'로 갈리는 확인이라 반복 가치는 낮음)
- [x] 덤: 도구 이식성 수정 (`docker-compose.yml`에 `name: data-brain` 고정) + 검증 도구가
      조용히 잘못된 결론을 내던 문제 3건 수정 (ADR-0016)

### 2-2. 1단계 마무리 (~11/8)
- [ ] Kafka에 볼륨 추가 (지금은 `down`하면 토픽 데이터가 사라짐 → 7일 보관 원칙과 안 맞음)
- [ ] MinIO 공식 이미지 배포 정책 확인 (배치 단계 전까지)
- [ ] README의 "현재 상태" 표 갱신
- [ ] `chore/dev-setup` 브랜치를 main에 합치기 (PR 만들어서 직접 해 보기)

### 2-3. 11월: AWS 준비 (ADR-0006)
- [ ] Terraform으로 EC2 구성 (관리형 서비스·NAT Gateway 금지, 예산 약 20만 원)
- [ ] 100배속 처리량 측정 (PC로는 못 따라감: PC 약 1,250건/초, 100배속 평균 약 2,600건/초)
- [x] ~~Flink 병렬 2 이상에서 상위 판정이 쪼개지는 문제~~ → **2026-10-19 해소 (ADR-0013)**
  - 쪼개짐은 100%였다 (산수로 보장됨). 그런데도 판정 불일치 0.02%, 봇 판정률 97.2%로 병렬도와 무관
  - 토픽 칸 수를 맞추거나 조회만 세는 변경은 하지 않는다
- [ ] `tools/verify_shedding.py`를 병렬 2 이상에서 한 번 돌리기 (우선순위 낮춤. 창 집계·워터마크까지 포함한 확인용)
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
> - **학교 PC에서는 Docker가 안 된다.** 브로커가 필요한 일은 집 PC에서 한다 (ADR-0012)
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

### 2026-10-19 — 학교 PC 환경 구축, Docker 불가 확인
- 학교 PC에 환경을 처음부터 재현: Python 3.12.10 + `.venv` + `pip install -e .`, 단위 테스트 24개 통과
- REES46 2019-Nov를 다시 받아 `prepare` 실행 → **67,501,979행 / 30일, 날짜별 행 수가 ADR-0009 실측과 전부 일치**
  - 변환 후 원본 CSV(8.4GB) 삭제. Parquet 4.4GB만 남김 (C: 여유가 좁아서)
  - 압축이 `2019-Nov.csv`라는 *폴더* 안에 같은 이름 파일로 풀렸지만 DuckDB가 폴더째 읽어 정상 변환됨
- **Docker Desktop을 띄울 수 없음을 확인 (ADR-0012)**: WSL·Hyper-V 기능이 둘 다 미설치이고 계정이 일반 사용자
  → 학교 PC는 브로커가 필요 없는 일(시뮬레이션·DuckDB 도구·설계·연구) 전용으로 쓴다
- 일지 2-1(ClickHouse 실제 데이터 확인)은 집 PC 작업으로 이동
- **병렬도 문제 해소 (ADR-0013)**: `tools/compare_parallel.py`를 만들어 11/17 14시로 측정
  - 조회·장바구니가 다른 서브태스크로 갈라지는 비율은 **100%** (토픽 이름 해시 시작값이 1 차이라 산수로 보장됨)
  - 그런데도 판정 불일치 0.02%, 봇 판정률 97.2%가 병렬 1~12에서 전부 동일. crc32/murmur2 분배기도 무관
  - 0%(병렬 1)와 100%(병렬 2~12) 양 끝을 다 재었으므로 배정 규칙 가정이 틀려도 결론은 같다
- **수면 재생 설계 (ADR-0014)**: ADR-0002가 `(ADR-0006 예정)`으로 가리켰던 빈 기록을 채움
  - 세 단계: 재계산(확정값) → 모형 교정(처리 능력 되찾기) → 조절값 탐색(optuna)
  - **2단계 구현 완료** (`src/databrain/sleep/fit.py`, 테스트 9개). 그날 `log.decisions`만으로
    `capacity_eps`와 `filter_cost`를 되찾는다. 꽉 찬 구간에서 (읽은 수, 남긴 수)가 직선 위에 모이는 성질을 쓴다
  - 정답을 아는 가짜 하루로 검증: 오차 0.00%, r²=1.0. 밀림 잡음 ±1,000건에서 +12.7%, ±10,000건이면 스스로 거부
  - 순환 논리 방지: 시뮬레이터가 그날 실제 결과를 재현하지 못하면 교정을 건너뛰는 '역검증 관문'
  - 테스트가 버그 2개를 잡음 (꽉 찬 구간 판정, 외피 집는 방식의 기울기 편향)
- **학교 PC에 Docker 올림**: `wsl --install --no-distribution` + 재시작으로 해결 (ADR-0012 정정).
  토픽 12개와 ClickHouse 테이블 9개 확인, Flink 2.2.1 작업자 1개/슬롯 2개. 메모리 합계 약 1.8GB
- **오차 범위와 H2 실측 (ADR-0015)**: `sampling/interval.py` + 테스트 12개 + `tools/measure_h2.py`
  - 혼합 샘플링은 묶음 추출이다. 단순 이항 분산을 쓰면 적중률 80% 미만. `(2i-1)(1-r)/r²`을 실어 더하면 됨
  - **기획서 H2 기준 ③은 달성 불가**: 통과 10%/5분 조회수 오차 실측 7.2% (목표 3%) → 기획서 갱신 요청
  - 적중률: 1분 91.9~94.7%, 5분 86.0~93.1%. **식은 맞고 추정이 원인** (정답 분산이면 94.3~95.5%)
  - → 화면 기본 단위는 1분. 5분 이상은 수면 재생 확정값의 Σ A²로 보정 (ADR-0014와 연결)
- **ClickHouse 적재 확인 완료 (ADR-0016, 일지 2-1)**: `tools/verify_clickhouse.py` 추가
  - 실제 11월 데이터로 main 777/777, vital 548/548 정답 일치. 중복이 들어와도 `FINAL`은 그대로
  - `main_job.py`와 `init.sql`에 오차 범위 열(`var_sum`) 추가, 화면용 `agg_minute_shown` 뷰 추가
  - 도구가 컨테이너 이름을 박아 둬서 폴더명이 다른 PC에서 전부 실패할 상태였다 → `name: data-brain` 고정
  - **교훈**: 검증 도구가 조용히 잘못된 결론을 내는 실수를 세 번 했다 (stderr 버림, 성급한 기다림 2종).
    '틀린 판정보다 판정 불가가 낫다'로 고쳤다. 검증 중에는 스택을 조회만 한다

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
