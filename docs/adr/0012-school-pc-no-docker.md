# ADR-0012: 학교 PC는 Docker를 쓸 수 없다 → PC별 역할 분담

- 날짜: 2026-10-19 / 상태: 채택 (ADR-0006 '개발은 PC의 가벼운 설정'을 PC별로 나눔)
- 2026-10-19 정정: 계정에 관리자 권한이 있음을 확인. WSL 설치가 가능하므로 아래 역할 분담은
  '설치 전까지, 그리고 설치가 실패할 경우의 대비책'이다. 설치에 성공하면 상태를 '대체됨'으로 바꾼다

## 상황
ADR-0006은 개발을 '개발 PC의 가벼운 설정'에서 하도록 정했다. 그 PC는 집 PC(Docker 8GB 할당)였다.
학교 PC에서 같은 구성을 띄우려 하니 Docker Desktop의 Linux 엔진이 시작되지 않았다.

## 확인한 것 (2026-10-19, 학교 PC)
- Docker 로그에 `backend is not running`이 27분간 반복. 컨테이너가 하나도 뜨지 못함
- `LxssManager` 서비스 없음 → WSL Windows 기능이 설치되지 않았다
- `Lxss` 레지스트리 키 없음 → WSL 배포판 0개. `wsl --list --verbose`도 인식되지 않음
- `vmcompute` 서비스 없음 → Hyper-V 기능도 설치되지 않았다
- 계정 `dita_810`은 **Administrators 그룹 구성원** → UAC 승인으로 `wsl --install` 가능
  (처음에 `net session`으로 '일반 사용자'라고 적었으나, 그 명령은 '현재 셸이 관리자로 떠 있는가'만 본다. 2026-10-19 정정)
- 하드웨어: 삼성 데스크톱, 메모리 16GB, BIOS 2017(P04MBS.097.170601), HypervisorPresent=True
  → `systeminfo`가 '하이퍼바이저가 감지됨'이라고 보고하므로 BIOS 가상화는 켜져 있다. 기능만 켜면 될 것으로 본다 (추측)

## 후보
| 방식 | 판정 |
| --- | --- |
| `wsl --install`로 직접 설치 (관리자 승인 + 재시작) | **가능.** 되면 학교 PC도 집 PC와 같은 개발 장비가 된다 |
| 학교에서도 Docker 없이 전체 흐름 재현 | 탈락. Kafka·Flink·ClickHouse가 이 프로젝트의 본체다 |
| **PC별로 할 일을 나눈다** | **채택** |

## 결정
| 장비 | 하는 일 |
| --- | --- |
| **학교 PC** | 브로커가 필요 없는 일: 단위 테스트, `sim/` 시뮬레이션, DuckDB 도구(`profile_data.py`, `compare_heavy.py`), 설계·ADR·문서, 다음 실험 설계, 개선점 연구 |
| **집 PC (D:)** | Kafka·Flink·ClickHouse 실행 검증 (일지 2-1 등) |
| **AWS EC2 (11월)** | E1~E6 실험 수치 (ADR-0006 그대로) |

## 결과
- 학교 PC에도 데이터와 환경을 준비해 뒀다 (이 PC에서 처음부터 재현한 것)
  - Python 3.12.10 + `.venv`, `pip install -e .`, 단위 테스트 24개 통과
  - `data/parquet/2019-11` 67,501,979행 / 30일. 날짜별 행 수가 ADR-0009 실측과 모두 일치
  - 원본 CSV(8.4GB)는 변환 후 삭제. Parquet 4.4GB만 남김 (C: 여유가 좁음)
- 일지 2-1(ClickHouse 실제 데이터 확인)은 집 PC 작업으로 옮긴다
- 측정이 필요한 항목(`filter_cost` 실측 등)은 학교 PC에서 '설계까지', 숫자는 집 PC·EC2에서
