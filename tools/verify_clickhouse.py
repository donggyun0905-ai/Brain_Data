"""ClickHouse 적재 정확성 확인 (개발 일지 2-1).

Flink 출력이 Kafka 엔진 테이블 → 구체화 뷰 → ReplacingMergeTree를 거쳐 들어간 값을
DuckDB 정답과 대조한다. `verify_flink.py`가 Kafka 토픽까지 확인한 것을 ClickHouse까지 넓힌 것이다.

    python tools/verify_clickhouse.py make data/sub/ch5          # 11/17 14:00~14:06 부분 데이터 만들기
    python tools/verify_clickhouse.py run  data/sub/ch5 live 5   # 작업 먼저 띄우고 5배속 재생
    python tools/verify_clickhouse.py run  data/sub/ch5 catchup 30   # 먼저 다 쌓은 뒤 작업 띄움
    python tools/verify_clickhouse.py twice data/sub/ch5 30      # 같은 데이터를 두 번 보냄 (덮어쓰기 확인)
    python tools/verify_clickhouse.py shed  data/sub/ch5 5 0.1   # 조회 10%로 거른 뒤 오차 범위 적중률 확인

## 무엇을 보는가
1. **값이 맞는가**: ClickHouse `SELECT ... FINAL`의 (키, 분)별 건수·매출이 DuckDB 정답과 같은지
2. **오차 범위가 맞는가** (`shed`): 조회 비율을 고정해 거른 뒤, ClickHouse `agg_minute_shown`의
   95% 구간이 DuckDB 정답을 품는지 센다. ADR-0015를 실제 파이프라인으로 확인하는 것이다
3. **두 번 들어와도 한 번만 세는가**: 같은 창이 또 들어와도 ReplacingMergeTree가 덮어쓰는지 (`twice`).
   따라잡기나 작업 재시작 때 같은 창이 다시 나오므로 이게 깨지면 화면 숫자가 부풀어 오른다 (설계 원칙 9)

## 주의
- 시각은 `toUnixTimestamp(minute)*1000`으로 꺼내 비교한다. ClickHouse DateTime은 서버 시간대에
  맞춰 보이므로 글자로 비교하면 시간대 차이에 걸린다
- 토픽을 다시 만들면 ClickHouse Kafka 엔진의 소비자가 옛 위치를 들고 있다 → clickhouse 컨테이너를
  다시 띄워 새로 붙게 한다
- 마지막 1분 창은 다음 데이터가 와야 닫히므로 대조에서 뺀다 (ADR-0007, verify_flink.py와 같은 규칙)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

sys.path.insert(0, "src")
sys.path.insert(0, "tools")
from verify_flink import ENV, reset, rest, sh, submit  # noqa: E402

SRC = "data/parquet/2019-11/day=2019-11-17"
WINDOW = ("2019-11-17 14:00:00", "2019-11-17 14:06:00")      # 6분: 닫히는 창 5개가 남는다
CH = "data-brain-clickhouse-1"
TABLES = ("agg_minute", "purchase_minute", "decisions")


def make(out: str) -> None:
    """정답 대조용 부분 데이터. 봇은 넣지 않는다 (여기서는 적재 경로만 본다).

    재생기와 정답 계산 모두 `<폴더>/*.parquet`를 읽으므로 하위 폴더를 만들지 않고 평평하게 쓴다.
    `day=...` 하위 폴더에 넣으면 재생기가 파일을 못 찾는다 (2026-10-19에 겪음).
    """
    import duckdb
    os.makedirs(out, exist_ok=True)
    con = duckdb.connect()
    con.sql(f"""COPY (SELECT * FROM read_parquet('{SRC}/*.parquet')
                 WHERE event_time >= '{WINDOW[0]}' AND event_time < '{WINDOW[1]}'
                 ORDER BY event_time)
                TO '{out}/data_0.parquet' (FORMAT parquet)""")
    n = con.sql(f"SELECT count(*), min(event_time), max(event_time) "
                f"FROM read_parquet('{out}/*.parquet')").fetchall()[0]
    print(f"만듦: {n[0]:,}건 {n[1]} ~ {n[2]} → {out}")


def replay(day: str, speed: str) -> None:
    """재생. `verify_flink.replay`는 stderr를 버리고 stdout 60자만 찍어서 실패가 묻힌다
    (2026-10-19: 경로가 틀려 한 건도 안 보냈는데 조용히 넘어갔다). 여기서는 터뜨린다."""
    r = subprocess.run([sys.executable, "-m", "databrain.replayer.main", day, "--speed", speed],
                       capture_output=True, text=True, env=ENV, timeout=1800)
    if r.returncode != 0:
        raise RuntimeError(f"재생 실패 (코드 {r.returncode}): {(r.stderr or r.stdout).strip()[-500:]}")
    print("재생:", r.stdout.strip(), flush=True)


def produced() -> dict[str, int]:
    """이벤트 토픽에 실제로 몇 건 들어갔는지. 0이면 더 기다릴 이유가 없다."""
    out = {}
    for t in ("events.view", "events.cart", "events.purchase"):
        r = sh("docker", "exec", "data-brain-kafka-1", "/opt/kafka/bin/kafka-get-offsets.sh",
               "--bootstrap-server", "localhost:9092", "--topic", t)
        out[t] = sum(int(ln.rsplit(":", 1)[1]) for ln in r.stdout.splitlines() if ":" in ln)
    return out


def ch(sql: str, fmt: str = "TSV") -> str:
    r = subprocess.run(["docker", "exec", CH, "clickhouse-client", "--user", "databrain",
                        "--password", "databrain", "-q", f"{sql} FORMAT {fmt}" if fmt else sql],
                       capture_output=True, text=True, env=ENV, timeout=120)
    if r.returncode != 0:
        raise RuntimeError(f"ClickHouse 오류: {r.stderr.strip()[:300]}")
    return r.stdout.strip()


def ch_reset() -> None:
    """저장 테이블을 비우고, Kafka 엔진 소비자를 새 토픽에 다시 붙인다."""
    for t in TABLES:
        ch(f"TRUNCATE TABLE databrain.{t}", fmt="")
    sh("docker", "restart", CH)
    for _ in range(60):
        try:
            ch("SELECT 1")
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("ClickHouse가 다시 뜨지 않음")


def truth(day: str):
    """DuckDB 정답. verify_flink.check와 같은 식을 쓴다 (키 모양도 같게)."""
    import duckdb
    q = lambda sql: duckdb.sql(sql.replace("P", f"read_parquet('{day}/*.parquet')")).fetchall()
    main = {(f"{t}|{c}", int(m)): n for t, c, m, n in q(
        "select event_type, category_code, (epoch(date_trunc('minute', event_time))*1000)::bigint, count(*) "
        "from P where lane<>'purchase' group by all")}
    vital = {(f"{b}|{c}", int(m)): (n, round(s, 2)) for b, c, m, n, s in q(
        "select brand, category_code, (epoch(date_trunc('minute', event_time))*1000)::bigint, count(*), "
        "round(sum(price),2) from P where lane='purchase' group by all")}
    return main, vital


def from_ch():
    """ClickHouse에 들어간 값. FINAL로 덮어쓰기를 적용해 읽는다."""
    main = {}
    for line in ch("SELECT concat(event_type,'|',category_code), toUnixTimestamp(minute)*1000, "
                   "sum(raw_count) FROM databrain.agg_minute FINAL GROUP BY 1,2").splitlines():
        if line:
            k, m, n = line.split("\t")
            main[(k, int(m))] = int(n)
    vital = {}
    for line in ch("SELECT concat(brand,'|',category_code), toUnixTimestamp(minute)*1000, "
                   "sum(orders), round(sum(revenue),2) FROM databrain.purchase_minute FINAL "
                   "GROUP BY 1,2").splitlines():
        if line:
            k, m, n, s = line.split("\t")
            vital[(k, int(m))] = (int(n), round(float(s), 2))
    return main, vital


def agg_offsets() -> dict[str, int]:
    """Flink가 집계 토픽에 내보낸 총 건수. 적재가 끝났는지 판단하는 기준이다."""
    out = {}
    for t in ("agg.minute", "agg.purchase"):
        r = sh("docker", "exec", "data-brain-kafka-1", "/opt/kafka/bin/kafka-get-offsets.sh",
               "--bootstrap-server", "localhost:9092", "--topic", t)
        out[t] = sum(int(ln.rsplit(":", 1)[1]) for ln in r.stdout.splitlines() if ":" in ln)
    return out


def jobs_read() -> int:
    """돌고 있는 작업들이 지금까지 읽은 건수 합. 늘고 있으면 아직 일하는 중이다."""
    total = 0
    for j in rest("/jobs/overview")["jobs"]:
        if j["state"] != "RUNNING":
            continue
        d = rest(f"/jobs/{j['jid']}")
        total += sum(v["metrics"].get("read-records", 0) for v in d["vertices"])
    return total


def wait_pipeline(quiet_s: float = 40.0, limit_s: float = 900.0) -> dict:
    """1) Flink가 일을 멈추고 출력이 더 늘지 않을 때까지 → 2) ClickHouse 행이 그 수에 이를 때까지.

    두 번 데었다 (2026-10-19).
    - ClickHouse 행 수만 세며 기다리니, 구매 쪽이 먼저 끝나 조용해지면 메인 쪽이 아직
      계산 중인데도 빠져나와 'agg_minute 0행'으로 오판했다
    - 출력 건수만 봐도 같은 일이 난다. 메인의 2단계 집계는 한참 읽다가 창이 닫힐 때
      몰아서 내보내므로, 읽는 중에는 출력이 조용하다
    → 그래서 '출력 건수'와 '작업이 읽은 건수'가 **둘 다** 멈춘 뒤에 다음으로 간다.
    """
    start, last_change, prev = time.time(), None, None
    while True:
        cur, now = {**agg_offsets(), "_read": jobs_read()}, time.time()
        if cur != prev:
            prev, last_change = cur, now
        if last_change and sum(cur.values()) > 0 and now - last_change >= quiet_s:
            break
        if now - start > limit_s:
            print(f"  (Flink 출력 기다림 한계: {cur})", flush=True)
            break
        time.sleep(3)
    prev.pop("_read", None)
    want = {"agg_minute": prev["agg.minute"], "purchase_minute": prev["agg.purchase"]}
    while True:
        got = {t: int(ch(f"SELECT count() FROM databrain.{t}")) for t in want}
        if all(got[t] >= want[t] for t in want):
            return {"flink_out": want, "ch_rows": got}
        if time.time() - start > limit_s:
            print(f"  (ClickHouse 적재 기다림 한계: {got} / 목표 {want})", flush=True)
            return {"flink_out": want, "ch_rows": got}
        time.sleep(3)


def compare(day: str):
    t_main, t_vital = truth(day)
    g_main, g_vital = from_ch()
    out = {}
    for name, want, got in (("main", t_main, g_main), ("vital", t_vital, g_vital)):
        if not want:
            out[name] = {"closed": 0, "note": "정답이 비어 있음"}
            continue
        last = max(m for _, m in want)                    # 마지막 창은 닫히지 않는다
        closed = {k: v for k, v in want.items() if k[1] != last}
        bad = [(list(k), v, got.get(k)) for k, v in closed.items() if got.get(k) != v]
        extra = [list(k) for k in got if k not in want]
        out[name] = {"closed": len(closed), "match": len(closed) - len(bad),
                     "ch_rows": len(got), "extra_in_ch": len(extra),
                     "bad_sample": bad[:3], "extra_sample": extra[:3]}
    return out


def run_once(day: str, mode: str, speed: str) -> dict:
    reset()
    ch_reset()
    if mode == "live":
        submit("vital_job", 1)
        submit("main_job", 2)
        time.sleep(5)
        replay(day, speed)
    else:
        replay(day, speed)
        submit("vital_job", 1)
        submit("main_job", 2)
    sent = produced()
    if sum(sent.values()) == 0:
        raise RuntimeError(f"이벤트 토픽이 비어 있다. 재생이 안 됐다: {sent}")
    print("보낸 건수:", sent, flush=True)
    waited = wait_pipeline()
    print("적재:", waited, flush=True)
    return compare(day)


def shed(day: str, speed: str, view_rate: float) -> dict:
    """조회 비율을 고정해 거른 뒤, 화면에 띄울 오차 범위가 정답을 품는지 센다 (ADR-0015)."""
    from confluent_kafka import Producer
    reset()
    ch_reset()
    now = int(time.time() * 1000)
    pr = Producer({"bootstrap.servers": "localhost:29092"})
    pr.produce("control.rates", key=b"rates", value=json.dumps(
        {"version": now, "cart": 1.0, "view": view_rate, "at_ms": now}).encode())
    pr.flush()
    submit("main_job", 1)
    time.sleep(10)                       # 브로드캐스트 상태에 비율이 들어갈 틈을 준다
    replay(day, speed)
    sent = produced()
    if sum(sent.values()) == 0:
        raise RuntimeError(f"이벤트 토픽이 비어 있다: {sent}")
    print("적재:", wait_pipeline(), flush=True)

    t_main, _ = truth(day)
    last = max(m for _, m in t_main)
    want = {k: v for k, v in t_main.items() if k[1] != last and k[0].startswith("view|")}
    shown = {}
    for line in ch("SELECT concat(event_type,'|',category_code), toUnixTimestamp(minute)*1000, "
                   "est_count, lo, hi, half_width, raw_count FROM databrain.agg_minute_shown "
                   "WHERE event_type='view'").splitlines():
        if line:
            k, m, e, lo, hi, hw, raw = line.split("	")
            shown[(k, int(m))] = (float(e), float(lo), float(hi), float(hw), int(raw))
    big = {k: v for k, v in want.items() if v >= 100}      # 화면에 띄울 만한 칸만
    seen = [k for k in big if k in shown]
    hit = [k for k in seen if shown[k][1] <= big[k] <= shown[k][2]]
    err = [abs(shown[k][0] - big[k]) / big[k] for k in seen]
    rel = [shown[k][3] / big[k] for k in seen]
    kept = sum(shown[k][4] for k in seen)
    tot = sum(big[k] for k in seen)
    import statistics as st
    return {"view_rate": view_rate, "cells_true_ge_100": len(big), "cells_in_ch": len(seen),
            "missing_in_ch": len(big) - len(seen),
            "적중률%": round(len(hit) / len(seen) * 100, 1) if seen else None,
            "실제 통과율%": round(kept / tot * 100, 2) if tot else None,
            "추정 오차 중앙%": round(st.median(err) * 100, 1) if err else None,
            "추정 오차 p95%": round(sorted(err)[int(len(err) * 0.95)] * 100, 1) if err else None,
            "평균 상대 반폭%": round(st.mean(rel) * 100, 1) if rel else None,
            "반폭 0인 칸": sum(1 for k in seen if shown[k][3] == 0.0)}


def cancel_jobs() -> None:
    """토픽은 건드리지 않고 돌고 있는 작업만 멈춘다.

    작업자 슬롯이 2개뿐이라 먼저 취소하지 않고 또 제출하면 자리가 없어 CREATED에서 멈춘다
    (2026-10-19: `twice`를 처음 짰을 때의 결함).
    """
    for j in rest("/jobs/overview")["jobs"]:
        if j["state"] == "RUNNING":
            rest(f"/jobs/{j['jid']}?mode=cancel", "PATCH")
    while any(j["state"] in ("RUNNING", "CANCELLING") for j in rest("/jobs/overview")["jobs"]):
        time.sleep(1)


def twice(day: str, speed: str) -> dict:
    """같은 창을 두 번 보낸다. 토픽은 비우지 않고 작업만 다시 돌려 earliest부터 또 읽게 한다.

    따라잡기나 작업 재시작 때 실제로 일어나는 상황이다. 저장 테이블의 행은 늘어나지만
    `FINAL` 결과는 그대로여야 한다 (설계 원칙 9).
    """
    first = run_once(day, "catchup", speed)
    rows_before = {t: int(ch(f"SELECT count() FROM databrain.{t}")) for t in TABLES[:2]}
    cancel_jobs()
    submit("vital_job", 1)
    submit("main_job", 2)
    print("적재(2회차):", wait_pipeline(), flush=True)
    rows_after = {t: int(ch(f"SELECT count() FROM databrain.{t}")) for t in TABLES[:2]}
    second = compare(day)
    grew = {t: rows_after[t] > rows_before[t] for t in rows_before}
    verdict = {}
    for name, table in (("main", "agg_minute"), ("vital", "purchase_minute")):
        f, sc = first[name], second[name]
        if f["closed"] != f["match"]:
            verdict[name] = f"판정 불가 (1회차가 이미 안 맞음: {f['match']}/{f['closed']})"
        elif not grew[table]:
            verdict[name] = "판정 불가 (중복이 들어오지 않았다 — 2회차 출력이 없음)"
        elif sc["closed"] == sc["match"]:
            verdict[name] = "통과 (행은 늘었는데 FINAL 값은 그대로)"
        else:
            verdict[name] = f"실패 (FINAL 값이 달라졌다: {sc['match']}/{sc['closed']})"
    return {"first": first, "second_after_duplicate": second,
            "rows_before": rows_before, "rows_after": rows_after,
            "행이 늘었나": grew, "판정": verdict}


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "make":
        make(sys.argv[2])
    elif cmd == "run":
        print(json.dumps(run_once(sys.argv[2], sys.argv[3], sys.argv[4]), ensure_ascii=False, indent=1))
    elif cmd == "shed":
        print(json.dumps(shed(sys.argv[2], sys.argv[3], float(sys.argv[4])), ensure_ascii=False, indent=1))
    elif cmd == "twice":
        print(json.dumps(twice(sys.argv[2], sys.argv[3]), ensure_ascii=False, indent=1))
    else:
        raise SystemExit(__doc__)
