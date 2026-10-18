"""Flink 집계 정확성 확인: 메인·구매 작업 출력을 DuckDB 정답과 대조한다 (제어기 없이 전부 통과).
PC 가벼운 설정에서 Kafka + Flink를 띄운 상태로 실행한다.

    python tools/verify_flink.py <parquet 날짜 폴더> live 5      # 작업을 먼저 띄우고 5배속 재생
    python tools/verify_flink.py <parquet 날짜 폴더> catchup 30  # 먼저 다 쌓은 뒤 작업을 띄움 (따라잡기)

마지막 1분 창은 다음 데이터가 와야 닫히므로 대조에서 뺀다 (ADR-0007).
"""
import json, os, subprocess, sys, time, urllib.request
from confluent_kafka import Consumer

B = "localhost:29092"
ENV = dict(os.environ, MSYS_NO_PATHCONV="1", PYTHONUTF8="1")
K = "/opt/kafka/bin/"


def rest(path, method="GET"):
    req = urllib.request.Request("http://localhost:8081" + path, method=method)
    with urllib.request.urlopen(req, timeout=3) as r:
        b = r.read()
    return json.loads(b) if b else None


def sh(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True, env=ENV)


def kx(*a):
    return sh("docker", "exec", "data-brain-kafka-1", *a)


TOPICS = {"events.view": 12, "events.cart": 6, "events.purchase": 3, "control.rates": 1,
          "agg.minute": 3, "agg.purchase": 3, "log.decisions": 3}
GROUPS = ("flink-main", "flink-main-control", "flink-vital")


def reset():
    for j in rest("/jobs/overview")["jobs"]:
        if j["state"] == "RUNNING":
            rest(f"/jobs/{j['jid']}?mode=cancel", "PATCH")
    while any(j["state"] in ("RUNNING", "CANCELLING") for j in rest("/jobs/overview")["jobs"]):
        time.sleep(1)
    for t in TOPICS:
        kx(K + "kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--delete", "--topic", t)
    time.sleep(5)
    for g in GROUPS:
        kx(K + "kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092", "--delete", "--group", g)
    for t, n in TOPICS.items():
        cfg = (["--config", "cleanup.policy=compact"] if t == "control.rates" else
               ["--config", "message.timestamp.type=LogAppendTime"] if t.startswith("agg.") else [])
        for _ in range(15):
            if kx(K + "kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists",
                  "--topic", t, "--partitions", str(n), "--replication-factor", "1", *cfg).returncode == 0:
                break
            time.sleep(2)
    time.sleep(5)


def submit(job, n_running):
    sh("docker", "exec", "data-brain-jobmanager-1", "flink", "run", "-d", "-py",
       f"/opt/databrain/src/databrain/flink_jobs/{job}.py")
    while True:
        jobs = [j for j in rest("/jobs/overview")["jobs"] if j["state"] == "RUNNING"]
        if len(jobs) >= n_running and all(j["tasks"]["running"] == j["tasks"]["total"] for j in jobs):
            return
        time.sleep(1)


def replay(day, speed):
    r = subprocess.run([sys.executable, "-m", "databrain.replayer.main", day, "--speed", speed],
                       capture_output=True, text=True, env=ENV, timeout=600)
    print("재생:", r.stdout.strip()[-60:], flush=True)


def collect(topic, quiet_s=20, first_wait_s=180):
    """첫 출력을 최대 first_wait_s초 기다리고, 그 뒤 새 출력이 quiet_s초 동안 없을 때까지 읽는다.
    출력이 Kafka에 기록된 시각(첫·마지막)도 돌려준다."""
    c = Consumer({"bootstrap.servers": B, "group.id": f"verify-{topic}-{time.time()}", "auto.offset.reset": "earliest"})
    c.subscribe([topic])
    rows, ts, start, last = [], [], time.time(), None
    while (last is None and time.time() - start < first_wait_s) or (last is not None and time.time() - last < quiet_s):
        m = c.poll(1.0)
        if m is not None and not m.error():
            rows.append(json.loads(m.value())); ts.append(m.timestamp()[1] / 1000); last = time.time()
    c.close()
    return rows, (min(ts) if ts else None, max(ts) if ts else None)


def check(day, agg, pur):
    import duckdb
    q = lambda sql: duckdb.sql(sql.replace("P", f"read_parquet('{day}/*.parquet')")).fetchall()
    want_m = {(f"{t}|{c}", int(m)): n for t, c, m, n in q(
        "select event_type, category_code, (epoch(date_trunc('minute', event_time))*1000)::bigint, count(*) "
        "from P where lane<>'purchase' group by all")}
    want_p = {(f"{b}|{c}", int(m)): (n, round(s, 2)) for b, c, m, n, s in q(
        "select brand, category_code, (epoch(date_trunc('minute', event_time))*1000)::bigint, count(*), sum(price) "
        "from P where lane='purchase' group by all")}
    got_m = {(r["key"], r["minute_ms"]): r["raw_count"] for r in agg}
    got_p = {(r["key"], r["minute_ms"]): (r["orders"], round(r["revenue"], 2)) for r in pur}
    out = {}
    for name, want, got in (("main", want_m, got_m), ("vital", want_p, got_p)):
        last = max(m for _, m in want)
        closed = {k: v for k, v in want.items() if k[1] != last}
        bad = [(k, v, got.get(k)) for k, v in closed.items() if got.get(k) != v]
        out[name] = {"closed": len(closed), "match": len(closed) - len(bad), "events": sum(closed.values()) if name == "main"
                     else sum(v[0] for v in closed.values()), "bad_sample": bad[:3]}
    return out


if __name__ == "__main__":
    day, mode, speed = sys.argv[1], sys.argv[2], sys.argv[3]
    reset()
    if mode == "live":
        submit("vital_job", 1); submit("main_job", 2); time.sleep(5)
        replay(day, speed)
    else:
        replay(day, speed)
        submit("vital_job", 1); submit("main_job", 2)
    t_end = time.time()
    (agg, ta), (pur, tp) = collect("agg.minute"), collect("agg.purchase", quiet_s=5)
    lag = lambda t: None if t[0] is None else [round(t[0] - t_end, 1), round(t[1] - t_end, 1)]
    res = {"mode": mode, "speed": speed, "main_output_s_after_replay": lag(ta),
           "vital_output_s_after_replay": lag(tp), **check(day, agg, pur)}
    print(json.dumps(res, ensure_ascii=False), flush=True)
