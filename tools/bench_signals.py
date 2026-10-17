"""ADR-0008 실측 도구. PC 가벼운 설정에서 Kafka + Flink(main_job)를 띄운 상태로 실행한다.
체크포인트 주기별로 (1) 처리량 (2) 커밋 위치가 실제보다 얼마나 늦는지 (3) REST 지표 지연을 잰다.
python bench_signals.py <parquet 날짜 폴더> <결과.jsonl> <체크포인트ms,...> <반복>"""
import json, os, subprocess, sys, threading, time, urllib.request
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

B = "localhost:29092"; GROUP = "flink-main"
TOPICS = {"events.view": 12, "events.cart": 6}
DAY, OUT = sys.argv[1], sys.argv[2]
CPS = [int(x) for x in sys.argv[3].split(",")]
REPS = int(sys.argv[4])
REP0 = int(sys.argv[5]) if len(sys.argv) > 5 else 0
ENV = dict(os.environ, MSYS_NO_PATHCONV="1")
adm = AdminClient({"bootstrap.servers": B})
PARTS = [TopicPartition(t, p) for t, n in TOPICS.items() for p in range(n)]


def rest(path, method="GET"):
    req = urllib.request.Request("http://localhost:8081" + path, method=method)
    with urllib.request.urlopen(req, timeout=3) as r:
        body = r.read()
    return json.loads(body) if body else None


def sh(*cmd):
    return subprocess.run(cmd, capture_output=True, text=True, env=ENV)


def reset():
    global adm
    adm = AdminClient({'bootstrap.servers': B})   # 토픽을 지웠다 만들면 예전 메타데이터로 멈출 수 있음
    for j in rest("/jobs/overview")["jobs"]:
        if j["state"] == "RUNNING":
            rest(f"/jobs/{j['jid']}?mode=cancel", "PATCH")
    while any(j["state"] in ("RUNNING", "CANCELLING") for j in rest("/jobs/overview")["jobs"]):
        time.sleep(1)
    k = "/opt/kafka/bin/"
    for t in TOPICS:
        sh("docker", "exec", "data-brain-kafka-1", k + "kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--delete", "--topic", t)
    time.sleep(3)
    sh("docker", "exec", "data-brain-kafka-1", k + "kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092", "--delete", "--group", GROUP)
    for t, n in TOPICS.items():
        for _ in range(10):
            r = sh("docker", "exec", "data-brain-kafka-1", k + "kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                   "--create", "--if-not-exists", "--topic", t, "--partitions", str(n), "--replication-factor", "1")
            if r.returncode == 0:
                break
            time.sleep(2)
    # 삭제가 끝나고 빈 토픽이 다시 생겼는지 확인 (지우는 중인 토픽에 보내면 데이터가 사라짐)
    while True:
        try:
            fe = adm.list_offsets({tp: OffsetSpec.latest() for tp in PARTS}, request_timeout=3)
            if sum(v.result(timeout=3).offset for v in fe.values()) == 0:
                break
        except Exception:
            pass
        time.sleep(1)
    time.sleep(3)


def submit(cp):
    sh("docker", "exec", "-e", f"CHECKPOINT_MS={cp}", "data-brain-jobmanager-1", "flink", "run", "-d", "-py",
       "/opt/databrain/src/databrain/flink_jobs/main_job.py")
    while True:
        jobs = [j for j in rest("/jobs/overview")["jobs"] if j["state"] == "RUNNING"]
        if jobs and jobs[0]["tasks"]["running"] == jobs[0]["tasks"]["total"]:
            return jobs[0]["jid"]
        time.sleep(1)


def metric_query(jid):
    vs = rest(f"/jobs/{jid}")["vertices"]
    src = [v["id"] for v in vs if "Source: src_" in v["name"]]
    cop = [v["id"] for v in vs if "Co-Process" in v["name"]][0]
    while True:   # 파티션이 배정된 뒤에야 파티션별 지표가 생긴다
        q, n = {}, 0
        for v in src:
            ids = [m["id"] for m in rest(f"/jobs/{jid}/vertices/{v}/metrics") if m["id"].endswith("currentOffset")]
            q[v] = ",".join(ids); n += len(ids)
        if n == sum(TOPICS.values()):
            return q, cop
        time.sleep(1)


def sample(jid, q, cop, rows, stop):
    while not stop.is_set():
        t0 = time.time()
        try:
            fe = adm.list_offsets({tp: OffsetSpec.latest() for tp in PARTS}, request_timeout=3)
            fc = adm.list_consumer_group_offsets([ConsumerGroupTopicPartitions(GROUP, PARTS)])
            end = sum(v.result(timeout=3).offset for v in fe.values())
            com = sum(max(tp.offset, 0) for tp in list(fc.values())[0].result(timeout=3).topic_partitions)
            cur = 0
            for v, ids in q.items():
                # currentOffset = 마지막으로 읽은 레코드 위치 → 다음 읽을 위치는 +1
                cur += sum(int(float(m["value"])) + 1 for m in rest(f"/jobs/{jid}/vertices/{v}/metrics?get={ids}")
                           if float(m["value"]) >= 0)
            cin = rest(f"/jobs/{jid}/vertices/{cop}/metrics?get=0.numRecordsIn")
            rows.append({"t": t0, "end": end, "com": com, "cur": cur,
                         "cop_in": int(float(cin[0]["value"])) if cin else 0})
        except Exception as e:  # 일시적인 조정자 오류 등
            print("재시도", e, flush=True)
        time.sleep(max(0, 0.25 - (time.time() - t0)))


def first(rows, pred):
    return next((r["t"] for r in rows if pred(r)), None)


for rep in range(REP0, REP0 + REPS):
    for cp in CPS:
        print('reset', flush=True); reset()
        print('submit', flush=True); jid = submit(cp)
        print('job', jid, flush=True)
        q, cop = metric_query(jid)
        time.sleep(5)
        rows, stop = [], threading.Event()
        th = threading.Thread(target=sample, args=(jid, q, cop, rows, stop)); th.start()
        time.sleep(1)
        print("replay", flush=True)
        r = subprocess.run([sys.executable, "-m", "databrain.replayer.main", DAY, "--speed", "1000000"], capture_output=True, text=True, env=ENV, timeout=120)
        print("replayed", r.returncode, r.stdout.strip()[-80:], r.stderr.strip()[-200:], flush=True)
        deadline = time.time() + 600
        while time.time() < deadline:
            time.sleep(1)
            if int(time.time()) % 30 == 0:
                print("wait", len(rows), rows[-1] if rows else None, flush=True)
            if rows and rows[-1]["end"] > 0 and rows[-1]["com"] >= rows[-1]["end"] and rows[-1]["cur"] >= rows[-1]["end"]:
                break
        print('done wait', rows[-1] if rows else None, flush=True)
        time.sleep(1)
        stop.set(); th.join()
        total = rows[-1]["end"]
        t_start = first(rows, lambda r: r["end"] > 0)
        t_cur = first(rows, lambda r: r["cur"] >= total) or rows[-1]["t"]
        t_com = first(rows, lambda r: r["com"] >= total) or rows[-1]["t"]
        busy = [r for r in rows if t_start <= r["t"] <= t_cur]
        # 커밋 위치 기준 밀림이 실제(REST) 밀림보다 얼마나 큰지
        over = sorted((r["end"] - r["com"]) - (r["end"] - r["cur"]) for r in busy)
        gap = sorted(r["cur"] - r["cop_in"] for r in busy)   # 소스가 읽었지만 거르기 단계에 아직 안 온 것
        cps = rest(f"/jobs/{jid}/checkpoints")
        summ = cps.get("summary", {}).get("end_to_end_duration", {})
        res = {"cp_ms": cp, "rep": rep, "events": total, "proc_s": round(t_cur - t_start, 2),
               "eps": round(total / (t_cur - t_start)), "commit_late_s": round(t_com - t_cur, 2),
               "over_med": over[len(over) // 2], "over_max": over[-1],
               "gap_med": gap[len(gap) // 2], "gap_max": gap[-1],
               "ckpt_count": cps.get("counts", {}).get("completed"), "ckpt_ms_avg": summ.get("avg"),
               "ckpt_ms_max": summ.get("max")}
        print(json.dumps(res), flush=True)
        with open(OUT, "a") as f:
            f.write(json.dumps(res) + "\n")
        with open(OUT.replace(".jsonl", f"_cp{cp}_r{rep}.rows.json"), "w") as f:
            json.dump(rows, f)
