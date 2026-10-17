"""ADR-0008 실제 확인 도구. PC 가벼운 설정에서 Kafka + Flink를 띄운 상태로 실행한다.
실제 Flink에서 제어기 신호 방식 비교: 2만 건 덩어리를 넣고 제어기 판단을 기록한다.
python verify_controller.py <parquet 날짜 폴더> <결과.jsonl> <signal> <반복번호> <처리능력>"""
import json, os, subprocess, sys, threading, time, urllib.request
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

B = "localhost:29092"
DAY, OUT, SIGNAL, REP, CAP = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5]
ENV = dict(os.environ, MSYS_NO_PATHCONV="1", PYTHONUTF8="1")
TOPICS = {"events.view": (12, None), "events.cart": (6, None), "control.rates": (1, "cleanup.policy=compact"),
          "log.decisions": (3, None)}
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


def reset():
    for j in rest("/jobs/overview")["jobs"]:
        if j["state"] == "RUNNING":
            rest(f"/jobs/{j['jid']}?mode=cancel", "PATCH")
    while any(j["state"] in ("RUNNING", "CANCELLING") for j in rest("/jobs/overview")["jobs"]):
        time.sleep(1)
    for t in TOPICS:
        kx(K + "kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--delete", "--topic", t)
    time.sleep(3)
    for g in ("flink-main", "flink-main-control"):
        kx(K + "kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092", "--delete", "--group", g)
    for t, (n, cfg) in TOPICS.items():
        for _ in range(15):
            r = kx(K + "kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists",
                   "--topic", t, "--partitions", str(n), "--replication-factor", "1", *(["--config", cfg] if cfg else []))
            if r.returncode == 0:
                break
            time.sleep(2)
    adm = AdminClient({"bootstrap.servers": B})
    parts = [TopicPartition(t, p) for t, (n, _) in TOPICS.items() for p in range(n)]
    while True:
        try:
            fe = adm.list_offsets({tp: OffsetSpec.latest() for tp in parts}, request_timeout=3)
            if sum(v.result(timeout=3).offset for v in fe.values()) == 0:
                break
        except Exception:
            pass
        time.sleep(1)
    time.sleep(3)


def submit():
    sh("docker", "exec", "data-brain-jobmanager-1", "flink", "run", "-d", "-py",
       "/opt/databrain/src/databrain/flink_jobs/main_job.py")
    while True:
        jobs = [j for j in rest("/jobs/overview")["jobs"] if j["state"] == "RUNNING"]
        if jobs and jobs[0]["tasks"]["running"] == jobs[0]["tasks"]["total"]:
            return jobs[0]["jid"]
        time.sleep(1)


def filter_counts(jid):
    """거르기 단계 입력/출력 (조회+장바구니+제어 메시지)"""
    v = [x["id"] for x in rest(f"/jobs/{jid}")["vertices"] if "Co-Process" in x["name"]][0]
    m = {x["id"]: int(float(x["value"])) for x in rest(f"/jobs/{jid}/vertices/{v}/metrics?get=0.numRecordsIn,0.numRecordsOut")}
    return m.get("0.numRecordsIn", 0), m.get("0.numRecordsOut", 0)


reset()
jid = submit()
time.sleep(5)
ctl = subprocess.Popen([sys.executable, "-m", "databrain.controller.main", "--capacity", CAP, "--signal", SIGNAL],
                       env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
time.sleep(8)
in0, out0 = filter_counts(jid)
t_replay = time.time()
r = subprocess.run([sys.executable, "-m", "databrain.replayer.main", DAY, "--speed", "2000"],
                   capture_output=True, text=True, env=ENV, timeout=120)
# Flink가 다 처리할 때까지 + 30초 더 관찰
total = None
t_done = None
deadline = time.time() + 400
while time.time() < deadline:
    fin, fout = filter_counts(jid)
    adm = AdminClient({"bootstrap.servers": B})
    parts = [TopicPartition(t, p) for t in ("events.view", "events.cart") for p in range(TOPICS[t][0])]
    end = sum(v.result(timeout=3).offset for v in adm.list_offsets({tp: OffsetSpec.latest() for tp in parts}).values())
    if t_done is None and fin - in0 >= end:      # 거르기 단계가 모든 이벤트를 받음 (제어 메시지 몇 건 오차)
        t_done = time.time()
    if t_done and time.time() - t_done > 30:
        break
    time.sleep(0.5)
ctl.terminate(); ctl.wait()
fin, fout = filter_counts(jid)
# 제어기 판단 기록 읽기
c = Consumer({"bootstrap.servers": B, "group.id": f"verify-{time.time()}", "auto.offset.reset": "earliest"})
c.subscribe(["log.decisions"])
dec, idle = [], 0
while idle < 5:
    m = c.poll(1.0)
    if m is None:
        idle += 1
        continue
    if not m.error():
        dec.append(json.loads(m.value()))
c.close()
dec.sort(key=lambda d: d["at_ms"])
ep = [d for d in dec if d["at_ms"] / 1000 >= t_replay]
after = [d for d in ep if d["at_ms"] / 1000 >= t_done]
res = {"signal": SIGNAL, "rep": REP, "events": end, "drain_s": round(t_done - t_replay, 1),
       "kept_pct": round(100 * (fout - out0) / max(fin - in0, 1), 1),
       "view_min": min(d["view"] for d in ep), "view_mean_until_done": round(
           sum(d["view"] for d in ep if d["at_ms"] / 1000 < t_done) / max(1, sum(1 for d in ep if d["at_ms"] / 1000 < t_done)), 3),
       "false_backlog_s": sum(1 for d in after if d["backlog_view"] > 1000),
       "view_below_1_after_s": sum(1 for d in after if d["view"] < 0.999),
       "max_backlog_seen": max(d["backlog_view"] for d in ep),
       "signals": sorted({d.get("signal", "commit") for d in ep})}
print(json.dumps(res), flush=True)
with open(OUT, "a") as f:
    f.write(json.dumps(res) + "\n")
with open(OUT.replace(".jsonl", f"_{SIGNAL}_{REP}.dec.json"), "w") as f:
    json.dump({"t_replay": t_replay, "t_done": t_done, "dec": ep}, f)
