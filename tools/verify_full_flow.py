"""제어기까지 포함한 전체 흐름 확인 + 그 기록으로 모형 교정 검증 (개발 일지 2-2, ADR-0014 2단계).

    python tools/verify_full_flow.py make data/sub/ctl30                  # 11/17 14:00~14:30 만들기
    python tools/verify_full_flow.py run  data/sub/ctl30 5 1200 600,600,6 # 5배속, 처리능력 1200, 폭증 주입

흐름: 재생기 → Kafka → Flink(거르기·집계) ← 제어기(1초마다 비율 계산) → ClickHouse

## 왜 폭증을 주입하는가
모형 교정(`sleep/fit.py`)은 '꽉 찬 구간에서 (읽은 수, 남긴 수)가 직선 위에 모인다'는 성질을 쓴다.
통과 비율이 바닥에 붙어만 있으면 읽은 수가 거의 일정해서 직선을 정할 수 없고, 도구가
`읽은 수가 거의 일정` 이유로 교정을 거부한다 (ADR-0014의 안전장치).
그래서 평시 → 폭증 → 회복이 있는 구간을 만들어 비율이 쓸려 내려갔다 올라오게 한다.

## 무엇을 보는가
1. 제어기가 실제로 걸렀는가: `log.decisions` → ClickHouse `decisions`의 비율 범위
2. 거른 뒤에도 추정이 맞는가: ClickHouse `est_count` 합을 DuckDB 정답 총량과 비교
3. **그날 기록만으로 처리 능력을 되찾을 수 있는가**: `sleep/fit.py`를 실제 기록에 돌린다.
   지금까지는 정답을 아는 가짜 하루로만 검증했다 (ADR-0014)
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time

sys.path.insert(0, "src")
sys.path.insert(0, "tools")
from databrain.sleep.fit import (Decision, fit_capacity, fit_from_rates,  # noqa: E402
                                 rate_windows, steps)
from verify_clickhouse import (ENV, agg_offsets, ch, ch_reset, produced,  # noqa: E402
                               truth, wait_pipeline)
from verify_flink import reset, submit  # noqa: E402

SRC = "data/parquet/2019-11/day=2019-11-17"
WINDOW = ("2019-11-17 14:00:00", "2019-11-17 14:30:00")
COMPOSE = ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.local.yml", "--profile", "core"]


def make(out: str) -> None:
    import duckdb
    os.makedirs(out, exist_ok=True)
    duckdb.sql(f"""COPY (SELECT * FROM read_parquet('{SRC}/*.parquet')
                   WHERE event_time >= '{WINDOW[0]}' AND event_time < '{WINDOW[1]}'
                   ORDER BY event_time) TO '{out}/data_0.parquet' (FORMAT parquet)""")
    r = duckdb.sql(f"SELECT count(*), min(event_time), max(event_time) "
                   f"FROM read_parquet('{out}/*.parquet')").fetchall()[0]
    print(f"만듦: {r[0]:,}건 {r[1]} ~ {r[2]} → {out}")


def controller(capacity: float, up: bool) -> None:
    env = dict(ENV, CAPACITY_EPS=str(capacity))
    cmd = COMPOSE + (["up", "-d", "controller"] if up else ["stop", "controller"])
    r = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=180)
    if r.returncode != 0:
        raise RuntimeError(f"제어기 {'시작' if up else '정지'} 실패: {r.stderr.strip()[-300:]}")
    print(f"제어기 {'시작' if up else '정지'} (capacity={capacity:g})", flush=True)


def replay_with_burst(day: str, speed: str, burst: str | None) -> None:
    cmd = [sys.executable, "-m", "databrain.replayer.main", day, "--speed", speed]
    if burst:
        cmd += ["--burst", burst]
    r = subprocess.run(cmd, capture_output=True, text=True, env=ENV, timeout=3600)
    if r.returncode != 0:
        raise RuntimeError(f"재생 실패: {(r.stderr or r.stdout).strip()[-500:]}")
    print("재생:", r.stdout.strip(), flush=True)


def read_decisions() -> list[Decision]:
    """ClickHouse `decisions`를 모형 교정이 쓰는 모양으로 읽는다.

    밀림은 **측정값**(raw_backlog_*)을 쓴다. backlog_* 는 추측 항법으로 앞으로 당겨 둔 값이라
    '들어온 것 − 읽은 것' 뺄셈에 넣으면 읽은 수가 틀어진다 (ADR-0008).
    """
    rows = []
    sql = ("SELECT toUnixTimestamp64Milli(at)/1000, in_cart_eps, in_view_eps, "
           "raw_backlog_cart, raw_backlog_view, cart, view FROM databrain.decisions ORDER BY at")
    for line in ch(sql).splitlines():
        if not line:
            continue
        a = line.split("\t")
        rows.append(Decision(at_s=float(a[0]), in_cart_eps=float(a[1]), in_view_eps=float(a[2]),
                             backlog_cart=float(a[3]), backlog_view=float(a[4]),
                             cart=float(a[5]), view=float(a[6])))
    return rows


def dump_decisions(rows: list[Decision], out_dir: str = "data/runs") -> str:
    """판단 기록을 파일로 남긴다.

    ClickHouse의 `decisions`는 다음 실행이 `ch_reset()`으로 비운다. 기록을 남겨 두지 않으면
    추정기를 고친 뒤 '그때 자료로 다시 재 보기'를 할 수 없어 실험을 처음부터 돌려야 한다
    (2026-10-19에 실제로 겪었다). `data/`는 .gitignore에 있다.
    """
    os.makedirs(out_dir, exist_ok=True)
    path = f"{out_dir}/decisions-{time.strftime('%Y%m%d-%H%M%S')}.tsv"
    cols = ("at_s", "in_cart_eps", "in_view_eps", "backlog_cart", "backlog_view", "cart", "view")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\t".join(cols) + "\n")
        for r in rows:
            f.write("\t".join(str(getattr(r, c)) for c in cols) + "\n")
    return path


def load_decisions(path: str) -> list[Decision]:
    """`dump_decisions`가 남긴 파일을 다시 읽는다 (추정기를 고친 뒤 재분석용)."""
    rows = []
    with open(path, encoding="utf-8") as f:
        next(f)
        for line in f:
            a = line.rstrip("\n").split("\t")
            if len(a) == 7:
                rows.append(Decision(*(float(x) for x in a)))
    return rows


def rate_story(rows: list[Decision]) -> dict:
    """제어기가 실제로 걸렀는지, 내려갔다 올라왔는지."""
    import statistics as st
    v = [r.view for r in rows]
    c = [r.cart for r in rows]
    bl = [r.backlog_cart + r.backlog_view for r in rows]
    shed = [i for i, x in enumerate(v) if x < 0.999]
    return {"판단 줄 수": len(rows),
            "조회 비율 최소/중앙/최대": [round(min(v), 4), round(st.median(v), 4), round(max(v), 4)],
            "장바구니 비율 최소/최대": [round(min(c), 4), round(max(c), 4)],
            "거른 초 수": len(shed),
            "비율이 내려갔다 올라왔나": bool(shed) and v[-1] > min(v) + 0.05,
            "최대 밀림": int(max(bl)) if bl else 0,
            "서로 다른 비율 값 수": len({round(x, 3) for x in v})}


def accuracy(day: str = "data/sub/ctl30") -> dict:
    """거른 뒤에도 추정 총량이 맞는가.

    **두 가지 오차를 모두 낸다** (2026-10-20에 고침).
    예전에는 'ClickHouse에 있는 칸'만 모아 비교해서 +0.42%로 보고했다. 그런데 통과 비율이
    바닥일 때는 어떤 칸의 사용자가 전부 걸러져 **행이 아예 안 생긴다**. 그 칸을 빼고 세면
    오차가 좋아 보인다. 실제로 누락 칸이 전체 조회의 4%를 쥐고 있었고,
    전체 정답 기준 오차는 -3.58%였다 (ADR-0018).

    화면 입장에서 중요한 쪽은 **전체 정답 기준**이다. 사라진 칸은 화면에 아무것도 안 뜨므로
    범주를 더해 만든 합계가 그만큼 모자란다.
    """
    t_main, _ = truth(day)
    last = max(m for _, m in t_main)
    want = {k: v for k, v in t_main.items() if k[1] != last and k[0].startswith("view|")}
    got = {}
    for line in ch("SELECT concat(event_type,'|',category_code), toUnixTimestamp(minute)*1000, "
                   "est_count, raw_count FROM databrain.agg_minute FINAL WHERE event_type='view'").splitlines():
        if line:
            k, m, e, raw = line.split("	")
            got[(k, int(m))] = (float(e), int(raw))
    seen = [k for k in want if k in got]
    missing = [k for k in want if k not in got]
    t_seen = sum(want[k] for k in seen)
    t_all = sum(want.values())
    t_missing = sum(want[k] for k in missing)
    e_tot = sum(got[k][0] for k in seen)
    r_tot = sum(got[k][1] for k in seen)
    return {"닫힌 조회 칸": len(want), "ClickHouse에 있는 칸": len(seen),
            "사라진 칸": len(missing),
            "사라진 칸이 쥔 조회": t_missing,
            "사라진 칸 비중%": round(t_missing / t_all * 100, 2) if t_all else None,
            "정답 총량(전체)": t_all, "정답 총량(있는 칸)": t_seen, "추정 총량": round(e_tot),
            "추정 오차%(있는 칸만, 낙관적)": round((e_tot - t_seen) / t_seen * 100, 2) if t_seen else None,
            "추정 오차%(전체 정답 기준)": round((e_tot - t_all) / t_all * 100, 2) if t_all else None,
            "실제 통과율%": round(r_tot / t_all * 100, 2) if t_all else None}


def run(day: str, speed: str, capacity: float, burst: str | None) -> dict:
    reset()
    ch_reset()
    controller(capacity, up=True)
    submit("vital_job", 1)
    submit("main_job", 2)
    time.sleep(8)                     # 제어기가 첫 비율을 내보낼 틈
    replay_with_burst(day, speed, burst)
    sent = produced()
    if sum(sent.values()) == 0:
        raise RuntimeError(f"이벤트 토픽이 비어 있다: {sent}")
    print("보낸 건수:", sent, flush=True)
    waited = wait_pipeline()
    controller(capacity, up=False)
    time.sleep(8)                     # 마지막 판단 기록이 ClickHouse에 들어갈 틈

    rows = read_decisions()
    dumped = dump_decisions(rows)
    print("판단 기록 저장:", dumped, flush=True)
    out = {"speed": speed, "capacity_given": capacity, "burst": burst,
           "sent": sent, "적재": waited, "판단 기록 파일": dumped,
           "제어기": rate_story(rows), "정확도": accuracy(day)}

    # ---- ADR-0014 2단계: 그날 기록만으로 처리 능력 되찾기
    # 두 추정기를 함께 보고한다. 1초 방식은 실제 기록에서 거부당한다 (ADR-0017)
    st = steps(rows)
    neg = sum(1 for x in st if x.read == 0.0)
    out["모형 교정"] = {"쓸 수 있는 구간": len(st),
                   "읽은 수가 음수여서 깎인 구간": f"{neg}/{len(st)}"}
    for name, f in (("1초 방식 (fit_capacity)", fit_capacity(rows)),
                    ("창 방식 (fit_from_rates, 20초)", fit_from_rates(rows, window_s=20.0))):
        out["모형 교정"][name] = {
            "꽉 찬 단위": f.n_saturated, "ok": f.ok, "이유": f.reason,
            "되찾은 capacity": round(f.capacity_eps, 1) if f.ok else None,
            "되찾은 filter_cost": round(f.filter_cost, 4) if f.ok else None,
            "r2": round(f.r2, 4) if f.ok else None,
            "준 값 대비%": round((f.capacity_eps / capacity - 1) * 100, 1) if f.ok else None}
    # 창 길이에 얼마나 민감한지도 같이 남긴다
    out["모형 교정"]["창 길이별 capacity"] = {
        f"{w:g}s": (round(fit_from_rates(rows, window_s=w).capacity_eps, 0)
                    if fit_from_rates(rows, window_s=w).ok else None)
        for w in (10.0, 15.0, 20.0, 30.0, 45.0)}
    return out


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "make":
        make(sys.argv[2])
    elif cmd == "run":
        burst = sys.argv[5] if len(sys.argv) > 5 else None
        print(json.dumps(run(sys.argv[2], sys.argv[3], float(sys.argv[4]), burst),
                         ensure_ascii=False, indent=1))
    else:
        raise SystemExit(__doc__)
