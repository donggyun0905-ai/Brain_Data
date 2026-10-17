"""
제어기 밀림 신호 비교 (ADR-0008).

문제: 제어기는 '소비자 그룹 커밋 위치'로 밀림을 계산하는데 Flink는 체크포인트 때만 커밋한다.
     → 이미 처리한 데이터를 아직 밀린 것으로 보고 조회를 쓸데없이 버린다 (2026-10-17 실측).

이 시뮬레이션은 실제 제어 계산(databrain.controller.rate_model.compute_rates)을 그대로 쓰고,
'제어기가 보는 처리 위치'만 바꿔 가며 결과를 비교한다.

  신호(제어기가 보는 처리 위치)
    commit:C    체크포인트 C초마다 커밋된 위치 (+ 체크포인트 완료까지 걸린 시간 뒤에 보임)
    rest:F      Flink REST의 소스 currentOffset. 작업 관리자가 F초마다 모아 둔 값
                (Python 묶음 처리 때문에 실제 처리보다 BUNDLE건 앞서 있음)
    rest+:F     REST에 측정 지점 하나 추가: 소스가 내보낸 수 - 거르기 단계가 받은 수(이동 중)를 더해 앞섬 보정
    hybrid:F    rest+:F를 쓰되, REST가 응답하지 않으면 커밋 10초 + 추측항법으로 바꿈 (대비책)
  장애: '장애' 장면은 '하루'와 같되 200~260초 동안 REST가 응답하지 않는다 (작업 재시작 등).
       대비책이 없으면 제어기는 마지막으로 받은 값을 계속 쓴다
    push:P      Flink 작업이 P초마다 진행 위치를 Kafka 토픽에 직접 보냄
    true        지금 이 순간의 실제 위치 (이론상 최선, 비교 기준)
  보정(묵은 값을 지금 값으로 추정)
    raw         그대로 사용 (지금 방식)
    dr          추측 항법: 묵은 위치 + 처리 능력 모델로 계산한 처리 속도 × 경과 시간
    obs         추측 항법: 묵은 위치 + 최근 두 측정 사이에 실제로 처리한 속도 × 경과 시간
  제어 루프
    T           판단 주기 (초)
    rise        1초에 올릴 수 있는 비율 폭 (판단 주기가 짧으면 한 번에 올리는 폭도 작아짐)
    dead        작은 밀림(처리 능력 × dead초 이하)은 0으로 봄
    win         유입 속도 계산 구간 (초). 'T'면 판단 주기와 같음(지금 방식)

    python sim/compare_lag_signals.py            # 표 출력 (3개 씨앗, 중앙값과 범위)
"""
from __future__ import annotations

import json
import math
import os
import sys
from collections import deque
from dataclasses import dataclass, field

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from databrain.controller.rate_model import CapacityModel, Rates, compute_rates  # noqa: E402

DT = 0.05
CAP = 1250.0             # 진짜 처리 능력 (이벤트/초). PC 실측 중앙값 1,234~1,260 (bench, 2026-10-18)
FC = 0.05                # 거르는 판단 비용 (무거운 처리 1 대비)
APPLY_DELAY = 0.2        # 제어 신호가 Flink에 닿기까지 (추측)
CK_DONE = 5.0            # 커밋이 실제 처리보다 늦는 기본 시간 (초). 실측 밀림 과대치/처리량에 맞춤:
                         #   10초 주기 11,016건(≈8.9초), 2초·1초 주기 7,000건(≈5.6초) → 5초 + 주기/2
OUTAGE = (200.0, 260.0)  # '장애' 장면에서 REST가 응답하지 않는 구간 (초)
BUNDLE = 1000            # Python 묶음 처리 때문에 소스가 거르기 단계보다 앞서 읽는 양 (실측 중앙값 1,000건)


@dataclass
class Cfg:
    name: str
    signal: str = "commit:10"
    est: str = "raw"
    T: float = 1.0
    rise: float = 0.10
    dead: float = 0.0
    win: str = "T"
    cap_est: float = 1.0           # 제어기가 아는 처리 능력 / 진짜 처리 능력
    ck_cost_s: float = 0.0         # 체크포인트 한 번에 잃는 처리 시간 (초)
    extra: dict = field(default_factory=dict)


# ---------------- 부하 모양 ----------------
def scenario(kind: str, rng) -> tuple[np.ndarray, list[tuple[float, float]]]:
    """(초당 유입 [장바구니, 조회] 배열, 한꺼번에 들어오는 덩어리 [(시각, 건수)])"""
    if kind == "dump":          # 실측 장면: 평소 20% 부하 + 2만 건이 2.7초에 쏟아짐 (따라잡기)
        n = int(120 / DT)
        rate = np.tile([0.2 * CAP * 0.066, 0.2 * CAP * 0.934], (n, 1))
        return rate, [(30.0, 20000.0)]
    if kind == "burst":         # 진짜 폭증: 평소 50%, 조회 8배 60초 (10초 경사)
        n = int(240 / DT); t = np.arange(n) * DT
        rate = np.tile([0.5 * CAP * 0.066, 0.5 * CAP * 0.934], (n, 1))
        shape = np.minimum(np.clip((t - 60) / 10, 0, 1), np.clip((130 - t) / 10, 0, 1))
        rate[:, 0] *= 1 + shape * 1.0
        rate[:, 1] *= 1 + shape * 7.0
        return rate, []
    if kind in ("day", "outage"):   # 무작위 하루 조각: 평소 40%, 폭증 3번 + 따라잡기 덩어리 2번
        n = int(600 / DT); t = np.arange(n) * DT
        rate = np.tile([0.4 * CAP * 0.066, 0.4 * CAP * 0.934], (n, 1))
        rate *= (1 + 0.15 * np.sin(2 * np.pi * t / 200))[:, None]
        for _ in range(3):
            s = rng.uniform(20, 520); d = rng.uniform(15, 60); m = rng.uniform(2, 6)
            shape = np.minimum(np.clip((t - s) / 5, 0, 1), np.clip((s + d - t) / 5, 0, 1))
            rate[:, 1] *= 1 + shape * (m - 1)
            rate[:, 0] *= 1 + shape * (m / 3)
        dumps = [(float(rng.uniform(20, 580)), float(rng.uniform(5000, 20000))) for _ in range(2)]
        return rate, dumps
    raise ValueError(kind)


# ---------------- 시뮬레이션 ----------------
def run(cfg: Cfg, kind: str, seed: int) -> dict:
    rng = np.random.default_rng(seed)
    rate, dumps = scenario(kind, rng)
    n = len(rate)
    kind_sig, _, par = cfg.signal.partition(":")
    par = float(par) if par else 0.0
    model = CapacityModel(capacity_eps=CAP * cfg.cap_est, filter_cost=FC)
    # 신호 주기와 제어기 판단 시각은 서로 맞춰져 있지 않다 → 어긋남을 무작위로
    phase = float(np.random.default_rng(seed + 100).uniform(0, par)) if par else 0.0

    q = deque()                       # Kafka에 쌓인 [도착시각, 장바구니, 조회]
    end = np.zeros(2)                 # 끝 위치 (누적 도착)
    pos = np.zeros(2)                 # 실제 처리 위치 (누적 읽음)
    hist_pos = np.zeros((n + 1, 2))   # 시각별 처리 위치 (지연 신호 재현용)
    hist_end = np.zeros((n + 1, 2))
    rates = Rates(); applied = deque([(0.0, 1.0, 1.0)]); kc = kv = 1.0
    lat, lw = [], []
    read_in = np.zeros(2); kept = np.zeros(2)
    changes = 0
    lost_until = -1.0
    period = max(1, round(cfg.T / DT))
    win_steps = period if cfg.win == "T" else max(1, round(float(cfg.win) / DT))
    last_meas = None                  # (측정 시각, 위치) — obs 보정용
    prev_meas = None
    dumps = sorted(dumps)
    use_dr = False
    for k in range(n):
        now = k * DT
        a = rng.poisson(rate[k] * DT).astype(float)
        while dumps and dumps[0][0] <= now:
            _, cnt = dumps.pop(0)
            a += rng.multinomial(int(cnt), [0.066, 0.934])
        if a.sum() > 0:
            q.append([now, a[0], a[1]])
        end += a
        # 제어 신호 적용 (조금 늦게 닿음)
        while len(applied) > 1 and applied[1][0] <= now:
            applied.popleft()
        _, kc, kv = applied[0]
        # 체크포인트 비용: 체크포인트 순간 잠깐 멈춘다고 단순화
        if cfg.ck_cost_s > 0 and kind_sig == "commit" and (now % par) < DT:
            lost_until = now + cfg.ck_cost_s
        budget = 0.0 if now < lost_until else CAP * DT
        while budget > 1e-9 and q:
            t0, c, v = q[0]
            tot = c + v
            cost = FC + (c * kc + v * kv) / tot
            take = min(budget / cost, tot)
            f = take / tot
            rc, rv = c * f, v * f
            kcc, kvv = rc * kc, rv * kv
            read_in += [rc, rv]; kept += [kcc, kvv]
            if kcc + kvv > 0:
                lat.append(now + DT - t0); lw.append(kcc + kvv)
            pos += [rc, rv]
            budget -= take * cost
            q[0][1] -= rc; q[0][2] -= rv
            if take >= tot - 1e-9:
                q.popleft()
        hist_pos[k + 1] = pos; hist_end[k + 1] = end

        if k % period:
            continue
        # ---- 제어기가 보는 처리 위치 ----
        def at(t):  # 시각 t의 처리 위치
            return hist_pos[max(0, min(k + 1, int(round(t / DT)) + 1))]
        if kind_sig == "true":
            tm = now
        elif kind_sig == "commit":     # 체크포인트 시작 시각의 위치가, 완료된 뒤에 보임
            tm = math.floor((now - CK_DONE - phase) / par) * par + phase
        elif kind_sig in ("rest", "rest+", "hybrid"):   # 작업 관리자가 F초마다 모아 둔 값
            tm = math.floor((now - phase) / par) * par + phase
            if kind == "outage" and OUTAGE[0] <= now < OUTAGE[1]:
                if kind_sig == "hybrid":   # 대비책: 커밋 위치 + 추측항법
                    tm = math.floor((now - CK_DONE - phase) / 10) * 10 + phase
                    use_dr = True
                else:                      # 마지막으로 받은 값에 머묾
                    tm = min(tm, OUTAGE[0])
        elif kind_sig == "push":       # 작업이 P초마다 보냄 + 전달 50ms
            tm = math.floor((now - 0.05 - phase) / par) * par + phase
        tm = max(0.0, tm)
        meas = at(tm).copy()
        if kind_sig == "rest":         # 소스는 거르기 단계보다 묶음 하나만큼 앞서 읽는다
            ahead = np.minimum(np.maximum(hist_end[int(round(tm / DT))] - meas, 0), BUNDLE * meas / max(meas.sum(), 1))
            meas = meas + ahead
        if last_meas is None or tm > last_meas[0]:
            prev_meas, last_meas = last_meas, (tm, meas)
        age = now - tm
        est = meas.copy()
        est_kind = "dr" if use_dr else cfg.est
        use_dr = False
        if est_kind != "raw" and age > 0:
            bl = np.maximum(end - meas, 0)
            if bl.sum() > 0:
                if est_kind == "dr":     # 처리 능력 모델로 지금까지 처리했을 양을 더함
                    w = (bl[0] * rates.cart + bl[1] * rates.view) / bl.sum()
                    out = model.capacity_eps / (FC + w)
                elif est_kind == "obs":  # 최근 두 측정 사이 실제 처리 속도
                    out = 0.0
                    if prev_meas is not None and last_meas[0] > prev_meas[0]:
                        out = (last_meas[1].sum() - prev_meas[1].sum()) / (last_meas[0] - prev_meas[0])
                est = meas + np.minimum(bl, bl / bl.sum() * out * age)
        backlog = np.maximum(end - est, 0)
        if cfg.dead > 0 and backlog.sum() < model.capacity_eps * cfg.dead:
            backlog = np.zeros(2)
        j0 = max(0, k + 1 - win_steps)
        span = (k + 1 - j0) * DT
        inr = (hist_end[k + 1] - hist_end[j0]) / span
        m = CapacityModel(capacity_eps=model.capacity_eps, filter_cost=FC, max_rise=cfg.rise * cfg.T)
        new = compute_rates(m, inr[0], inr[1], backlog[0], backlog[1], rates)
        if new.version != rates.version:
            changes += 1
            applied.append((now + APPLY_DELAY, new.cart, new.view))
        rates = new

    v = np.asarray(lat); w = np.asarray(lw)
    o = np.argsort(v); v, w = v[o], w[o]
    p99 = float(v[np.searchsorted(np.cumsum(w) / w.sum(), 0.99)]) if len(v) else 0.0
    return {"fresh_p99": p99, "view_drop": float(1 - kept[1] / max(read_in[1], 1)),
            "cart_drop": float(1 - kept[0] / max(read_in[0], 1)), "changes": changes}


def summarize(cfg: Cfg, seeds=(1, 2, 3)) -> dict:
    out = {}
    for kind in ("dump", "burst", "day", "outage"):
        rs = [run(cfg, kind, s) for s in seeds]
        out[kind] = {k: (float(np.median([r[k] for r in rs])), float(min(r[k] for r in rs)),
                         float(max(r[k] for r in rs))) for k in rs[0]}
    return out


CONFIGS = [
    # 1) 신호만 바꿈 (보정 없음, 1초 판단)
    Cfg("A0 커밋 10초 (지금)", "commit:10"),
    Cfg("A1 커밋 5초", "commit:5"),
    Cfg("A2 커밋 2초", "commit:2"),
    Cfg("A3 커밋 1초", "commit:1"),
    Cfg("B0 REST 10초 (기본 설정)", "rest:10"),
    Cfg("B1 REST 1초", "rest:1"),
    Cfg("B2 REST 0.25초", "rest:0.25"),
    Cfg("B3 REST 1초, 능력 50% 과대추정", "rest:1", cap_est=1.5),
    Cfg("B4 REST+ 1초 (이동 중 보정)", "rest+:1"),
    Cfg("B5 REST+ 0.25초 (이동 중 보정)", "rest+:0.25"),
    Cfg("C1 진행 토픽 1초", "push:1"),
    Cfg("C2 진행 토픽 0.25초", "push:0.25"),
    Cfg("Z  실제 위치 (이론 최선)", "true"),
    # 2) 묵은 신호 + 보정
    Cfg("D1 커밋 10초 + 추측항법(모델)", "commit:10", "dr"),
    Cfg("D2 커밋 10초 + 추측항법(실측속도)", "commit:10", "obs"),
    Cfg("D3 커밋 2초 + 추측항법(모델)", "commit:2", "dr"),
    Cfg("D4 REST 1초 + 추측항법(모델)", "rest:1", "dr"),
    Cfg("D5 커밋 10초 + 추측항법, 능력 30% 과소추정", "commit:10", "dr", cap_est=0.7),
    Cfg("D6 커밋 10초 + 추측항법, 능력 50% 과대추정", "commit:10", "dr", cap_est=1.5),
    Cfg("D7 REST 1초 + 추측항법, 능력 50% 과대추정", "rest:1", "dr", cap_est=1.5),
    # 3) 제어 루프 손질 (신호는 REST 1초)
    Cfg("E1 REST 1초 + 판단 0.5초", "rest:1", T=0.5, win="1"),
    Cfg("E2 REST 0.25초 + 판단 0.25초", "rest:0.25", T=0.25, win="1"),
    Cfg("E3 REST 0.25초 + 판단 0.25초 (유입 0.25초 구간)", "rest:0.25", T=0.25, win="T"),
    Cfg("E4 REST 1초 + 상승 폭 0.25", "rest:1", rise=0.25),
    Cfg("E5 REST 1초 + 작은 밀림 무시(0.5초분)", "rest:1", dead=0.5),
    Cfg("E6 커밋 10초 + 판단 0.25초", "commit:10", T=0.25, win="1"),
    # 4) 조합
    Cfg("F1 REST 0.25초 + 추측항법 + 판단 0.25초", "rest:0.25", "dr", T=0.25, win="1"),
    Cfg("F2 REST 1초 + 추측항법 + 판단 0.5초 + 상승 0.25", "rest:1", "dr", T=0.5, win="1", rise=0.25),
    Cfg("F3 진행 토픽 0.25초 + 판단 0.25초 + 상승 0.25", "push:0.25", T=0.25, win="1", rise=0.25),
    Cfg("F4 커밋 2초 + 추측항법 + 판단 0.5초", "commit:2", "dr", T=0.5, win="1"),
    Cfg("F5 REST+ 0.25초 + 추측항법 + 판단 0.5초 + 상승 0.25", "rest+:0.25", "dr", T=0.5, win="1", rise=0.25),
    Cfg("G1 F5 + 장애 때 커밋·추측항법 대비책", "hybrid:0.25", "dr", T=0.5, win="1", rise=0.25),
    Cfg("G2 REST+ 1초 + 추측항법 + 대비책 (판단 1초)", "hybrid:1", "dr"),
    Cfg("G3 G2 + 판단 0.5초 + 상승 0.25", "hybrid:1", "dr", T=0.5, win="1", rise=0.25),
    Cfg("G4 G2, 능력 50% 과대추정", "hybrid:1", "dr", cap_est=1.5),
    Cfg("G5 G2, 능력 30% 과소추정", "hybrid:1", "dr", cap_est=0.7),
]


if __name__ == "__main__":
    only = os.environ.get("ONLY")          # 예: ONLY=G 이면 이름이 G로 시작하는 것만
    if only:
        CONFIGS[:] = [c for c in CONFIGS if c.name.startswith(tuple(only.split(",")))]
    ck_cost = float(os.environ.get("CK_COST_S", "0"))   # 체크포인트 비용 (bench_signals 실측)
    res = {}
    for c in CONFIGS:
        if c.signal.startswith("commit"):
            c.ck_cost_s = ck_cost
        res[c.name] = summarize(c)
        r = res[c.name]
        f = lambda x, d=1, mul=1: f"{x[0]*mul:.{d}f} ({x[1]*mul:.{d}f}~{x[2]*mul:.{d}f})"
        print(f"{c.name:46s} | 덩어리: 조회 버림 {f(r['dump']['view_drop'],0,100)}% p99 {f(r['dump']['fresh_p99'])}s"
              f" | 폭증: 버림 {f(r['burst']['view_drop'],0,100)}% p99 {f(r['burst']['fresh_p99'])}s 조절 {r['burst']['changes'][0]:.0f}"
              f" | 하루: 버림 {f(r['day']['view_drop'],0,100)}% p99 {f(r['day']['fresh_p99'])}s 조절 {r['day']['changes'][0]:.0f}"
              f" | 장애: 버림 {f(r['outage']['view_drop'],0,100)}% p99 {f(r['outage']['fresh_p99'])}s",
              flush=True)
    json.dump(res, open(os.environ.get("OUT", "lag_signals.json"), "w", encoding="utf-8"), ensure_ascii=False, indent=1)
