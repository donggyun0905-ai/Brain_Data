"""H2(정직한 정확도) 성공 기준을 실제 데이터로 잰다 (ADR-0015).

    python tools/measure_h2.py data/parquet/2019-11/day=2019-11-17 14

기획서의 H2 성공 기준
  ① 화면에 표시한 95% 오차 범위가 실제값을 95% 안팎으로 포함
  ③ 통과 비율 10% 이상에서 **5분 단위** 조회수 오차 3% 이내   ← 기획서에 '실측 필요'로 남아 있던 항목

②(퍼널 전환율 편향 1%p 이내)는 `tools/compare_heavy.py`가 이미 재므로 여기서는 다루지 않는다.

거르기는 실제 코드(`sampling/hashing.keep`)와 실제 상위 사용자 기준(`sampling/heavy`, 30분 감쇠 50건)을
그대로 쓴다. 오차 범위는 `sampling/interval`의 묶음 분산을 쓴다.

적중률은 '칸마다 한 번'이 아니라 **시도마다 모든 칸을 세서** 모은다.
한 칸의 95% 구간이 실제값을 품었는지가 베르누이 시행이고, 칸과 시도를 합쳐 비율을 낸다.
"""
from __future__ import annotations

import json
import sys

import numpy as np

sys.path.insert(0, "src")
sys.path.insert(0, "tools")
from compare_heavy import load  # noqa: E402
from databrain.sampling.heavy import HeavyUserTracker  # noqa: E402
from databrain.sampling.hashing import keep  # noqa: E402
from databrain.sampling.interval import Z95, ClusterIndex, variance_part  # noqa: E402

RATES = (0.5, 0.2, 0.1, 0.05)
WINDOWS = {"1분": 60_000, "5분": 300_000}
MIN_TRUE = 100           # 이 미만인 칸은 화면에 띄울 값이 아니므로 제외 (compare_heavy와 같은 기준)
TRIALS = 30


def heavy_flags(df):
    """실제 기준(30분 감쇠 50건)으로 이벤트마다 상위 여부를 매긴다."""
    t = HeavyUserTracker()
    uids, ms = df.user_id.values, df.ms.values.astype(np.int64)
    out = np.empty(len(df), bool)
    for i in range(len(df)):
        us = str(uids[i])
        t.add(us, int(ms[i]))
        out[i] = t.is_heavy(us)
    return out


def measure(df, heavy, window_ms: int, rate: float, trials: int, seed0: int = 0):
    """반환: (적중률, 칸 수, 조회수 오차 p95%, 평균 상대 반폭%)"""
    view = df.lane.values == "view"
    vi = np.flatnonzero(view)
    uids = df.user_id.values[vi].astype(str)
    ms = df.ms.values.astype(np.int64)[vi]
    cats = df.category_code.values[vi].astype(str)
    hv = heavy[vi]
    win = ms // window_ms - ms.min() // window_ms
    cell_key = np.char.add(np.char.add(cats, "|"), win.astype(str))
    cells, cell_idx = np.unique(cell_key, return_inverse=True)
    truth = np.bincount(cell_idx, minlength=len(cells)).astype(float)
    big = truth >= MIN_TRUE

    tot_win = np.bincount(win)                      # 창별 전체 조회수 (지표 ③)
    hits = shown = 0
    cell_err, halfw = [], []
    win_err = []
    for t in range(trials):
        ci = ClusterIndex()
        est = np.zeros(len(cells))
        var = np.zeros(len(cells))
        est_win = np.zeros(len(tot_win))
        for k in range(len(vi)):
            u = f"{uids[k]}@{seed0 + t}"            # 시도마다 사용자 해시를 바꾼다
            if not keep(u, f"e{k}", rate, heavy=bool(hv[k])):
                continue
            c = cell_idx[k]
            idx = ci.next(u, cells[c], int(win[k]), rate, bool(hv[k]))
            est[c] += 1.0 / rate
            var[c] += variance_part(rate, idx)
            est_win[win[k]] += 1.0 / rate
        half = Z95 * np.sqrt(var[big])
        lo, hi = np.maximum(0.0, est[big] - half), est[big] + half
        hits += int(((lo <= truth[big]) & (truth[big] <= hi)).sum())
        shown += int(big.sum())
        cell_err.append(np.abs(est[big] - truth[big]) / truth[big])
        halfw.append(half / np.maximum(truth[big], 1))
        m = tot_win >= MIN_TRUE
        win_err.append(np.abs(est_win[m] - tot_win[m]) / tot_win[m])
    return (hits / shown, int(big.sum()),
            float(np.percentile(np.concatenate(win_err), 95) * 100),
            float(np.percentile(np.concatenate(cell_err), 95) * 100),
            float(np.mean(np.concatenate(halfw)) * 100))


def main(day: str, hour: int, trials: int = TRIALS) -> None:
    df = load(day, hour)
    heavy = heavy_flags(df)
    out = {}
    for wname, wms in WINDOWS.items():
        for r in RATES:
            cov, n_cells, win_p95, cell_p95, halfw = measure(df, heavy, wms, r, trials)
            out[f"{wname}/{r}"] = {"적중률%": round(cov * 100, 1), "칸 수": n_cells,
                                   "창 총량 오차 p95%": round(win_p95, 2),
                                   "범주·창 오차 p95%": round(cell_p95, 1),
                                   "평균 상대 반폭%": round(halfw, 1)}
    print(json.dumps({"day": day, "hour": hour, "trials": trials,
                      "events": len(df), "views": int((df.lane.values == "view").sum()),
                      "heavy_view%": round(float(heavy[df.lane.values == "view"].mean()) * 100, 2),
                      "res": out}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]), *(int(sys.argv[3]),) if len(sys.argv) > 3 else ())
