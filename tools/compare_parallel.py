"""거르기 작업을 여러 개로 늘렸을 때 상위 사용자 판정이 어떻게 달라지는지 본다 (일지 2-3의 미확인 문제).

    python tools/compare_parallel.py data/parquet/2019-11/day=2019-11-17 14
    python tools/compare_parallel.py data/parquet/2019-11/day=2019-11-17 14 5 10   # 가짜 봇 5개, 각 분당 10건

## 왜 문제가 되는가
상위 사용자 판정은 `HeavyUserTracker`가 들고 있는 dict로 한다 (ADR-0010). 이 dict는
**거르기 작업(서브태스크)마다 따로** 있다. 그런데 조회 토픽은 12칸, 장바구니 토픽은 6칸이라
(`infra/kafka/create_topics.sh`) 같은 사용자의 조회와 장바구니가 서로 다른 서브태스크로 갈 수 있다.
그러면 한 사용자의 건수가 작업마다 쪼개져 세어지고, 50건 기준에 못 미쳐 상위 판정이 사라질 수 있다.

일지 2-3은 "조회가 94%라 영향은 작을 것으로 추측"이라고만 적어 두고 AWS에서 확인하기로 미뤘다.
이 도구는 그 추측을 실제 데이터로 미리 재어 본다 (브로커 없이, 집 PC·EC2를 기다리지 않고).

## 모형 (둘 다 가정이다. 집 PC에서 실제 클러스터로 확인할 것)
- Kafka 칸 배정: librdkafka 기본 분배기 `consistent_random`은 키가 있으면 `crc32(키) % 칸수`를 쓴다.
  재생기는 confluent-kafka(librdkafka)를 쓰므로 이 방식이다. Java 기본값인 murmur2와는 다르다
  → `--partitioner murmur2`로 바꿔 돌려 보면 결과가 배정 방식에 민감한지 알 수 있다
- Flink 칸→서브태스크 배정: `KafkaSourceEnumerator.getSplitOwner`
  `시작 = ((토픽이름.hashCode() * 31) & 0x7FFFFFFF) % 병렬도`, `서브태스크 = (시작 + 칸) % 병렬도`

## 지표
- 판정 불일치%      : 병렬 1(정답)과 상위 판정이 달라진 조회 비율
- 판정 사라진 조회% : 정답에서는 상위였는데 이 병렬도에서는 아닌 조회 비율 (이게 손해의 본체)
- 경로 갈림 사용자% : 조회와 장바구니가 서로 다른 서브태스크로 가는 사용자 비율
- 그 아래 지표들은 `compare_heavy.py`와 같다 (ADR-0011 표와 직접 비교할 수 있게 재사용)
"""
from __future__ import annotations

import json
import sys
import zlib

import numpy as np

sys.path.insert(0, "src")
sys.path.insert(0, "tools")
from compare_heavy import RATES, add_bots, evaluate, load  # noqa: E402
from databrain.sampling.heavy import HeavyUserTracker  # noqa: E402

LANES = {"view": ("events.view", 12), "cart": ("events.cart", 6)}   # create_topics.sh와 같아야 한다
TAU_MS, THRESHOLD = 30 * 60_000, 50.0                               # ADR-0011에서 고른 기준
PARALLELS = (1, 2, 4, 6, 12)


def java_hash(s: str) -> int:
    """Java String.hashCode (32비트 넘침 포함). Flink가 토픽 이름을 이 값으로 섞는다."""
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    return h - (1 << 32) if h >= (1 << 31) else h


def murmur2(data: bytes) -> int:
    """Kafka Java 클라이언트 기본 분배기가 쓰는 해시 (비교용)."""
    length, seed = len(data), 0x9747B28C
    m, r = 0x5BD1E995, 24
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(0, length - length % 4, 4):
        k = int.from_bytes(data[i:i + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    rem = length % 4
    if rem == 3:
        h ^= (data[length - 1] << 16) | (data[length - 2] << 8) | data[length - 3]
        h = (h * m) & 0xFFFFFFFF
    elif rem == 2:
        h ^= (data[length - 2] << 8) | data[length - 1]
        h = (h * m) & 0xFFFFFFFF
    elif rem == 1:
        h ^= data[length - 1]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h & 0x7FFFFFFF        # Java는 부호를 떼고 쓴다


def partition_of(user_id: str, n_parts: int, how: str) -> int:
    key = user_id.encode()
    return (murmur2(key) if how == "murmur2" else zlib.crc32(key)) % n_parts


def subtask_of(topic: str, partition: int, parallel: int) -> int:
    start = ((java_hash(topic) * 31) & 0x7FFFFFFF) % parallel
    return (start + partition) % parallel


def heavy_flags(df, parallel: int, how: str):
    """병렬도 parallel에서 서브태스크마다 따로 센 상위 판정. 반환: (판정 배열, 서브태스크 배열)."""
    uids, ms, lanes = df.user_id.values, df.ms.values.astype(np.int64), df.lane.values
    flags = np.empty(len(df), bool)
    subtasks = np.empty(len(df), np.int16)
    trackers: dict[int, HeavyUserTracker] = {}
    route: dict[tuple, int] = {}                       # (사용자, 경로) → 서브태스크 (한 번만 계산)
    for i in range(len(df)):
        us, lane = str(uids[i]), lanes[i]
        key = (us, lane)
        st = route.get(key)
        if st is None:
            topic, n_parts = LANES[lane]
            st = route[key] = subtask_of(topic, partition_of(us, n_parts, how), parallel)
        t = trackers.get(st)
        if t is None:
            t = trackers[st] = HeavyUserTracker(tau_ms=TAU_MS, threshold=THRESHOLD)
        t.add(us, int(ms[i]))
        flags[i], subtasks[i] = t.is_heavy(us), st
    return flags, subtasks


def split_users(df, parallel: int, how: str) -> float:
    """조회와 장바구니가 서로 다른 서브태스크로 가는 사용자 비율 (장바구니도 있는 사용자 중)."""
    (vt, vn), (ct, cn) = LANES["view"], LANES["cart"]
    both, split = 0, 0
    for us in {str(x) for x in df.user_id[df.lane == "cart"].values}:
        sv = subtask_of(vt, partition_of(us, vn, how), parallel)
        sc = subtask_of(ct, partition_of(us, cn, how), parallel)
        both += 1
        split += sv != sc
    return split / both * 100 if both else float("nan")


def main(day: str, hour: int, n_bots: int = 0, per_min: float = 0, how: str = "crc32") -> None:
    df, bot = load(day, hour), None
    if n_bots:
        df, bot = add_bots(df, n_bots, per_min)
    u = np.unique(df.user_id.values, return_inverse=True)[1]
    ms = df.ms.values.astype(np.int64)
    mi = ms // 60000 - ms.min() // 60000
    view = df.lane.values == "view"
    groups = [range(o, o + 10) for o in (0, 100, 200)]

    base = None
    report = {}
    for p in PARALLELS:
        flags, subtasks = heavy_flags(df, p, how)
        if base is None:
            base = flags
        row = {"상위 판정 조회%": round(flags[view].mean() * 100, 1),
               "판정 불일치%": round((flags != base)[view].mean() * 100, 2),
               "판정 사라진 조회%": round((base & ~flags)[view].mean() * 100, 2),
               "판정 생긴 조회%": round((~base & flags)[view].mean() * 100, 2),
               "경로 갈림 사용자%": round(split_users(df, p, how), 1),
               "쓰인 서브태스크": int(len(np.unique(subtasks)))}
        if bot is not None:
            row["봇 판정%"] = round(flags[bot].mean() * 100, 1)
        runs = [evaluate(df, u, mi, view, flags, g, bot) for g in groups]
        for r in RATES:
            med = [sorted(x) for x in zip(*(run[r] for run in runs))]
            row[str(r)] = {m: [round(v[1], 1), round(v[0], 1), round(v[2], 1)] for m, v in
                           zip(("분 총량 p95%", "범주·분 p95%", "퍼널 p95%p", "쪼개진 세션%",
                                "세션 통째 보존%", "경로 보존%", "체류 시간 틀림%", "담기 전 조회%",
                                "봇 범주·분 p95%"), med) if not np.isnan(v[1])}
        report[f"병렬 {p}"] = row
    print(json.dumps({"day": day, "hour": hour, "partitioner": how, "bots": [n_bots, per_min],
                      "events": len(df), "views": int(view.sum()),
                      "topics": {k: v[1] for k, v in LANES.items()}, "res": report}, ensure_ascii=False))


if __name__ == "__main__":
    a = sys.argv[1:]
    kw = {}
    if len(a) > 4:
        kw = {"n_bots": int(a[2]), "per_min": float(a[3]), "how": a[4]}
    elif len(a) > 3:
        kw = {"n_bots": int(a[2]), "per_min": float(a[3])}
    elif len(a) > 2:
        kw = {"how": a[2]}
    main(a[0], int(a[1]), **kw)
