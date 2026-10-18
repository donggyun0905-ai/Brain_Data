"""브로커 없이 검증할 수 있는 핵심 로직 테스트. 실행: pytest -q"""
import math
import random

import pytest

from databrain import wire
from databrain.controller.lag import Snapshot, backlog, dead_reckon, drain_rate, in_flight, in_rate, parse_offsets
from databrain.controller.rate_model import CapacityModel, Rates, compute_rates
from databrain.replayer.faults import inject
from databrain.replayer.schedule import build_wall_clock
from databrain.sampling.hashing import keep, unit_hash, weight
from databrain.sampling.heavy import HeavyUserTracker


# ---------- 메시지 형식 (ADR-0005)
def test_wire_roundtrip_and_peek_does_not_parse_body():
    raw = wire.encode(42, "abc", 1569888000000, 1700000000000, {"price": 9.9, "brand": "삼성"})
    h = wire.peek(raw)
    assert (h.user_id, h.event_id, h.event_ms, h.ingest_ms) == ("42", "abc", 1569888000000, 1700000000000)
    assert h.body.startswith("{")                       # 본문은 문자열 그대로
    h2, body = wire.decode(raw)
    assert body == {"price": 9.9, "brand": "삼성"}


def test_wire_rejects_tab_in_ids():
    with pytest.raises(ValueError):
        wire.encode("a\tb", 1, 0, 0, {})


# ---------- 혼합 샘플링 (ADR-0003)
def test_hash_is_stable_and_in_unit_interval():
    assert unit_hash("u:1") == unit_hash("u:1")
    assert all(0 <= unit_hash(f"u:{i}") < 1 for i in range(1000))


def test_nested_thresholds_keep_sessions_consistent():
    users = [str(i) for i in range(5000)]
    at5 = {u for u in users if keep(u, "e", 0.05, heavy=False)}
    at10 = {u for u in users if keep(u, "e", 0.10, heavy=False)}
    assert at5 <= at10                                   # 5%에 뽑힌 사용자는 10%에도 항상 포함


def test_user_hash_keeps_or_drops_whole_user():
    for u in map(str, range(200)):
        decisions = {keep(u, str(e), 0.3, heavy=False) for e in range(20)}
        assert len(decisions) == 1


def test_heavy_users_are_sampled_per_event():
    decisions = [keep("whale", str(e), 0.3, heavy=True) for e in range(2000)]
    assert 0.25 < sum(decisions) / len(decisions) < 0.35


def test_horvitz_thompson_estimate_is_close():
    rng = random.Random(1)
    events = [(str(rng.randrange(20000)), str(i)) for i in range(200_000)]
    rate = 0.1
    est = sum(weight(rate) for u, e in events if keep(u, e, rate, heavy=False))
    assert abs(est - len(events)) / len(events) < 0.05


# ---------- 상위 사용자 판별
def test_heavy_tracker_finds_whale_and_decays():
    t = HeavyUserTracker(tau_ms=30 * 60_000, threshold=20)
    for i in range(30):                      # 10초마다 1건 → 5분에 30건
        t.add("whale", i * 10_000)
    for i in range(5):                       # 1분마다 1건
        t.add("user7", i * 60_000)
    assert t.is_heavy("whale")
    assert not t.is_heavy("user7")
    t.add("whale", 30 * 10_000 + 2 * 3_600_000)   # 2시간 쉬면 거의 0으로 줄어든다
    assert not t.is_heavy("whale")


def test_heavy_tracker_same_result_regardless_of_count_and_late_events():
    t = HeavyUserTracker(tau_ms=60_000)
    t.add("u", 120_000)
    assert t.add("u", 60_000) == pytest.approx(2.0)   # 늦게 온 이벤트는 감쇠 없이 더함
    assert t.add("u", 180_000) == pytest.approx(2.0 * math.exp(-1) + 1)


def test_heavy_tracker_prunes_idle_users():
    t = HeavyUserTracker(tau_ms=60_000, prune_every_ms=60_000, prune_below=0.05)
    t.add("idle", 0)
    t.add("busy", 10 * 60_000)               # 10분 뒤 정리 → idle은 e^-10 로 지워짐
    assert "idle" not in t.users and "busy" in t.users


# ---------- 예측형 제어기 (ADR-0002)
M = CapacityModel(capacity_eps=10_000)


def test_no_load_keeps_everything():
    r = compute_rates(M, 100, 1000, 0, 0, Rates())
    assert (r.cart, r.view) == (1.0, 1.0) and r.version == 0


def test_overload_cuts_view_before_cart():
    r = compute_rates(M, 2_000, 50_000, 0, 0, Rates())
    assert r.cart == 1.0 and r.view < 0.2 and r.version == 1


def test_extreme_overload_then_cuts_cart_but_not_below_floor():
    r = compute_rates(M, 30_000, 300_000, 500_000, 5_000_000, Rates())
    assert r.view == M.min_view and M.min_cart <= r.cart < 1.0


def test_backlog_alone_triggers_shedding():
    r = compute_rates(M, 300, 5_000, 0, 200_000, Rates())
    assert r.view < 1.0


def test_rise_is_limited_but_drop_is_immediate():
    low = Rates(cart=1.0, view=0.05, version=3)
    up = compute_rates(M, 100, 1000, 0, 0, low)
    assert up.view == pytest.approx(0.15) and up.version == 4
    down = compute_rates(M, 2_000, 80_000, 0, 0, Rates())
    assert down.view < 0.15


def test_kept_load_fits_capacity_with_margin():
    in_c, in_v = 3_000, 60_000
    r = compute_rates(M, in_c, in_v, 0, 0, Rates())
    used = (in_c + in_v) * M.filter_cost + in_c * r.cart + in_v * r.view
    assert used <= M.margin * M.capacity_eps * 1.001


# ---------- 밀림 계산
def test_backlog_and_in_rate():
    a = Snapshot(0.0, {0: 100, 1: 100}, {0: 90, 1: 100})
    b = Snapshot(2.0, {0: 300, 1: 140}, {0: 150, 1: 120})
    assert backlog(a) == 10 and backlog(b) == 170
    assert in_rate(a, b) == pytest.approx(120.0)
    assert in_rate(None, b) == 0.0


def test_backlog_prefers_flink_position_and_falls_back_per_partition():
    s = Snapshot(0.0, {0: 100, 1: 100}, {0: 10, 1: 20}, processed={0: 95})
    assert backlog(s) == 5 + 80                       # 파티션 1은 Flink 값이 없어 커밋 위치 사용
    assert backlog(s, use_processed=False) == 90 + 80


def test_parse_flink_offsets():
    m = [{"id": "0.Source__src_view[1].KafkaSourceReader.topic.events_view.partition.3.currentOffset", "value": "41"},
         {"id": "0.Source__src_view[1].KafkaSourceReader.topic.events_view.partition.4.currentOffset", "value": "-1"},
         {"id": "0.Source__src_view[1].KafkaSourceReader.topic.replay_events_view.partition.0.currentOffset", "value": "9"},
         {"id": "0.Source__src_view[1].KafkaSourceReader.topic.events_view.partition.3.committedOffset", "value": "7"}]
    assert parse_offsets(m, ["events.view", "events.cart"]) == {"events.view": {3: 42}, "events.cart": {}}


def test_in_flight_subtracts_control_messages():
    assert in_flight({"events.view": 900, "events.cart": 100}, control_out=3, filter_in=3) == {"events.view": 900, "events.cart": 100}
    assert in_flight({"events.view": 900, "events.cart": 100}, control_out=3, filter_in=1003) == {"events.view": 0, "events.cart": 0}


def test_dead_reckoning_removes_what_flink_has_drained_since():
    m = CapacityModel(capacity_eps=1000)
    r = Rates()                                       # 다 통과: 이벤트 하나에 1 + 거르기 0.05
    assert drain_rate(m, r, 0, 1000) == pytest.approx(1000 / 1.05)
    bc, bv = dead_reckon(m, r, 100, 900, age_s=0.5)
    assert bc + bv == pytest.approx(1000 - 500 / 1.05)
    assert bc / bv == pytest.approx(100 / 900)        # 두 경로는 쌓인 비중대로 줄어듦
    assert dead_reckon(m, r, 100, 900, age_s=60) == (0.0, 0.0)
    # Flink가 멈춰도 오래된 측정으로 밀림을 0으로 착각하지 않도록 경과 시간 상한
    bc, bv = dead_reckon(m, r, 0, 100_000, age_s=600, max_age_s=15)
    assert bv == pytest.approx(100_000 - 15_000 / 1.05)
    assert dead_reckon(m, r, 0, 0, age_s=5) == (0, 0)


# ---------- 재생 시간표
def test_wall_clock_speed_and_burst():
    w = build_wall_clock(100)
    assert w(1000) == pytest.approx(10.0)
    wb = build_wall_clock(100, [(100, 100, 10)])
    # 0~100초는 1초, 100~200초(10배 폭증)는 0.1초, 200~300초는 1초
    assert wb(300) == pytest.approx(2.1)
    ts = [wb(t) for t in range(0, 400, 7)]
    assert ts == sorted(ts)


# ---------- 문제 주입
def test_injection_returns_ground_truth():
    evs = [(float(i), {"event_id": str(i)}) for i in range(10_000)]
    out, log = inject(evs, dup_rate=0.02, delay_rate=0.05, seed=3)
    assert len(out) == len(evs) + len(log.duplicated)
    assert 150 < len(log.duplicated) < 250 and 400 < len(log.delayed) < 600
    assert [t for t, _ in out] == sorted(t for t, _ in out)
