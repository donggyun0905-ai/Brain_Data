"""오차 범위 적중률 테스트 (H2 기준 ①, ADR-0015). 브로커 없이 돌아간다.

'95% 구간이 실제값을 95% 안팎으로 포함하는가'를 몬테카를로로 센다.
거르기는 실제 코드(`sampling/hashing.keep`)를 그대로 쓰고, 정답은 만들 때부터 알고 있다.
"""
import pytest

from databrain.sampling.hashing import keep
from databrain.sampling.interval import ClusterIndex, interval, variance_part

CELL = "view|electronics.smartphone"


def one_cell(users, rate, salt, heavy_users=frozenset()):
    """한 칸을 한 번 거른다. users = {사용자: 그 칸의 이벤트 수}.

    반환: (추정치, 모아 둔 흔들림, 실제값)
    `salt`는 시도마다 사용자 해시를 다르게 하려고 사용자 이름에 붙인다 (해시가 고정이라 그냥 돌리면 늘 같은 결과).
    """
    ci = ClusterIndex()
    est = var = 0.0
    truth = sum(users.values())
    for u, m in users.items():
        uid = f"{u}@{salt}"
        hv = u in heavy_users
        for e in range(m):
            if not keep(uid, f"e{e}", rate, heavy=hv):
                continue
            idx = ci.next(uid, CELL, 0, rate, hv)
            est += 1.0 / rate
            var += variance_part(rate, idx)
    return est, var, truth


def coverage(users, rate, trials=1200, heavy_users=frozenset()):
    hit = 0
    widths = []
    for t in range(trials):
        est, var, truth = one_cell(users, rate, t, heavy_users)
        iv = interval(est, var)
        hit += iv.lo <= truth <= iv.hi
        widths.append(iv.half_width)
    return hit / trials, sum(widths) / len(widths)


# ---------- 식이 맞는지 (작은 경우를 손으로 확인)
def test_variance_part_builds_squared_cluster_size():
    """묶음 i번째마다 2i-1을 더하면 합이 A²이다."""
    r = 0.25
    for a in range(1, 8):
        got = sum(variance_part(r, i) for i in range(1, a + 1))
        assert got == pytest.approx(a * a * (1 - r) / (r * r))


def test_heavy_events_are_their_own_cluster():
    ci = ClusterIndex()
    assert [ci.next("u1", CELL, 0, 0.1, heavy=True) for _ in range(5)] == [1, 1, 1, 1, 1]
    assert [ci.next("u2", CELL, 0, 0.1, heavy=False) for _ in range(5)] == [1, 2, 3, 4, 5]


def test_cluster_index_resets_each_window_and_splits_by_rate():
    ci = ClusterIndex()
    assert ci.next("u", CELL, 0, 0.10, heavy=False) == 1
    assert ci.next("u", CELL, 0, 0.10, heavy=False) == 2
    assert ci.next("u", CELL, 0, 0.50, heavy=False) == 1      # 비율이 다르면 다른 묶음
    assert ci.next("u", CELL, 60_000, 0.10, heavy=False) == 1  # 창이 바뀌면 처음부터


def test_interval_is_zero_width_when_nothing_is_dropped():
    assert variance_part(1.0, 1) == 0.0
    iv = interval(500.0, 0.0)
    assert (iv.lo, iv.hi, iv.half_width) == (500.0, 500.0, 0.0)


def test_interval_clips_at_zero():
    assert interval(5.0, 100.0).lo == 0.0


# ---------- 적중률 (H2 기준 ①)
@pytest.mark.parametrize("rate", [0.5, 0.2, 0.1])
def test_coverage_for_user_hash_only(rate):
    """상위 사용자가 없는 칸. 긴 꼬리로 사용자마다 이벤트 수가 다르다."""
    users = {f"u{i}": 1 + (i % 7) for i in range(400)}
    cov, _ = coverage(users, rate)
    assert 0.90 <= cov <= 0.99, f"적중률 {cov:.3f}"


@pytest.mark.parametrize("rate", [0.5, 0.2, 0.1])
def test_coverage_with_heavy_users_mixed(rate):
    """상위 사용자 몇 명이 큰 몫을 차지하는 칸 (실제 모습에 가깝다)."""
    users = {f"u{i}": 1 + (i % 5) for i in range(300)}
    users.update({f"whale{j}": 200 for j in range(3)})
    cov, _ = coverage(users, rate, heavy_users={f"whale{j}" for j in range(3)})
    assert 0.90 <= cov <= 0.99, f"적중률 {cov:.3f}"


def test_naive_binomial_interval_is_too_narrow():
    """단순 이항 분산을 쓰면 구간이 좁아 적중률이 95%에 한참 못 미친다 (이 모듈이 필요한 이유)."""
    users = {f"u{i}": 1 + (i % 7) for i in range(400)}
    rate, hit = 0.1, 0
    for t in range(800):
        est, _, truth = one_cell(users, rate, t)
        naive_var = est * (1 - rate) / rate          # 모든 이벤트가 독립이라고 본 경우
        iv = interval(est, naive_var)
        hit += iv.lo <= truth <= iv.hi
    assert hit / 800 < 0.80, f"단순 분산 적중률 {hit / 800:.3f} (너무 높으면 이 테스트가 의미 없음)"
