"""수면 재생 모형 교정 테스트 (ADR-0014). 브로커 없이 돌아간다.

정답을 아는 가짜 하루를 만들어(`synth`) 기록만 보고 원래 숫자를 되찾는지 본다.
가짜 하루의 처리 비용 모형은 시뮬레이터(`sim/compare_controllers.py`)와 같은 식을 쓴다.
"""
import pytest

from databrain.sleep.fit import Decision, fit_capacity, steps

CAP, FC = 10_000.0, 0.05


def synth(cap=CAP, fc=FC, arrivals=None, view_rates=None, cart_frac=0.05, dt=1.0):
    """정답 (cap, fc)로 돌아가는 가짜 하루의 판단 기록을 만든다."""
    bc = bv = 0.0
    rows = []
    for i, (a, kv) in enumerate(zip(arrivals, view_rates)):
        ac, av = a * dt * cart_frac, a * dt * (1 - cart_frac)
        qc, qv = bc + ac, bv + av                       # 지금 시각에 쌓여 있는 양
        rows.append(Decision(at_s=i * dt, in_cart_eps=ac / dt, in_view_eps=av / dt,
                             backlog_cart=qc, backlog_view=qv, cart=1.0, view=kv))
        q = qc + qv
        if q <= 0:
            bc = bv = 0.0
            continue
        cost = fc + (qc * 1.0 + qv * kv) / q            # 읽은 이벤트 하나당 작업량
        take = min(cap * dt / cost, q)
        f = take / q
        bc, bv = qc * (1 - f), qv * (1 - f)
    return rows


def burst_day(n=300):
    """앞 20초는 한가하고, 그 뒤는 처리 능력을 크게 넘는 유입. 통과 비율은 1.0에서 0.02까지 쓸어내린다."""
    arrivals = [2_000.0] * 20 + [60_000.0] * (n - 20)
    rates = [1.0] * 20 + [max(0.02, 1.0 - 0.98 * i / (n - 21)) for i in range(n - 20)]
    return synth(arrivals=arrivals, view_rates=rates)


# ---------- 뺄셈으로 읽은 수를 되찾는다
def test_steps_recovers_read_and_kept():
    rows = burst_day()
    st = steps(rows)
    assert len(st) == len(rows) - 1
    # 꽉 찬 구간에서는 작업량이 처리 능력과 같아야 한다
    sat = [s for s in st if s.backlog_start > s.arrivals]
    assert len(sat) > 100
    for s in sat[:50]:
        work = FC * s.read + s.kept
        assert work == pytest.approx(CAP, rel=1e-6)


# ---------- 기록만 보고 정답을 되찾는다
def test_fit_recovers_capacity_and_filter_cost():
    f = fit_capacity(burst_day())
    assert f.ok, f.reason
    assert f.capacity_eps == pytest.approx(CAP, rel=0.02)
    assert f.filter_cost == pytest.approx(FC, rel=0.05)
    assert f.r2 > 0.99


@pytest.mark.parametrize("cap,fc", [(1_250.0, 0.05), (10_000.0, 0.20), (40_000.0, 0.01)])
def test_fit_works_for_other_truths(cap, fc):
    n = 300
    arrivals = [2_000.0] * 20 + [cap * 8] * (n - 20)
    rates = [1.0] * 20 + [max(0.02, 1.0 - 0.98 * i / (n - 21)) for i in range(n - 20)]
    f = fit_capacity(synth(cap=cap, fc=fc, arrivals=arrivals, view_rates=rates))
    assert f.ok, f.reason
    assert f.capacity_eps == pytest.approx(cap, rel=0.03)
    assert f.filter_cost == pytest.approx(fc, rel=0.10)


# ---------- 믿을 수 없을 때는 믿지 않는다
def test_fit_refuses_when_never_saturated():
    n = 200
    rows = synth(arrivals=[500.0] * n, view_rates=[1.0] * n)     # 처리 능력에 한참 못 미치는 유입
    f = fit_capacity(rows)
    assert not f.ok and "꽉 찬 구간" in f.reason


def test_fit_refuses_when_rate_never_changes():
    """비율이 고정이면 읽은 수가 거의 일정해서 직선을 정할 수 없다."""
    n = 300
    rows = synth(arrivals=[2_000.0] * 20 + [60_000.0] * (n - 20), view_rates=[1.0] * n)
    f = fit_capacity(rows)
    assert not f.ok and "거의 일정" in f.reason


def test_fit_refuses_on_empty_or_short_log():
    assert not fit_capacity([]).ok
    assert not fit_capacity(burst_day()[:5]).ok


# ---------- 잡음이 있어도 크게 틀리지 않는다
def test_fit_tolerates_backlog_noise():
    """밀림 측정에 절대 오차를 섞는다. 밀림은 폭증 중 수백만까지 커지므로 비율 오차가 아니라
    '몇백 건 어긋남'이 실제 모습이다 (처리 위치 신호가 묵은 정도에서 온다. ADR-0008)."""
    import random
    rng = random.Random(7)
    noisy = [Decision(r.at_s, r.in_cart_eps, r.in_view_eps,
                      max(0.0, r.backlog_cart + rng.uniform(-300, 300)),
                      max(0.0, r.backlog_view + rng.uniform(-300, 300)), r.cart, r.view)
             for r in burst_day()]
    f = fit_capacity(noisy)
    assert f.ok, f.reason
    assert f.capacity_eps == pytest.approx(CAP, rel=0.15)


# ---------- 창 방식 추정기 (ADR-0017)
# 실제 기록에서는 1초 단위 뺄셈이 깨진다. 여러 초를 묶고, 기록에 남은 비율로 맞추는 방식.
from databrain.sleep.fit import effective_rate, fit_from_rates, rate_windows  # noqa: E402


@pytest.mark.parametrize("cap,fc", [(1_200.0, 0.05), (10_000.0, 0.05), (1_250.0, 0.20), (40_000.0, 0.01)])
def test_fit_from_rates_recovers_truth(cap, fc):
    n = 400
    arrivals = [2_000.0] * 20 + [cap * 8] * (n - 20)
    rates = [1.0] * 20 + [max(0.02, 1.0 - 0.98 * i / (n - 21)) for i in range(n - 20)]
    f = fit_from_rates(synth(cap=cap, fc=fc, arrivals=arrivals, view_rates=rates), window_s=20.0)
    assert f.ok, f.reason
    assert f.capacity_eps == pytest.approx(cap, rel=0.05)
    assert f.filter_cost == pytest.approx(fc, rel=0.15)
    assert f.r2 > 0.98


def test_window_read_telescopes_past_per_second_noise():
    """창 방식은 창 양 끝 밀림만 쓰므로, 중간 초의 밀림이 흔들려도 읽은 총수가 그대로다.

    1초 방식이 실제 기록에서 깨진 이유가 바로 이것이다 (초마다 음수를 0으로 깎으면 상쇄가 깨진다).
    """
    import random
    rng = random.Random(11)
    rows = burst_day()
    clean = rate_windows(rows, window_s=20.0)
    jittered = [Decision(r.at_s, r.in_cart_eps, r.in_view_eps,
                         max(0.0, r.backlog_cart + rng.uniform(-2000, 2000)),
                         max(0.0, r.backlog_view + rng.uniform(-2000, 2000)), r.cart, r.view)
                if 0 < i < len(rows) - 1 else r                      # 양 끝은 그대로 둔다
                for i, r in enumerate(rows)]
    noisy = rate_windows(jittered, window_s=20.0)
    # 창 경계가 아닌 초의 잡음은 읽은 총수에 영향이 거의 없다
    inner = [(a, b) for a, b in zip(clean, noisy)][1:-1]
    assert inner
    for a, b in inner:
        assert b.read == pytest.approx(a.read, rel=0.05)


def test_effective_rate_weights_by_queue_mix():
    d = Decision(0.0, 0.0, 0.0, backlog_cart=100.0, backlog_view=900.0, cart=1.0, view=0.1)
    assert effective_rate(d) == pytest.approx((100 * 1.0 + 900 * 0.1) / 1000)
    empty = Decision(0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.5)
    assert effective_rate(empty) == pytest.approx(0.75)      # 큐가 비면 단순 평균


def test_fit_from_rates_refuses_without_enough_full_windows():
    n = 200
    rows = synth(arrivals=[500.0] * n, view_rates=[1.0] * n)      # 늘 한가함
    f = fit_from_rates(rows, window_s=20.0)
    assert not f.ok and "꽉 찬 창" in f.reason
