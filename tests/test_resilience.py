import pytest

from fraudstream.alert_service import AlertProcessor, DownstreamError
from fraudstream.resilience import (BreakerState, CircuitBreaker, IdempotencyStore,
                                    retry_with_backoff)


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


class ScriptedClient:
    """Fails the first `failures` calls, then succeeds."""

    def __init__(self, failures=0, exc=DownstreamError):
        self.failures, self.exc, self.calls = failures, exc, 0

    def create_case(self, alert):
        self.calls += 1
        if self.calls <= self.failures:
            raise self.exc("boom")
        return "case-1"


def alert(i=1, card="card_1"):
    return {"alert_id": f"a{i}", "card_id": card, "rules": ["VELOCITY"], "score": 0.5,
            "labelled_fraud_txns": 3, "detected_at": "2026-10-01T12:00:00Z"}


def make_processor(client, **kw):
    breaker = kw.pop("breaker", CircuitBreaker(failure_threshold=3, reset_timeout_s=30))
    return AlertProcessor(IdempotencyStore(), client, breaker, max_retries=kw.pop("max_retries", 3),
                          cooldown_s=kw.pop("cooldown_s", 300), sleep=lambda s: None, **kw)


# ---- circuit breaker ----------------------------------------------------
def test_breaker_opens_after_threshold_and_half_opens_after_timeout():
    clock = FakeClock()
    b = CircuitBreaker(failure_threshold=3, reset_timeout_s=10, clock=clock)
    for _ in range(3):
        assert b.allow()
        b.record_failure()
    assert b.state is BreakerState.OPEN and not b.allow()
    clock.t = 10
    assert b.allow() and b.state is BreakerState.HALF_OPEN
    b.record_failure()                       # failed probe -> straight back to open
    assert b.state is BreakerState.OPEN
    clock.t = 20
    assert b.allow()
    b.record_success()
    assert b.state is BreakerState.CLOSED


# ---- retries ------------------------------------------------------------
def test_retry_recovers_from_transient_failures():
    client = ScriptedClient(failures=2)
    assert retry_with_backoff(lambda: client.create_case({}), retries=3, sleep=lambda s: None) == "case-1"
    assert client.calls == 3


def test_retry_gives_up_and_reraises():
    client = ScriptedClient(failures=10)
    with pytest.raises(DownstreamError):
        retry_with_backoff(lambda: client.create_case({}), retries=2, sleep=lambda s: None)
    assert client.calls == 3


def test_retry_does_not_retry_unexpected_errors():
    client = ScriptedClient(failures=1, exc=ValueError)
    with pytest.raises(ValueError):
        retry_with_backoff(lambda: client.create_case({}), retries=3,
                           retry_on=(DownstreamError,), sleep=lambda s: None)
    assert client.calls == 1


# ---- processor end to end ----------------------------------------------
def test_duplicate_alert_is_processed_once():
    client = ScriptedClient()
    p = make_processor(client)
    assert p.handle(alert(1)) == "delivered"
    assert p.handle(alert(1)) == "duplicate"
    assert client.calls == 1


def test_overlapping_windows_for_same_card_are_suppressed():
    p = make_processor(ScriptedClient())
    assert p.handle(alert(1, "card_9")) == "delivered"
    assert p.handle(alert(2, "card_9")) == "suppressed"
    assert p.handle(alert(3, "card_10")) == "delivered"


def test_transient_failure_still_delivers():
    assert make_processor(ScriptedClient(failures=2)).handle(alert()) == "delivered"


def test_persistent_failure_dead_letters_and_opens_breaker():
    client = ScriptedClient(failures=100)
    breaker = CircuitBreaker(failure_threshold=3, reset_timeout_s=30)
    p = make_processor(client, breaker=breaker)
    assert p.handle(alert(1, "c1")) == "dead_lettered"
    assert breaker.state is BreakerState.OPEN
    calls = client.calls
    assert p.handle(alert(2, "c2")) == "dead_lettered"   # fails fast: breaker open, no call made
    assert client.calls == calls
    assert {a["status"] for a in p.store.recent()} == {"dead_lettered"}


def test_timeouts_are_treated_as_retryable():
    assert make_processor(ScriptedClient(failures=1, exc=TimeoutError)).handle(alert()) == "delivered"
