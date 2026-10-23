from collections import defaultdict
from datetime import datetime, timezone

import pytest

from fraudstream import rules
from fraudstream.simulator import FRAUD_PATTERNS, TransactionSimulator

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def features(txns):
    return {"txn_count": len(txns), "total_amount": sum(t["amount"] for t in txns),
            "country_count": len({t["country"] for t in txns})}


@pytest.mark.parametrize("feats,expected", [
    ({"txn_count": 5, "total_amount": 100, "country_count": 1}, []),
    ({"txn_count": 6, "total_amount": 100, "country_count": 1}, ["VELOCITY"]),
    ({"txn_count": 3, "total_amount": 3000, "country_count": 3}, ["MULTI_COUNTRY", "HIGH_SPEND"]),
])
def test_window_rule_thresholds(feats, expected):
    assert rules.evaluate_window(feats) == expected


def test_score_is_capped_and_threshold_matches_single_rules():
    assert rules.score(["VELOCITY", "MULTI_COUNTRY", "HIGH_SPEND"]) == 1.0
    for r in rules.WINDOW_RULES:  # every single rule is strong enough to alert on its own
        assert rules.score([r.name]) >= rules.ALERT_THRESHOLD


def test_txn_rule_needs_all_three_conditions():
    base = {"merchant_category": "crypto_exchange", "card_present": False, "amount": 2000}
    assert rules.evaluate_txn(base) == ["HIGH_RISK_MERCHANT"]
    assert rules.evaluate_txn(base | {"card_present": True}) == []
    assert rules.evaluate_txn(base | {"amount": 100}) == []
    assert rules.evaluate_txn(base | {"merchant_category": "grocery"}) == []


def test_alert_id_is_deterministic():
    assert rules.make_alert_id("card_1", "w:x") == rules.make_alert_id("card_1", "w:x")
    assert rules.make_alert_id("card_1", "w:x") != rules.make_alert_id("card_2", "w:x")


def test_simulator_is_reproducible_with_seed():
    a = TransactionSimulator(n_cards=100, seed=7).fraud_burst(NOW, "velocity")
    b = TransactionSimulator(n_cards=100, seed=7).fraud_burst(NOW, "velocity")
    assert a == b


@pytest.mark.parametrize("pattern", FRAUD_PATTERNS)
def test_every_fraud_pattern_is_caught_by_some_rule(pattern):
    sim = TransactionSimulator(n_cards=100, seed=1)
    for _ in range(25):
        burst = sim.fraud_burst(NOW, pattern)
        assert all(t["is_fraud"] for t in burst)
        triggered = rules.evaluate_window(features(burst)) + [
            r for t in burst for r in rules.evaluate_txn(t)]
        assert rules.score(triggered) >= rules.ALERT_THRESHOLD, (pattern, burst)


def test_normal_traffic_rarely_alerts():
    """5 minutes of normal traffic at 20 tps: false-positive windows should be rare."""
    sim = TransactionSimulator(n_cards=10_000, fraud_rate=0.0, seed=3)
    by_card = defaultdict(list)
    for _ in range(20 * 300):
        t = sim.normal_txn(NOW)
        by_card[t["card_id"]].append(t)
    flagged = [c for c, txns in by_card.items()
               if rules.score(rules.evaluate_window(features(txns))) >= rules.ALERT_THRESHOLD]
    assert len(flagged) <= 3
    assert all(t["amount"] > 0 for txns in by_card.values() for t in txns)
