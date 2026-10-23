"""Fraud rules, defined once and used by both Spark and plain Python.

Window rules run on per-card features computed over a 5-minute sliding window.
Transaction rules run on a single transaction (no state needed).
Each triggered rule adds its weight to the risk score (capped at 1.0); an alert
is raised when the score reaches ALERT_THRESHOLD.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Mapping, Sequence

WINDOW_DURATION = "5 minutes"
WINDOW_SLIDE = "1 minute"
WATERMARK = "2 minutes"
ALERT_THRESHOLD = 0.5

HIGH_RISK_CATEGORIES = ("crypto_exchange", "gift_cards", "money_transfer")


@dataclass(frozen=True)
class WindowRule:
    name: str
    feature: str
    threshold: float
    weight: float
    description: str


WINDOW_RULES: tuple[WindowRule, ...] = (
    WindowRule("VELOCITY", "txn_count", 6, 0.5, "6+ transactions on one card in 5 min"),
    WindowRule("MULTI_COUNTRY", "country_count", 3, 0.6, "Card used in 3+ countries in 5 min"),
    WindowRule("HIGH_SPEND", "total_amount", 3000.0, 0.5, "$3,000+ spent on one card in 5 min"),
)

TXN_RULE_NAME = "HIGH_RISK_MERCHANT"
TXN_RULE_WEIGHT = 0.7
TXN_RULE_MIN_AMOUNT = 1500.0


def score(triggered: Sequence[str]) -> float:
    weights = {r.name: r.weight for r in WINDOW_RULES} | {TXN_RULE_NAME: TXN_RULE_WEIGHT}
    return round(min(1.0, sum(weights[name] for name in triggered)), 3)


def evaluate_window(features: Mapping[str, float]) -> list[str]:
    """Return the names of window rules triggered by one card's window features."""
    return [r.name for r in WINDOW_RULES if features.get(r.feature, 0) >= r.threshold]


def evaluate_txn(txn: Mapping) -> list[str]:
    """Card-not-present, high-value purchase at a high-risk merchant category."""
    hit = (
        txn["merchant_category"] in HIGH_RISK_CATEGORIES
        and not txn["card_present"]
        and txn["amount"] >= TXN_RULE_MIN_AMOUNT
    )
    return [TXN_RULE_NAME] if hit else []


def make_alert_id(card_id: str, scope: str) -> str:
    """Deterministic ID so re-emitted alerts (retries, Spark update mode) are de-duplicated."""
    return hashlib.sha1(f"{card_id}|{scope}".encode()).hexdigest()[:20]
