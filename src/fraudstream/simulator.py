"""Synthetic card-transaction generator with injected fraud patterns.

Every transaction carries a ground-truth `is_fraud` label. The rules never read it;
it is only used to measure alert precision on the dashboard.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone

COUNTRIES = ["IN", "US", "GB", "SG", "AE", "DE", "FR", "JP", "BR", "NG"]
NORMAL_CATEGORIES = ["grocery", "fuel", "restaurant", "travel", "electronics", "pharmacy", "apparel"]
RISKY_CATEGORIES = ["crypto_exchange", "gift_cards", "money_transfer"]
FRAUD_PATTERNS = ("velocity", "multi_country", "high_spend", "risky_merchant")


class TransactionSimulator:
    def __init__(self, n_cards: int = 10_000, fraud_rate: float = 0.01, seed: int | None = None):
        self.rng = random.Random(seed)
        self.fraud_rate = fraud_rate
        self.cards = [f"card_{i:06d}" for i in range(n_cards)]
        self.home_country = {c: self.rng.choices(COUNTRIES, weights=[60, 10, 6, 5, 5, 4, 4, 3, 2, 1])[0]
                             for c in self.cards}

    # ---- building blocks -------------------------------------------------
    def _txn(self, card: str, ts: datetime, amount: float, country: str, category: str,
             card_present: bool, is_fraud: bool, pattern: str | None) -> dict:
        return {
            "txn_id": str(uuid.UUID(int=self.rng.getrandbits(128))),
            "card_id": card,
            "amount": round(amount, 2),
            "currency": "USD",
            "merchant_id": f"m_{self.rng.randint(1, 5000):05d}",
            "merchant_category": category,
            "country": country,
            "card_present": card_present,
            "event_time": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "is_fraud": is_fraud,
            "fraud_pattern": pattern,
        }

    def normal_txn(self, now: datetime) -> dict:
        card = self.rng.choice(self.cards)
        home = self.home_country[card]
        country = home if self.rng.random() < 0.97 else self.rng.choice(COUNTRIES)
        amount = min(self.rng.lognormvariate(3.6, 1.0), 2500)  # median ~ $37
        return self._txn(card, now, amount, country, self.rng.choice(NORMAL_CATEGORIES),
                         self.rng.random() < 0.6, False, None)

    def fraud_burst(self, now: datetime, pattern: str | None = None) -> list[dict]:
        pattern = pattern or self.rng.choice(FRAUD_PATTERNS)
        card = self.rng.choice(self.cards)
        home = self.home_country[card]

        def at(sec: float) -> datetime:
            return now + timedelta(seconds=sec)

        if pattern == "velocity":  # card testing: many small online charges in ~1 minute
            return [self._txn(card, at(i * self.rng.uniform(4, 9)), self.rng.uniform(1, 25), home,
                              self.rng.choice(NORMAL_CATEGORIES), False, True, pattern)
                    for i in range(self.rng.randint(7, 10))]
        if pattern == "multi_country":  # cloned card used in several countries
            countries = self.rng.sample([c for c in COUNTRIES if c != home], 3)
            return [self._txn(card, at(i * 40), self.rng.uniform(80, 400), c,
                              self.rng.choice(NORMAL_CATEGORIES), True, True, pattern)
                    for i, c in enumerate([home, *countries])]
        if pattern == "high_spend":  # account takeover: a few large electronics purchases
            return [self._txn(card, at(i * 50), self.rng.uniform(1000, 1800), home,
                              "electronics", False, True, pattern) for i in range(3)]
        # risky_merchant: one large card-not-present cash-out
        return [self._txn(card, now, self.rng.uniform(1500, 4000), self.rng.choice(COUNTRIES),
                          self.rng.choice(RISKY_CATEGORIES), False, True, "risky_merchant")]

    def next_batch(self, now: datetime | None = None) -> list[dict]:
        now = now or datetime.now(timezone.utc)
        if self.rng.random() < self.fraud_rate:
            return self.fraud_burst(now)
        return [self.normal_txn(now)]
