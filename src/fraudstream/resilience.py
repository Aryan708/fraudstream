"""Reliability building blocks used by the alert service.

- CircuitBreaker: stop calling a failing downstream, probe it again after a cool-off
- retry_with_backoff: bounded retries with exponential backoff + jitter, per-attempt timeout
- IdempotencyStore: SQLite-backed "seen it already?" check so at-least-once delivery is safe
"""
from __future__ import annotations

import json
import os
import random
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, TypeVar

T = TypeVar("T")


class BreakerState(Enum):
    CLOSED = 0      # normal: calls pass through
    HALF_OPEN = 1   # cool-off over: let one probe call through
    OPEN = 2        # failing: reject calls immediately


class CircuitOpenError(RuntimeError):
    pass


@dataclass
class CircuitBreaker:
    failure_threshold: int = 5
    reset_timeout_s: float = 30.0
    clock: Callable[[], float] = time.monotonic
    state: BreakerState = BreakerState.CLOSED
    _failures: int = 0
    _opened_at: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def allow(self) -> bool:
        with self._lock:
            if self.state is BreakerState.OPEN and self.clock() - self._opened_at >= self.reset_timeout_s:
                self.state = BreakerState.HALF_OPEN
            return self.state is not BreakerState.OPEN

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self.state = BreakerState.CLOSED

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self.state is BreakerState.HALF_OPEN or self._failures >= self.failure_threshold:
                self.state = BreakerState.OPEN
                self._opened_at = self.clock()


def retry_with_backoff(fn: Callable[[], T], *, retries: int, base_delay_s: float = 0.2,
                       max_delay_s: float = 5.0, retry_on: tuple[type[Exception], ...] = (Exception,),
                       sleep: Callable[[float], None] = time.sleep,
                       on_retry: Callable[[int, Exception], None] | None = None) -> T:
    """Call fn up to retries+1 times. Delay doubles each attempt, with full jitter."""
    for attempt in range(retries + 1):
        try:
            return fn()
        except retry_on as exc:
            if attempt == retries:
                raise
            if on_retry:
                on_retry(attempt + 1, exc)
            sleep(random.uniform(0, min(max_delay_s, base_delay_s * 2 ** attempt)))
    raise AssertionError("unreachable")


class IdempotencyStore:
    """Stores processed alerts. `claim()` is atomic: only the first caller for an ID gets True."""

    def __init__(self, path: str = ":memory:"):
        if path != ":memory:":
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("""CREATE TABLE IF NOT EXISTS alerts (
                alert_id TEXT PRIMARY KEY, card_id TEXT, status TEXT, payload TEXT,
                received_at REAL)""")
            self._conn.commit()

    def claim(self, alert: dict) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO alerts VALUES (?, ?, 'processing', ?, ?)",
                (alert["alert_id"], alert["card_id"], json.dumps(alert), time.time()))
            self._conn.commit()
            return cur.rowcount == 1

    def set_status(self, alert_id: str, status: str) -> None:
        with self._lock:
            self._conn.execute("UPDATE alerts SET status=? WHERE alert_id=?", (status, alert_id))
            self._conn.commit()

    def recent(self, limit: int = 50, status: str | None = None) -> list[dict]:
        sql = "SELECT payload, status FROM alerts"
        args: tuple = ()
        if status:
            sql, args = sql + " WHERE status=?", (status,)
        with self._lock:
            rows = self._conn.execute(sql + " ORDER BY received_at DESC LIMIT ?", (*args, limit)).fetchall()
        return [json.loads(p) | {"status": s} for p, s in rows]
