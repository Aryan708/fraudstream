"""Alert service: consumes fraud alerts, de-duplicates them, and delivers them to a
(simulated) case-management system for investigators.

Reliability patterns, each visible in metrics:
  idempotency        - SQLite store keyed by deterministic alert_id (Spark update mode re-emits)
  suppression        - one open case per card per cooldown window (sliding windows overlap)
  retries + timeout  - exponential backoff with jitter on transient downstream failures
  circuit breaker    - stop hammering a failing downstream; fail fast, probe later
  dead-letter queue  - alerts that can't be delivered (or can't be parsed) are parked, never lost
  at-least-once      - offsets are committed only after an alert is fully handled
"""
from __future__ import annotations

import json
import random
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime

from fastapi import FastAPI, Response
from prometheus_client import Counter, Gauge, Histogram, make_asgi_app

from .config import settings
from .logging_utils import get_logger
from .resilience import (BreakerState, CircuitBreaker, CircuitOpenError, IdempotencyStore,
                         retry_with_backoff)

log = get_logger("alert-service")

CONSUMED = Counter("fs_alerts_consumed_total", "Alert messages read from Kafka")
OUTCOMES = Counter("fs_alerts_outcome_total", "How each alert was handled", ["outcome"])
DELIVERED_BY_RULE = Counter("fs_alerts_delivered_total", "Alerts delivered, by rule", ["rule"])
LABELLED = Counter("fs_alerts_labelled_total",
                   "Delivered alerts by ground-truth label (for precision)", ["truth"])
RETRIES = Counter("fs_delivery_retries_total", "Retry attempts against the case-management API")
PROCESS_SECONDS = Histogram("fs_alert_processing_seconds", "Time to handle one alert",
                            buckets=(0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
DETECTION_LAG = Histogram("fs_alert_detection_lag_seconds",
                          "Time from Spark detection to case creation",
                          buckets=(0.5, 1, 2, 5, 10, 20, 30, 60, 120))
BREAKER_STATE = Gauge("fs_circuit_breaker_state", "0=closed 1=half-open 2=open")
CONSUMER_LAG = Gauge("fs_consumer_lag_messages", "Messages not yet consumed", ["partition"])


class DownstreamError(RuntimeError):
    pass


@dataclass
class CaseManagementClient:
    """Stand-in for the investigators' case API. Fails and times out at configurable rates
    so the retry / breaker / DLQ paths actually get exercised."""
    failure_rate: float = 0.05
    timeout_s: float = 1.0
    rng: random.Random = field(default_factory=random.Random)

    def create_case(self, alert: dict) -> str:
        latency = self.rng.lognormvariate(-3.5, 0.8)  # ~30 ms typical
        if self.rng.random() < self.failure_rate / 2:
            latency = self.timeout_s * 2
        if latency > self.timeout_s:
            time.sleep(self.timeout_s)
            raise TimeoutError(f"case API exceeded {self.timeout_s}s")
        time.sleep(latency)
        if self.rng.random() < self.failure_rate / 2:
            raise DownstreamError("case API returned 503")
        return f"case-{alert['alert_id'][:8]}"


class AlertProcessor:
    def __init__(self, store: IdempotencyStore, client: CaseManagementClient, breaker: CircuitBreaker,
                 *, max_retries: int = 3, cooldown_s: float = 300, sleep=time.sleep, clock=time.time):
        self.store, self.client, self.breaker = store, client, breaker
        self.max_retries, self.cooldown_s = max_retries, cooldown_s
        self._sleep, self._clock = sleep, clock
        self._last_case: dict[str, float] = {}

    def handle(self, alert: dict) -> str:
        """Returns one of: delivered | duplicate | suppressed | dead_lettered."""
        if not self.store.claim(alert):
            return "duplicate"

        now = self._clock()
        last = self._last_case.get(alert["card_id"])
        if last is not None and now - last < self.cooldown_s:
            self.store.set_status(alert["alert_id"], "suppressed")
            return "suppressed"

        def attempt() -> str:
            if not self.breaker.allow():
                raise CircuitOpenError("case API circuit open")
            try:
                case_id = self.client.create_case(alert)
            except (TimeoutError, DownstreamError):
                self.breaker.record_failure()
                raise
            self.breaker.record_success()
            return case_id

        try:
            case_id = retry_with_backoff(
                attempt, retries=self.max_retries, retry_on=(TimeoutError, DownstreamError),
                sleep=self._sleep, on_retry=lambda n, e: RETRIES.inc())
        except (TimeoutError, DownstreamError, CircuitOpenError) as exc:
            # Graceful degradation: park the alert for replay instead of blocking the stream.
            self.store.set_status(alert["alert_id"], "dead_lettered")
            log.warning("delivery failed, sent to DLQ",
                        extra={"alert_id": alert["alert_id"], "error": str(exc)})
            return "dead_lettered"
        finally:
            BREAKER_STATE.set(self.breaker.state.value)

        self._last_case[alert["card_id"]] = now
        self.store.set_status(alert["alert_id"], "delivered")
        for rule in alert.get("rules", []):
            DELIVERED_BY_RULE.labels(rule=rule).inc()
        LABELLED.labels(truth="fraud" if alert.get("labelled_fraud_txns", 0) > 0 else "legit").inc()
        log.info("case created", extra={"alert_id": alert["alert_id"], "case_id": case_id,
                                        "card_id": alert["card_id"], "rules": alert.get("rules")})
        return "delivered"


class AlertConsumer(threading.Thread):
    """Kafka consume loop. Manual commits after handling => at-least-once, made safe by idempotency."""

    def __init__(self, processor: AlertProcessor):
        super().__init__(daemon=True, name="alert-consumer")
        self.processor = processor
        self.ready = threading.Event()
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        from confluent_kafka import Consumer, Producer

        consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap,
                             "group.id": settings.consumer_group,
                             "enable.auto.commit": False,
                             "auto.offset.reset": "earliest"})
        dlq = Producer({"bootstrap.servers": settings.kafka_bootstrap,
                        "enable.idempotence": True, "acks": "all"})
        consumer.subscribe([settings.alert_topic], on_assign=lambda c, p: self.ready.set())
        last_lag_check = 0.0
        while not self._stop.is_set():
            msg = consumer.poll(1.0)
            if time.time() - last_lag_check > 10:
                self._update_lag(consumer)
                last_lag_check = time.time()
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error", extra={"error": str(msg.error())})
                continue
            CONSUMED.inc()
            with PROCESS_SECONDS.time():
                try:
                    alert = json.loads(msg.value())
                    outcome = self.processor.handle(alert)
                    if outcome == "delivered":
                        detected = datetime.fromisoformat(alert["detected_at"].replace("Z", "+00:00"))
                        DETECTION_LAG.observe(max(0.0, time.time() - detected.timestamp()))
                except (ValueError, KeyError) as exc:  # poison message: isolate it, keep going
                    alert, outcome = None, "dead_lettered"
                    log.error("unparseable alert", extra={"error": str(exc)})
            OUTCOMES.labels(outcome=outcome).inc()
            if outcome == "dead_lettered":
                dlq.produce(settings.dlq_topic, key=msg.key(), value=msg.value(),
                            headers={"reason": "delivery_failed" if alert else "poison"})
                dlq.poll(0)
            consumer.commit(message=msg, asynchronous=False)
        dlq.flush(5)
        consumer.close()

    @staticmethod
    def _update_lag(consumer) -> None:
        try:
            for tp in consumer.assignment():
                low, high = consumer.get_watermark_offsets(tp, timeout=2)
                pos = consumer.position([tp])[0].offset
                CONSUMER_LAG.labels(partition=str(tp.partition)).set(max(0, high - (pos if pos >= 0 else low)))
        except Exception as exc:  # metrics must never take the consumer down
            log.warning("lag check failed", extra={"error": str(exc)})


store = IdempotencyStore(settings.db_path)
breaker = CircuitBreaker(failure_threshold=5, reset_timeout_s=30)
processor = AlertProcessor(store, CaseManagementClient(settings.sink_failure_rate, settings.sink_timeout_s),
                           breaker, max_retries=settings.max_retries, cooldown_s=settings.cooldown_s)
consumer_thread: AlertConsumer | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global consumer_thread
    consumer_thread = AlertConsumer(processor)
    consumer_thread.start()
    yield
    consumer_thread.stop()
    consumer_thread.join(timeout=10)


app = FastAPI(title="FraudStream Alert Service", lifespan=lifespan)
app.mount("/metrics", make_asgi_app())


@app.get("/health")
def health(response: Response) -> dict:
    """Liveness: the process is up and the consumer thread hasn't died."""
    alive = consumer_thread is not None and consumer_thread.is_alive()
    response.status_code = 200 if alive else 503
    return {"status": "ok" if alive else "consumer_dead"}


@app.get("/ready")
def ready(response: Response) -> dict:
    """Readiness: partitions assigned and the downstream circuit isn't open."""
    ok = bool(consumer_thread and consumer_thread.ready.is_set()) and breaker.state is not BreakerState.OPEN
    response.status_code = 200 if ok else 503
    return {"ready": ok, "circuit": breaker.state.name}


@app.get("/alerts")
def alerts(limit: int = 50, status: str | None = None) -> list[dict]:
    return store.recent(min(limit, 500), status)
