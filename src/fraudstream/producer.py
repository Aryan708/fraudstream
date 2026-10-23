"""Streams simulated card transactions into Kafka.

- Keyed by card_id, so all events for one card land on one partition in order.
- Idempotent producer (enable.idempotence + acks=all): broker retries never create duplicates.
- Exposes Prometheus metrics on :PRODUCER_METRICS_PORT.
"""
from __future__ import annotations

import heapq
import itertools
import json
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, start_http_server

from .config import settings
from .logging_utils import get_logger
from .simulator import TransactionSimulator

log = get_logger("producer")

TXN_PRODUCED = Counter("fs_transactions_produced_total", "Transactions delivered to Kafka", ["labelled_fraud"])
PRODUCE_ERRORS = Counter("fs_produce_errors_total", "Transactions Kafka failed to accept")
FRAUD_INJECTED = Counter("fs_fraud_bursts_injected_total", "Fraud patterns injected", ["pattern"])
QUEUE_DEPTH = Gauge("fs_producer_queue_depth", "Messages waiting in the producer buffer")


def _on_delivery(err, msg) -> None:
    if err is not None:
        PRODUCE_ERRORS.inc()
        log.warning("delivery failed", extra={"error": str(err), "topic": msg.topic()})
    else:
        TXN_PRODUCED.labels(labelled_fraud=json.loads(msg.value())["is_fraud"]).inc()


def run() -> None:
    start_http_server(settings.producer_metrics_port)
    producer = Producer({
        "bootstrap.servers": settings.kafka_bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
    })
    sim = TransactionSimulator(n_cards=settings.n_cards, fraud_rate=settings.fraud_rate)
    pending: list[tuple[float, int, dict]] = []  # (due_epoch, seq, txn) — bursts play out in real time
    seq = itertools.count()
    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    log.info("producer started", extra={"bootstrap": settings.kafka_bootstrap, "tps": settings.txn_per_second})

    interval = 1.0 / settings.txn_per_second
    while not stop:
        batch = sim.next_batch(datetime.now(timezone.utc))
        if batch[0]["is_fraud"]:
            FRAUD_INJECTED.labels(pattern=batch[0]["fraud_pattern"]).inc()
        for txn in batch:
            due = datetime.fromisoformat(txn["event_time"].replace("Z", "+00:00")).timestamp()
            heapq.heappush(pending, (due, next(seq), txn))

        now = time.time()
        while pending and pending[0][0] <= now:
            _, _, txn = heapq.heappop(pending)
            try:
                producer.produce(settings.txn_topic, key=txn["card_id"], value=json.dumps(txn),
                                 on_delivery=_on_delivery)
            except BufferError:
                log.warning("local queue full, backing off")
                producer.poll(0.5)
                heapq.heappush(pending, (now, next(seq), txn))
                break
        producer.poll(0)
        QUEUE_DEPTH.set(len(producer))
        time.sleep(interval)

    log.info("shutting down, flushing producer")
    producer.flush(10)


if __name__ == "__main__":
    run()
