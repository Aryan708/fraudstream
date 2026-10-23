"""Central configuration. Every setting can be overridden with an environment variable."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = _env("KAFKA_BOOTSTRAP", "localhost:29092")
    txn_topic: str = _env("TXN_TOPIC", "transactions")
    alert_topic: str = _env("ALERT_TOPIC", "fraud-alerts")
    dlq_topic: str = _env("DLQ_TOPIC", "fraud-alerts-dlq")

    # Producer / simulator
    txn_per_second: float = float(_env("TXN_PER_SECOND", "20"))
    n_cards: int = int(_env("N_CARDS", "10000"))
    fraud_rate: float = float(_env("FRAUD_RATE", "0.01"))

    # Spark
    checkpoint_dir: str = _env("CHECKPOINT_DIR", "/tmp/fraudstream/checkpoints")
    trigger_seconds: int = int(_env("TRIGGER_SECONDS", "10"))
    spark_kafka_package: str = _env(
        "SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
    )

    # Alert service
    consumer_group: str = _env("CONSUMER_GROUP", "alert-service")
    db_path: str = _env("ALERT_DB_PATH", "/tmp/fraudstream/alerts.db")
    sink_failure_rate: float = float(_env("SINK_FAILURE_RATE", "0.05"))
    sink_timeout_s: float = float(_env("SINK_TIMEOUT_S", "1.0"))
    max_retries: int = int(_env("MAX_RETRIES", "3"))
    cooldown_s: int = int(_env("ALERT_COOLDOWN_S", "300"))

    # Metrics ports (Prometheus scrapes these)
    producer_metrics_port: int = int(_env("PRODUCER_METRICS_PORT", "8001"))
    spark_metrics_port: int = int(_env("SPARK_METRICS_PORT", "8002"))


settings = Settings()
