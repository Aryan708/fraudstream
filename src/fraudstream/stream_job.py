"""Spark Structured Streaming job: transactions (Kafka) -> fraud alerts (Kafka).

Two queries share one parsed input stream:
  1. window query  - stateful 5-min sliding-window aggregation per card (update mode, watermarked)
  2. txn query     - stateless per-transaction rule
Both write through foreachBatch so each micro-batch also updates Prometheus metrics.
Checkpoints make the job restartable without losing or re-reading offsets.
"""
from __future__ import annotations

import time

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import settings
from .logging_utils import get_logger
from .spark_transforms import (parse_transactions, to_kafka_records, txn_alerts, window_alerts,
                               window_features)

log = get_logger("spark-job")

BATCHES = Counter("fs_spark_batches_total", "Micro-batches processed", ["query"])
BATCH_SECONDS = Histogram("fs_spark_batch_write_seconds", "Time to write a micro-batch", ["query"],
                          buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30))
ALERTS_EMITTED = Counter("fs_spark_alerts_emitted_total", "Alerts written to Kafka", ["query", "rule"])
INPUT_RPS = Gauge("fs_spark_input_rows_per_second", "Input rate reported by Spark", ["query"])
PROCESSED_RPS = Gauge("fs_spark_processed_rows_per_second", "Processing rate reported by Spark", ["query"])
TRIGGER_MS = Gauge("fs_spark_trigger_duration_ms", "Last trigger execution time", ["query"])
STATE_ROWS = Gauge("fs_spark_state_rows", "Rows held in streaming state", ["query"])
QUERY_UP = Gauge("fs_spark_query_active", "1 if the streaming query is running", ["query"])


def _sink(query_name: str):
    def write(batch: DataFrame, batch_id: int) -> None:
        start = time.perf_counter()
        batch.persist()
        try:
            for row in batch.select(F.explode("rules").alias("rule")).groupBy("rule").count().collect():
                ALERTS_EMITTED.labels(query=query_name, rule=row["rule"]).inc(row["count"])
            (to_kafka_records(batch).write.format("kafka")
             .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
             .option("topic", settings.alert_topic)
             .option("kafka.enable.idempotence", "true")
             .save())
        finally:
            batch.unpersist()
        BATCHES.labels(query=query_name).inc()
        BATCH_SECONDS.labels(query=query_name).observe(time.perf_counter() - start)
        log.info("batch written", extra={"query": query_name, "batch_id": batch_id})
    return write


def build_spark() -> SparkSession:
    return (SparkSession.builder.appName("fraudstream")
            .config("spark.jars.packages", settings.spark_kafka_package)
            .config("spark.sql.shuffle.partitions", "6")
            .config("spark.sql.streaming.stateStore.providerClass",
                    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
            .getOrCreate())


def run() -> None:
    start_http_server(settings.spark_metrics_port)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
           .option("subscribe", settings.txn_topic)
           .option("startingOffsets", "latest")
           .option("maxOffsetsPerTrigger", 50_000)   # back-pressure: cap work per micro-batch
           .option("failOnDataLoss", "false")
           .load())
    txns = parse_transactions(raw)
    trigger = {"processingTime": f"{settings.trigger_seconds} seconds"}

    queries = [
        (window_alerts(window_features(txns)).writeStream.queryName("window_rules")
         .outputMode("update").trigger(**trigger)
         .option("checkpointLocation", f"{settings.checkpoint_dir}/window_rules")
         .foreachBatch(_sink("window_rules")).start()),
        (txn_alerts(txns).writeStream.queryName("txn_rules")
         .outputMode("append").trigger(**trigger)
         .option("checkpointLocation", f"{settings.checkpoint_dir}/txn_rules")
         .foreachBatch(_sink("txn_rules")).start()),
    ]
    log.info("streaming queries started", extra={"queries": [q.name for q in queries]})

    # Publish Spark's own progress numbers as Prometheus gauges.
    while any(q.isActive for q in queries):
        for q in queries:
            QUERY_UP.labels(query=q.name).set(1 if q.isActive else 0)
            p = q.lastProgress
            if p:
                INPUT_RPS.labels(query=q.name).set(p.get("inputRowsPerSecond") or 0)
                PROCESSED_RPS.labels(query=q.name).set(p.get("processedRowsPerSecond") or 0)
                TRIGGER_MS.labels(query=q.name).set(p.get("durationMs", {}).get("triggerExecution", 0))
                STATE_ROWS.labels(query=q.name).set(
                    sum(s.get("numRowsTotal", 0) for s in p.get("stateOperators", [])))
        time.sleep(5)

    for q in queries:
        QUERY_UP.labels(query=q.name).set(0)
        if q.exception():
            log.error("query failed", extra={"query": q.name, "error": str(q.exception())})
    raise SystemExit(1)  # let the container restart policy bring the job back


if __name__ == "__main__":
    run()
