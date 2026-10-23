"""Pure Spark transformations. They work on both streaming and batch DataFrames,
so the exact same logic is unit-tested on a small static DataFrame."""
from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .rules import (ALERT_THRESHOLD, HIGH_RISK_CATEGORIES, TXN_RULE_MIN_AMOUNT, TXN_RULE_NAME,
                    TXN_RULE_WEIGHT, WATERMARK, WINDOW_DURATION, WINDOW_RULES, WINDOW_SLIDE)

TXN_SCHEMA = T.StructType([
    T.StructField("txn_id", T.StringType()),
    T.StructField("card_id", T.StringType()),
    T.StructField("amount", T.DoubleType()),
    T.StructField("currency", T.StringType()),
    T.StructField("merchant_id", T.StringType()),
    T.StructField("merchant_category", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("card_present", T.BooleanType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("is_fraud", T.BooleanType()),
    T.StructField("fraud_pattern", T.StringType()),
])

TS_FMT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def _alert_id(card: Column, scope: Column) -> Column:
    """Same hash as rules.make_alert_id, so IDs are stable across retries and replays."""
    return F.substring(F.sha1(F.concat_ws("|", card, scope)), 1, 20)


def parse_transactions(raw: DataFrame) -> DataFrame:
    """Kafka `value` bytes -> typed columns. Malformed records are dropped (data-quality guard)."""
    return (raw.select(F.from_json(F.col("value").cast("string"), TXN_SCHEMA).alias("t"))
            .select("t.*")
            .withColumn("event_time", F.to_timestamp("event_time"))
            .where(F.col("txn_id").isNotNull() & F.col("card_id").isNotNull()
                   & F.col("event_time").isNotNull() & (F.col("amount") > 0)))


def window_features(txns: DataFrame) -> DataFrame:
    """Per-card features over a 5-minute window sliding every minute."""
    return (txns.withWatermark("event_time", WATERMARK)
            .groupBy(F.window("event_time", WINDOW_DURATION, WINDOW_SLIDE), "card_id")
            .agg(F.count(F.lit(1)).alias("txn_count"),
                 F.round(F.sum("amount"), 2).alias("total_amount"),
                 F.size(F.collect_set("country")).alias("country_count"),
                 F.sum(F.col("is_fraud").cast("int")).alias("labelled_fraud_txns")))


def window_alerts(features: DataFrame) -> DataFrame:
    hits = [F.when(F.col(r.feature) >= r.threshold, F.lit(r.name)) for r in WINDOW_RULES]
    weights = [F.when(F.col(r.feature) >= r.threshold, F.lit(r.weight)).otherwise(F.lit(0.0))
               for r in WINDOW_RULES]
    score = F.round(F.least(F.lit(1.0), reduce(lambda a, b: a + b, weights)), 3)
    start = F.date_format("window.start", TS_FMT)
    return (features
            .withColumn("rules", F.filter(F.array(*hits), lambda x: x.isNotNull()))
            .withColumn("score", score)
            .where(F.col("score") >= ALERT_THRESHOLD)
            .select(_alert_id(F.col("card_id"), F.concat(F.lit("w:"), start)).alias("alert_id"),
                    F.lit("window").alias("alert_type"), "card_id", "rules", "score",
                    start.alias("window_start"),
                    F.date_format("window.end", TS_FMT).alias("window_end"),
                    "txn_count", "total_amount", "country_count", "labelled_fraud_txns",
                    F.date_format(F.current_timestamp(), TS_FMT).alias("detected_at")))


def txn_alerts(txns: DataFrame) -> DataFrame:
    """Stateless rule evaluated per transaction (no window state needed)."""
    hit = (F.col("merchant_category").isin(*HIGH_RISK_CATEGORIES) & ~F.col("card_present")
           & (F.col("amount") >= TXN_RULE_MIN_AMOUNT))
    ts = F.date_format("event_time", TS_FMT)
    return (txns.where(hit)
            .select(_alert_id(F.col("card_id"), F.concat(F.lit("t:"), F.col("txn_id"))).alias("alert_id"),
                    F.lit("transaction").alias("alert_type"), "card_id",
                    F.array(F.lit(TXN_RULE_NAME)).alias("rules"),
                    F.lit(TXN_RULE_WEIGHT).alias("score"),
                    ts.alias("window_start"), ts.alias("window_end"),
                    F.lit(1).cast("long").alias("txn_count"), F.col("amount").alias("total_amount"),
                    F.lit(1).alias("country_count"),
                    F.col("is_fraud").cast("long").alias("labelled_fraud_txns"),
                    F.date_format(F.current_timestamp(), TS_FMT).alias("detected_at")))


def to_kafka_records(alerts: DataFrame) -> DataFrame:
    return alerts.select(F.col("card_id").alias("key"),
                         F.to_json(F.struct(*alerts.columns)).alias("value"))
