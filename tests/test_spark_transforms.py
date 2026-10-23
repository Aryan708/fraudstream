"""Runs the real Spark transformations on a small static DataFrame (local mode)."""
import json
import shutil
from datetime import datetime, timedelta, timezone

import pytest

pyspark = pytest.importorskip("pyspark")
if not shutil.which("java"):
    pytest.skip("Java is required for Spark tests", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

from fraudstream import rules  # noqa: E402
from fraudstream.simulator import TransactionSimulator  # noqa: E402
from fraudstream.spark_transforms import (parse_transactions, to_kafka_records, txn_alerts,  # noqa: E402
                                          window_alerts, window_features)

NOW = datetime(2026, 10, 1, 12, 0, 30, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("fraudstream-tests")
         .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def txns(spark):
    sim = TransactionSimulator(n_cards=2000, seed=11)
    events = [sim.normal_txn(NOW + timedelta(seconds=i)) for i in range(300)]
    events += sim.fraud_burst(NOW, "velocity") + sim.fraud_burst(NOW, "multi_country")
    events += sim.fraud_burst(NOW, "risky_merchant")
    raw = [(json.dumps(e).encode(),) for e in events]
    raw.append((b"{not json",))                                   # malformed -> dropped
    raw.append((json.dumps(events[0] | {"amount": -5}).encode(),))  # invalid amount -> dropped
    return events, parse_transactions(spark.createDataFrame(raw, ["value"]))


def test_parse_drops_bad_records(txns):
    events, df = txns
    assert df.count() == len(events)


def test_window_rules_flag_fraud_cards_only(txns):
    events, df = txns
    alerts = window_alerts(window_features(df)).collect()
    must_catch = {e["card_id"] for e in events if e["fraud_pattern"] in ("velocity", "multi_country")}
    all_fraud = {e["card_id"] for e in events if e["is_fraud"]}
    # Every burst is caught; nothing outside the fraud cards is flagged. (A large risky-merchant
    # cash-out can also trip HIGH_SPEND, which is fine.)
    assert must_catch <= {a["card_id"] for a in alerts} <= all_fraud
    assert all(a["labelled_fraud_txns"] > 0 and a["score"] >= rules.ALERT_THRESHOLD for a in alerts)
    assert {r for a in alerts for r in a["rules"]} >= {"VELOCITY", "MULTI_COUNTRY"}


def test_spark_alert_ids_match_python(txns):
    _, df = txns
    a = window_alerts(window_features(df)).first()
    assert a["alert_id"] == rules.make_alert_id(a["card_id"], f"w:{a['window_start']}")


def test_txn_rule_and_kafka_serialisation(txns):
    events, df = txns
    alerts = txn_alerts(df)
    rows = alerts.collect()
    assert [r["card_id"] for r in rows] == [e["card_id"] for e in events if e["fraud_pattern"] == "risky_merchant"]
    rec = to_kafka_records(alerts).first()
    assert json.loads(rec["value"])["rules"] == ["HIGH_RISK_MERCHANT"]
    assert rec["key"] == rows[0]["card_id"]
