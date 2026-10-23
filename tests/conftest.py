import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("ALERT_DB_PATH", ":memory:")
# Make Spark's Python workers use the same interpreter as the test runner.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
