import pathlib
import sys
from datetime import datetime, timedelta

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import IntegerType, MapType, StringType, StructField, StructType, TimestampType

project_root = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))

import stream


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.appName("stream-tests").master("local[1]").getOrCreate()
    yield session
    session.stop()


def test_process_input_data_parses_valid_json(spark):
    payload = (
        '{"user_id": "u1", "event": "click", "value": 10, "timestamp": "2026-01-01T00:00:00Z",'
        ' "product_id": "p1", "category": "electronics", "region": "us-east-1",'
        ' "metadata": {"source": "web"}}'
    )
    lines = spark.createDataFrame([(payload,)], ["value"])

    events, _ = stream.process_input_data(lines)
    row = events.collect()[0]

    assert row["user_id"] == "u1"
    assert row["event"] == "click"
    assert row["value"] == 10
    assert row["metadata"]["source"] == "web"
    assert row["corrupt_record"] is None
    assert row["event_timestamp"] is not None
    assert row["ingest_timestamp"] is not None


def test_process_input_data_flags_corrupt_record(spark):
    lines = spark.createDataFrame([("not valid json",)], ["value"])

    events, _ = stream.process_input_data(lines)
    row = events.collect()[0]

    assert row["corrupt_record"] is not None


def test_split_late_events(spark):
    now = datetime(2026, 1, 1, 12, 0, 0)
    rows = [
        Row(event_timestamp=now - timedelta(seconds=5), ingest_timestamp=now),
        Row(event_timestamp=now - timedelta(seconds=60), ingest_timestamp=now),
    ]
    events = spark.createDataFrame(rows)

    on_time, late = stream.split_late_events(events, watermark_threshold=30)

    assert on_time.count() == 1
    assert late.count() == 1


def test_validate_events_splits_valid_and_invalid(spark):
    # corrupt_record is None for every row, so an explicit schema avoids failed type inference
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("event", StringType()),
        StructField("value", IntegerType()),
        StructField("product_id", StringType()),
        StructField("category", StringType()),
        StructField("region", StringType()),
        StructField("metadata", MapType(StringType(), StringType())),
        StructField("event_timestamp", TimestampType()),
        StructField("corrupt_record", StringType()),
    ])
    common = dict(
        event="click",
        value=1,
        product_id="p1",
        category="c",
        region="r",
        metadata={"source": "web"},
        event_timestamp=datetime(2026, 1, 1),
        corrupt_record=None,
    )
    rows = [
        Row(user_id="u1", **common),
        Row(user_id=None, **common),
    ]
    events = spark.createDataFrame(rows, schema=schema)

    valid, invalid = stream.validate_events(events)

    assert valid.count() == 1
    assert invalid.count() == 1


def test_aggregate_events_computes_aggregates(spark):
    rows = [
        Row(event_timestamp=datetime(2026, 1, 1, 12, 0, 0), value=10, event="click", category="c", region="r"),
        Row(event_timestamp=datetime(2026, 1, 1, 12, 0, 10), value=20, event="click", category="c", region="r"),
    ]
    events = spark.createDataFrame(rows)

    result = stream.aggregate_events(events).collect()

    assert len(result) == 1
    assert result[0]["total_value"] == 30
    assert result[0]["event_count"] == 2
    assert result[0]["average_value"] == 15.0


