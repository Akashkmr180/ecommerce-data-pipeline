"""
tests/test_transformations.py

Unit tests for the PySpark transformation logic.
Uses a local SparkSession with minimal config — no cluster required.

Run:
    pytest tests/ -v
    pytest tests/ -v --tb=short   # compact tracebacks
"""

import json
import pytest
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for the entire test session."""
    session = (
        SparkSession.builder
        .appName("PipelineTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")   # suppress Spark UI in tests
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_events(spark):
    """Return a small DataFrame of mixed-quality events."""
    data = [
        # Valid purchase
        {
            "event_id": "evt-001", "event_type": "purchase",
            "user_id": "user_1", "session_id": "sess-a",
            "product_id": "prod_10", "category": "Electronics",
            "timestamp": "2024-01-15T10:30:00Z", "amount": 299.99,
            "metadata": {"device": "mobile", "country": "IN"},
        },
        # Valid add_to_cart (no amount)
        {
            "event_id": "evt-002", "event_type": "add_to_cart",
            "user_id": "user_2", "session_id": "sess-b",
            "product_id": "prod_20", "category": "Clothing",
            "timestamp": "2024-01-15T11:00:00Z", "amount": None,
            "metadata": {"device": "desktop", "country": "US"},
        },
        # Valid refund (negative amount)
        {
            "event_id": "evt-003", "event_type": "refund",
            "user_id": "user_1", "session_id": "sess-a",
            "product_id": "prod_10", "category": "Electronics",
            "timestamp": "2024-01-15T14:00:00Z", "amount": -50.0,
            "metadata": {"device": "mobile", "country": "IN"},
        },
        # Duplicate of evt-001 — should be dropped
        {
            "event_id": "evt-001", "event_type": "purchase",
            "user_id": "user_1", "session_id": "sess-a",
            "product_id": "prod_10", "category": "Electronics",
            "timestamp": "2024-01-15T10:30:00Z", "amount": 299.99,
            "metadata": {"device": "mobile", "country": "IN"},
        },
        # Invalid: purchase with null amount — should be dropped
        {
            "event_id": "evt-004", "event_type": "purchase",
            "user_id": "user_3", "session_id": "sess-c",
            "product_id": "prod_30", "category": "Books",
            "timestamp": "2024-01-15T09:00:00Z", "amount": None,
            "metadata": {"device": "tablet", "country": "GB"},
        },
        # Invalid: unknown event type — should be dropped
        {
            "event_id": "evt-005", "event_type": "wishlist_add",
            "user_id": "user_4", "session_id": "sess-d",
            "product_id": "prod_40", "category": "Sports",
            "timestamp": "2024-01-15T08:00:00Z", "amount": None,
            "metadata": {"device": "mobile", "country": "DE"},
        },
    ]

    schema = StructType([
        StructField("event_id",   StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("user_id",    StringType(), False),
        StructField("session_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category",   StringType(), True),
        StructField("timestamp",  StringType(), False),
        StructField("amount",     DoubleType(), True),
        StructField("metadata",   StructType([
            StructField("device",  StringType(), True),
            StructField("country", StringType(), True),
        ]), True),
    ])

    return spark.createDataFrame(data, schema=schema)


# ── Import transforms (adjust sys.path so pytest can find them) ───────────────

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "transformation"))

from spark_transform import clean, enrich, aggregate_daily


# ── Tests: clean() ───────────────────────────────────────────────────────────

class TestClean:

    def test_removes_duplicates(self, sample_events):
        result = clean(sample_events)
        event_ids = [row.event_id for row in result.select("event_id").collect()]
        assert len(event_ids) == len(set(event_ids)), "Duplicate event_ids found after clean()"

    def test_drops_purchase_with_null_amount(self, sample_events):
        result = clean(sample_events)
        purchases_null = (
            result
            .filter((F.col("event_type") == "purchase") & F.col("amount").isNull())
            .count()
        )
        assert purchases_null == 0, "Purchases with null amount should be removed"

    def test_drops_unknown_event_types(self, sample_events):
        result = clean(sample_events)
        known_types = {"page_view", "add_to_cart", "purchase", "refund"}
        found_types = {row.event_type for row in result.select("event_type").collect()}
        assert found_types.issubset(known_types), f"Unknown event types found: {found_types - known_types}"

    def test_parses_timestamp(self, sample_events):
        result = clean(sample_events)
        assert "event_ts" in result.columns, "event_ts column should exist after clean()"
        assert "timestamp" not in result.columns, "Raw timestamp column should be dropped"
        null_ts = result.filter(F.col("event_ts").isNull()).count()
        assert null_ts == 0, "All remaining rows should have a valid event_ts"

    def test_row_count_after_clean(self, sample_events):
        # Started with 5 events, 2 should be removed (1 dup + 1 null amount + 1 bad type = 3 removed)
        # Wait: evt-001 dup=1 removed, evt-004 null purchase=1 removed, evt-005 bad type=1 removed
        # Remaining: evt-001, evt-002, evt-003 = 3
        result = clean(sample_events)
        assert result.count() == 3, f"Expected 3 rows after cleaning, got {result.count()}"


# ── Tests: enrich() ──────────────────────────────────────────────────────────

class TestEnrich:

    def test_adds_event_date(self, sample_events):
        result = enrich(clean(sample_events))
        assert "event_date" in result.columns
        null_dates = result.filter(F.col("event_date").isNull()).count()
        assert null_dates == 0

    def test_flattens_metadata(self, sample_events):
        result = enrich(clean(sample_events))
        assert "device" in result.columns
        assert "country" in result.columns
        assert "metadata" not in result.columns, "metadata struct should be dropped after flattening"

    def test_revenue_column_logic(self, sample_events):
        result = enrich(clean(sample_events))

        # Purchases should have positive revenue
        purchase_revenues = [
            row.revenue for row in
            result.filter(F.col("event_type") == "purchase").select("revenue").collect()
        ]
        assert all(r > 0 for r in purchase_revenues), "Purchase revenues must be positive"

        # Refunds should have negative revenue
        refund_revenues = [
            row.revenue for row in
            result.filter(F.col("event_type") == "refund").select("revenue").collect()
        ]
        assert all(r < 0 for r in refund_revenues), "Refund revenues must be negative"

        # Cart adds should have null revenue
        cart_revenues = [
            row.revenue for row in
            result.filter(F.col("event_type") == "add_to_cart").select("revenue").collect()
        ]
        assert all(r is None for r in cart_revenues), "Cart add revenues should be null"

    def test_boolean_flag_columns_exist(self, sample_events):
        result = enrich(clean(sample_events))
        for col in ("is_purchase", "is_cart", "is_refund"):
            assert col in result.columns, f"Missing flag column: {col}"


# ── Tests: aggregate_daily() ──────────────────────────────────────────────────

class TestAggregate:

    def test_output_schema(self, sample_events):
        result = aggregate_daily(enrich(clean(sample_events)))
        required_cols = {
            "event_date", "category", "total_orders",
            "cart_adds", "gross_revenue", "net_revenue",
        }
        assert required_cols.issubset(set(result.columns)), (
            f"Missing columns: {required_cols - set(result.columns)}"
        )

    def test_no_null_categories(self, sample_events):
        result = aggregate_daily(enrich(clean(sample_events)))
        null_cats = result.filter(F.col("category").isNull()).count()
        assert null_cats == 0, "Aggregated output should not contain null categories"

    def test_gross_revenue_positive(self, sample_events):
        result = aggregate_daily(enrich(clean(sample_events)))
        negative_revenue = (
            result
            .filter(F.col("gross_revenue").isNotNull() & (F.col("gross_revenue") < 0))
            .count()
        )
        assert negative_revenue == 0, "Gross revenue must always be non-negative"

    def test_abandonment_rate_between_0_and_1(self, sample_events):
        result = aggregate_daily(enrich(clean(sample_events)))
        out_of_range = (
            result
            .filter(
                F.col("cart_abandonment_rate").isNotNull() &
                ((F.col("cart_abandonment_rate") < 0) | (F.col("cart_abandonment_rate") > 1))
            )
            .count()
        )
        assert out_of_range == 0, "Abandonment rate must be between 0 and 1"


# ── Tests: Kafka simulator ────────────────────────────────────────────────────

class TestKafkaSimulator:

    def test_producer_consumer_roundtrip(self):
        sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))
        from kafka_simulator import EventProducer, EventConsumer

        producer = EventProducer(topic="test-topic")
        test_event = {"event_id": "test-001", "amount": 99.99}
        producer.send(test_event)

        consumer = EventConsumer(topics=["test-topic"], group_id="test-group")
        messages = consumer.poll(max_messages=10)

        assert len(messages) == 1
        assert messages[0].value["event_id"] == "test-001"
        assert messages[0].value["amount"] == 99.99

    def test_offset_increments(self):
        sys.path.insert(0, str(Path(__file__).parent.parent / "ingestion"))
        from kafka_simulator import EventProducer, MessageBroker

        # Use a unique topic to avoid state from other tests
        topic = f"offset-test-{datetime.utcnow().timestamp()}"
        producer = EventProducer(topic=topic)

        offsets = [producer.send({"i": i}) for i in range(5)]
        # Offsets should be sequential integers starting at 0
        assert offsets == list(range(len(offsets))), "Offsets should be sequential"
