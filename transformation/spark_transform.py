"""
transformation/spark_transform.py

PySpark ETL pipeline: reads raw event JSONL files, applies data quality
checks, enriches and aggregates, then writes partitioned Parquet output.

Pipeline stages:
  1. Read  — load JSONL from landing zone with explicit schema
  2. Clean — deduplicate, cast types, handle nulls, filter bad rows
  3. Enrich — extract date partition, device category, revenue flag
  4. Aggregate — daily revenue and order counts per category
  5. Write — partitioned Parquet (Hive-compatible layout)

Run:
    python transformation/spark_transform.py
    python transformation/spark_transform.py --date 2024-01-15
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Paths ───────────────────────────────────────────────────────────────────────

import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

PROJECT_ROOT  = Path(__file__).parent.parent
RAW_DIR       = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ── Schema ──────────────────────────────────────────────────────────────────────
# Always define schemas explicitly — inferSchema triggers an extra scan
# and can misinterpret nullable columns.

RAW_SCHEMA = StructType([
    StructField("event_id",   StringType(),    nullable=False),
    StructField("event_type", StringType(),    nullable=False),
    StructField("user_id",    StringType(),    nullable=False),
    StructField("session_id", StringType(),    nullable=True),
    StructField("product_id", StringType(),    nullable=True),
    StructField("category",   StringType(),    nullable=True),
    StructField("timestamp",  StringType(),    nullable=False),  # parsed below
    StructField("amount",     DoubleType(),    nullable=True),
    StructField("metadata",   StructType([
        StructField("device",  StringType(), True),
        StructField("country", StringType(), True),
    ]), nullable=True),
])

VALID_EVENT_TYPES = {"page_view", "add_to_cart", "purchase", "refund"}
VALID_CATEGORIES  = {"Electronics", "Clothing", "Home & Garden", "Sports", "Books"}


# ── Spark session ───────────────────────────────────────────────────────────────

def build_spark(app_name: str = "EcommerceETL") -> SparkSession:
    import os
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.warehouse.dir", str(PROCESSED_DIR))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.file.impl",
                "org.apache.hadoop.fs.LocalFileSystem")
        .getOrCreate()
    )




# ── Stage 1: Read ────────────────────────────────────────────────────────────────

def read_raw_events(spark: SparkSession, date_str: str):
    """
    Read raw JSONL events for a given date.
    Falls back to all available files if no date-specific file exists.
    """
    dated_file = RAW_DIR / f"events_{date_str}.jsonl"

    if dated_file.exists():
        paths = [str(dated_file)]
    else:
        paths = [str(p) for p in RAW_DIR.glob("events_*.jsonl")]
        if not paths:
            raise FileNotFoundError(
                f"No raw event files found in {RAW_DIR}. "
                "Run: python ingestion/event_generator.py"
            )

    print(f"[Read] Loading {len(paths)} file(s) from {RAW_DIR}")
    df = spark.read.schema(RAW_SCHEMA).json(paths)
    print(f"[Read] Raw row count: {df.count():,}")
    return df


# ── Stage 2: Clean ───────────────────────────────────────────────────────────────

def clean(df):
    """
    Apply data quality rules:
    - Drop rows missing critical fields
    - Parse timestamp string → TimestampType
    - Filter to known event types and categories
    - Remove duplicates by event_id
    - Validate amounts (purchases must have positive amounts)
    """
    before = df.count()

    df = (
        df
        # Parse ISO timestamp
        .withColumn(
            "event_ts",
            F.to_timestamp(F.col("timestamp"))
        )
        .drop("timestamp")

        # Drop rows with null critical fields
        .dropna(subset=["event_id", "event_type", "user_id", "event_ts"])

        # Filter to known event types
        .filter(F.col("event_type").isin(VALID_EVENT_TYPES))

        # Filter to known categories (nulls allowed for page_view)
        .filter(
            F.col("category").isin(VALID_CATEGORIES) |
            (F.col("event_type") == "page_view")
        )

        # Purchases without amounts are invalid
        .filter(
            ~(
                (F.col("event_type") == "purchase") &
                (F.col("amount").isNull() | (F.col("amount") <= 0))
            )
        )

        # Deduplicate (idempotent reruns won't create duplicate rows)
        .dropDuplicates(["event_id"])
    )

    after = df.count()
    print(f"[Clean] Dropped {before - after:,} bad rows ({after:,} remain)")
    return df


# ── Stage 3: Enrich ──────────────────────────────────────────────────────────────

def enrich(df):
    """
    Add derived columns used downstream by analytics queries.
    """
    return (
        df
        # Partition key (date string for Hive-style partitioning)
        .withColumn("event_date", F.to_date(F.col("event_ts")))

        # Flatten metadata struct for easier querying
        .withColumn("device",  F.col("metadata.device"))
        .withColumn("country", F.col("metadata.country"))
        .drop("metadata")

        # Revenue column: purchases positive, refunds negative, others null
        .withColumn(
            "revenue",
            F.when(F.col("event_type") == "purchase",  F.col("amount"))
             .when(F.col("event_type") == "refund",    F.col("amount"))
             .otherwise(F.lit(None).cast(DoubleType()))
        )

        # Boolean flags useful in SQL aggregations
        .withColumn("is_purchase", (F.col("event_type") == "purchase").cast("int"))
        .withColumn("is_cart",     (F.col("event_type") == "add_to_cart").cast("int"))
        .withColumn("is_refund",   (F.col("event_type") == "refund").cast("int"))
    )


# ── Stage 4: Aggregate ───────────────────────────────────────────────────────────

def aggregate_daily(df):
    """
    Roll up to daily category-level metrics.
    This table drives the analytics dashboard.
    """
    return (
        df
        .filter(F.col("category").isNotNull())
        .groupBy("event_date", "category")
        .agg(
            F.sum("is_purchase").alias("total_orders"),
            F.sum("is_cart").alias("cart_adds"),
            F.sum("is_refund").alias("refunds"),
            F.sum(F.when(F.col("revenue") > 0, F.col("revenue"))).alias("gross_revenue"),
            F.sum(F.when(F.col("revenue") < 0, F.col("revenue"))).alias("refund_amount"),
            F.countDistinct("user_id").alias("unique_users"),
            F.countDistinct("session_id").alias("unique_sessions"),
        )
        .withColumn(
            "net_revenue",
            F.coalesce(F.col("gross_revenue"), F.lit(0.0)) +
            F.coalesce(F.col("refund_amount"),  F.lit(0.0))
        )
        .withColumn(
            "cart_abandonment_rate",
            F.when(
                F.col("cart_adds") > 0,
                F.round(1 - F.col("total_orders") / F.col("cart_adds"), 4)
            ).otherwise(F.lit(None))
        )
        .orderBy("event_date", "category")
    )


# ── Stage 5: Write ───────────────────────────────────────────────────────────────

def write_parquet(df, table_name: str, partition_by: str = "event_date"):
    output_path = PROCESSED_DIR / table_name
    output_path.mkdir(parents=True, exist_ok=True)

    # Convert to pandas and save as CSV — works on Windows without winutils
    pandas_df = df.toPandas()
    file_path = output_path / "data.csv"
    pandas_df.to_csv(str(file_path), index=False)
    print(f"[Write] {table_name} → {file_path} ({len(pandas_df)} rows)")


# ── Orchestration ────────────────────────────────────────────────────────────────

def run(date_str: str):
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*55}")
    print(f"  E-Commerce ETL  |  date={date_str}")
    print(f"{'='*55}\n")

    # 1. Read
    raw = read_raw_events(spark, date_str)

    # 2. Clean
    clean_df = clean(raw)

    # 3. Enrich
    enriched = enrich(clean_df)

    # 4. Write full event log (clean + enriched) — supports ad-hoc queries
    write_parquet(enriched, "events_clean")

    # 5. Aggregate and write summary table
    daily = aggregate_daily(enriched)
    write_parquet(daily, "daily_category_metrics")

    # Quick preview
    print("\n[Preview] Daily category metrics:")
    daily.show(10, truncate=False)

    spark.stop()
    print("\n✓ ETL complete\n")


# ── CLI ──────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run PySpark ETL")
    parser.add_argument(
        "--date",
        default=datetime.utcnow().strftime("%Y-%m-%d"),
        help="Process date (YYYY-MM-DD). Defaults to today.",
    )
    args = parser.parse_args()
    run(args.date)
