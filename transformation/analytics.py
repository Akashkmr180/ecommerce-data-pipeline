"""
transformation/analytics.py

Business KPI queries using Spark SQL against the processed Parquet tables.
This layer is deliberately kept as pure SQL so analysts can run the same
queries in Azure Synapse, Databricks SQL, or Hive without changing logic.

KPIs computed:
  1. Top 5 revenue categories (last 7 days)
  2. Daily active users trend
  3. Cart abandonment rate by category
  4. Refund rate by category
  5. Day-over-day revenue change
"""

from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT  = Path(__file__).parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


# ── Spark session (read-only analytics) ─────────────────────────────────────────

def build_spark():
    return (
        SparkSession.builder
        .appName("EcommerceAnalytics")
        .master("local[*]")
        .config("spark.sql.warehouse.dir", str(PROCESSED_DIR))
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ── Register Parquet tables as temp views ────────────────────────────────────────

def register_views(spark: SparkSession):
    events_path  = PROCESSED_DIR / "events_clean" / "data.csv"
    metrics_path = PROCESSED_DIR / "daily_category_metrics" / "data.csv"

    if not events_path.exists():
        raise FileNotFoundError(
            "Processed data not found. Run spark_transform.py first."
        )

    spark.read.option("header", "true").csv(str(events_path)).createOrReplaceTempView("events")
    spark.read.option("header", "true").csv(str(metrics_path)).createOrReplaceTempView("daily_metrics")
    print("[Analytics] Registered temp views: events, daily_metrics")


# ── KPI queries ──────────────────────────────────────────────────────────────────

QUERIES = {

    "Top 5 Revenue Categories (all time)": """
        SELECT
            category,
            ROUND(SUM(gross_revenue), 2)   AS total_revenue,
            SUM(total_orders)              AS total_orders,
            ROUND(AVG(cart_abandonment_rate) * 100, 1) AS avg_abandonment_pct
        FROM daily_metrics
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY total_revenue DESC
        LIMIT 5
    """,

    "Daily Active Users (last 7 days)": """
        SELECT
            event_date,
            SUM(unique_users) AS daily_active_users,
            SUM(total_orders) AS daily_orders
        FROM daily_metrics
        GROUP BY event_date
        ORDER BY event_date DESC
        LIMIT 7
    """,

    "Cart Abandonment Rate by Category": """
        SELECT
            category,
            SUM(cart_adds)    AS total_cart_adds,
            SUM(total_orders) AS total_purchases,
            ROUND(
                (1 - SUM(total_orders) / NULLIF(SUM(cart_adds), 0)) * 100,
                1
            ) AS abandonment_rate_pct
        FROM daily_metrics
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY abandonment_rate_pct DESC
    """,

    "Refund Rate by Category": """
        SELECT
            category,
            SUM(total_orders)  AS orders,
            SUM(refunds)       AS refunds,
            ROUND(SUM(refunds) / NULLIF(SUM(total_orders), 0) * 100, 1) AS refund_rate_pct,
            ROUND(ABS(SUM(refund_amount)), 2) AS total_refund_value
        FROM daily_metrics
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY refund_rate_pct DESC
    """,

    "Net Revenue vs Gross Revenue by Category": """
        SELECT
            category,
            ROUND(SUM(gross_revenue), 2) AS gross_revenue,
            ROUND(ABS(SUM(refund_amount)), 2) AS refunded,
            ROUND(SUM(net_revenue), 2)   AS net_revenue,
            ROUND(
                ABS(SUM(refund_amount)) / NULLIF(SUM(gross_revenue), 0) * 100,
                1
            ) AS refund_pct
        FROM daily_metrics
        WHERE category IS NOT NULL
        GROUP BY category
        ORDER BY net_revenue DESC
    """,
}


# ── Runner ───────────────────────────────────────────────────────────────────────

def run_analytics():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        register_views(spark)
    except FileNotFoundError as e:
        print(f"\n[Error] {e}")
        spark.stop()
        return

    print(f"\n{'='*60}")
    print("  E-Commerce Analytics Dashboard")
    print(f"{'='*60}")

    for title, sql in QUERIES.items():
        print(f"\n{'─'*60}")
        print(f"  {title}")
        print(f"{'─'*60}")
        spark.sql(sql).show(truncate=False)

    spark.stop()
    print("\n✓ Analytics complete\n")


if __name__ == "__main__":
    run_analytics()
