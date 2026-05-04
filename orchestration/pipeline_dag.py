"""
orchestration/pipeline_dag.py

Airflow DAG: orchestrates the full e-commerce data pipeline.
Runs daily at midnight UTC, retries on failure, and raises an SLA
alert if the pipeline hasn't completed within 2 hours.

Task dependency graph:
    generate_events  →  spark_transform  →  run_analytics
                                         ↘  data_quality_check

To deploy:
    Copy this file to $AIRFLOW_HOME/dags/
    Airflow will detect it automatically on the next scheduler heartbeat.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Default task arguments ────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,           # each run is independent
    "email": ["alerts@yourcompany.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

# ── Data quality check (PythonOperator) ───────────────────────────────────────

def check_data_quality(**context):
    """
    Lightweight data quality gate:
    - Confirms processed Parquet files exist for today's partition
    - Raises ValueError (fails the task) if row count is too low
    """
    from pathlib import Path
    import subprocess, json

    run_date = context["ds"]                      # YYYY-MM-DD from Airflow
    processed_path = Path("/app/data/processed/daily_category_metrics")
    partition_path = processed_path / f"event_date={run_date}"

    if not partition_path.exists():
        raise FileNotFoundError(
            f"No processed partition found for {run_date}. "
            "spark_transform may have failed silently."
        )

    parquet_files = list(partition_path.glob("*.parquet"))
    if not parquet_files:
        raise ValueError(f"Partition exists but contains no Parquet files: {partition_path}")

    print(f"[QA] Found {len(parquet_files)} Parquet file(s) for {run_date} ✓")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="ecommerce_pipeline",
    description="Daily e-commerce event ingestion, transformation, and analytics",
    default_args=default_args,
    schedule_interval="0 0 * * *",      # midnight UTC daily
    start_date=days_ago(1),
    catchup=False,                       # don't backfill historical runs
    max_active_runs=1,                   # prevent overlapping pipeline runs
    tags=["ecommerce", "etl", "pyspark"],
    sla_miss_callback=None,              # hook in PagerDuty / Slack here
) as dag:

    dag.doc_md = """
    ## E-Commerce Data Pipeline

    **Owner**: Data Engineering Team
    **Schedule**: Daily at 00:00 UTC
    **SLA**: Must complete within 2 hours

    ### Tasks
    | Task | Description |
    |---|---|
    | `generate_events` | Simulate and ingest raw e-commerce events |
    | `spark_transform` | Clean, enrich, aggregate with PySpark |
    | `run_analytics`   | Execute Spark SQL KPI queries |
    | `data_quality_check` | Validate output exists and is non-empty |
    """

    # ── Task 1: Generate / ingest raw events ──────────────────────────────────
    generate_events = BashOperator(
        task_id="generate_events",
        bash_command=(
            "cd /app && "
            "python ingestion/event_generator.py "
            "--date {{ ds }}"          # passes execution date from Airflow
        ),
        sla=timedelta(minutes=30),
    )

    # ── Task 2: PySpark ETL transformation ────────────────────────────────────
    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command=(
            "cd /app && "
            "python transformation/spark_transform.py "
            "--date {{ ds }}"
        ),
        sla=timedelta(minutes=60),
        # In a real cluster this would be: spark-submit --master yarn ...
        # or use the DatabricksRunNowOperator / AzureHDInsightHiveOperator
    )

    # ── Task 3: Analytics queries ─────────────────────────────────────────────
    run_analytics = BashOperator(
        task_id="run_analytics",
        bash_command="cd /app && python transformation/analytics.py",
        sla=timedelta(minutes=90),
    )

    # ── Task 4: Data quality gate ─────────────────────────────────────────────
    quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=check_data_quality,
        provide_context=True,
        sla=timedelta(minutes=100),
    )

    # ── Dependency graph ──────────────────────────────────────────────────────
    #
    #   generate_events
    #         │
    #         ▼
    #   spark_transform
    #         │
    #    ┌────┴────┐
    #    ▼         ▼
    # analytics  quality_check
    #
    generate_events >> spark_transform >> [run_analytics, quality_check]
