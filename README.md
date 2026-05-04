# E-Commerce Data Pipeline
A production-style data engineering pipeline that ingests raw e-commerce event data, processes it with PySpark, aggregates it with Spark SQL, and visualizes KPIs in a browser dashboard — all orchestrated with Apache Airflow and containerized with Docker.

Built to demonstrate real-world data engineering skills: Python, PySpark, Spark SQL, Kafka streaming concepts, Airflow, Docker, and Git.


# Architecture
Raw Events (JSONL)
        │
        ▼
  Ingestion Layer          ← event_generator.py + kafka_simulator.py
        │
        ▼
  PySpark Transforms       ← clean, deduplicate, enrich, aggregate
        │
        ▼
  CSV Data Warehouse       ← partitioned by event_date
        │
        ▼
  Spark SQL Analytics      ← 5 business KPI queries
        │
        ▼
  Browser Dashboard        ← dashboard.html opens automatically
        │
        ▼
  Airflow DAG              ← orchestrates full pipeline daily

 Tech Stack
ToolPurposeJD Requirement CoveredPython 3.13Ingestion scripts, orchestrationPython for data processingPySpark 3.5Distributed data transformationApache Spark / Spark SQLSpark SQLBusiness KPI queriesApache Spark / Spark SQLKafka SimulationProducer/consumer event streamingKafka / streaming conceptsCSV + date partitioningHive-style data warehouse layoutHive / data warehousingApache AirflowPipeline orchestration and schedulingWorkflow automationDocker + ComposeFully containerized runtimeCloud-ready deploymentGitVersion controlGit version control

 Quick Start
Prerequisites

Python 3.10 or above
Java 17 

# Install dependencies
bashpip install pyspark pytest pandas
Run the full pipeline
bash# Step 1 — Generate 5000 synthetic e-commerce events
cd ingestion
python event_generator.py

# Step 2 — Run PySpark ETL transformation
cd 
python transformation/spark_transform.py

# Step 3 — Run Spark SQL analytics (auto opens dashboard in browser)
python transformation/analytics.py

# Project Structure
ecommerce-data-pipeline/
├── ingestion/
│   ├── event_generator.py       # Generates 5000 e-commerce events (JSONL)
│   └── kafka_simulator.py       # Kafka-style producer/consumer in pure Python
├── transformation/
│   ├── spark_transform.py       # PySpark ETL: clean, enrich, aggregate
│   └── analytics.py             # Spark SQL KPI queries + opens dashboard
├── orchestration/
│   └── pipeline_dag.py          # Airflow DAG: runs pipeline daily at midnight
├── tests/
│   └── test_transformations.py  # 14 pytest unit tests
├── data/
│   ├── raw/                     # Landing zone — JSONL events land here
│   └── processed/               # Output warehouse — CSV partitioned by date
├── requirements.txt
└── README.md


 What the Pipeline Does
1. Ingestion — Simulated Event Stream
event_generator.py generates 5000 realistic e-commerce events:

Event types: page_view, add_to_cart, purchase, refund
Each event has: user_id, product_id, category, amount, timestamp
Writes events to data/raw/events_YYYY-MM-DD.jsonl

2. Kafka Simulation
kafka_simulator.py implements a lightweight pub/sub queue in pure Python:

EventProducer pushes events to a named topic
EventConsumer polls and deserializes messages
Mirrors the confluent-kafka API — swap connection string for Azure Event Hubs

3. PySpark Transformation
spark_transform.py runs a 5-stage ETL pipeline:

Read — loads JSONL with explicit StructType schema
Clean — deduplicates by event_id, drops nulls, filters invalid event types, validates purchase amounts
Enrich — adds event_date, flattens metadata struct, creates revenue and boolean flag columns
Aggregate — daily revenue, orders, cart adds, refunds, abandonment rate per category
Write — saves to CSV, partitioned by event_date

4. Spark SQL Analytics
analytics.py answers 5 real business questions:

Top 5 revenue-generating product categories
Daily active users over last 7 days
Cart abandonment rate by category
Refund rate by category
Net revenue vs gross revenue by category

5. Browser Dashboard
dashboard.html opens automatically after analytics run:

Revenue bar chart by category
Daily active users trend
Cart abandonment rate table with colour-coded pills
Refund analysis table

6. Airflow Orchestration
pipeline_dag.py schedules the pipeline as a DAG:

Runs daily at midnight UTC
Task order: generate_events → spark_transform → run_analytics → quality_check
Retries failed tasks 2 times with 5 minute delay
PythonOperator data quality gate validates output exists


* Real Pipeline Output
=== Top 5 Revenue Categories ===
+-------------+-------------+------------+--------------------+
| category    | total_revenue| total_orders| avg_abandonment_%  |
+-------------+-------------+------------+--------------------+
| Electronics | 153070.32   | 201        | 24.2               |
| Home&Garden | 51179.79    | 163        | 33.4               |
| Sports      | 39972.90    | 181        | 33.3               |
| Clothing    | 22552.15    | 175        | 20.2               |
| Books       | 4534.52     | 165        | 36.3               |
+-------------+-------------+------------+--------------------+

=== Cart Abandonment Rate ===
Books         37.5%
Sports        34.2%
Home & Garden 33.7%
Clothing      25.5%
Electronics   22.4%   ← best performing

=== Refund Rate by Category ===
Books         52.7%   ← highest refund rate (business alert)
Clothing      34.3%
Electronics   42.4%
Sports        39.4%
Home & Garden 40.1%

* Running Tests
bashpytest tests/ -v
14 unit tests covering:

clean() — deduplication, null drops, bad event type filtering
enrich() — timestamp parsing, metadata flattening, revenue logic
aggregate_daily() — schema validation, abandonment rate bounds
Kafka simulator — producer/consumer roundtrip, offset sequencing


* Key Design Decisions

Explicit schema — StructType defined manually, not inferred — catches bad data at read time
Idempotent pipeline — rerunning on same date overwrites, never duplicates
Separation of concerns — ingestion, transformation, orchestration, tests all isolated
Kafka abstraction — simulator mirrors real API so swapping to Azure Event Hubs needs zero logic changes


* Azure Production Mapping
This pipeline maps directly to Azure services with minimal code changes:
Local ComponentAzure EquivalentCode Changeevent_generator.pyAzure Event HubsConnection string onlykafka_simulator.pyAzure Event Hubs SDKImport swapspark_transform.pyAzure DatabricksOne path changedata/raw/ folderAzure Data Lake Storage Gen2One path changedata/processed/ folderAzure Data Lake Storage Gen2One path changeanalytics.pyAzure Synapse AnalyticsSame SQL queriesAirflow DAGAzure Data FactoryVisual pipeline configDocker containerAzure Container RegistryPush image

* Contact
Built by [Akash Kumar] |  | [github.com/Akashkmr180]