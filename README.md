Insurance PySpark ETL Pipeline

Production-ready PySpark ETL that ingests incremental data from MySQL (customers, policies, claims, payments), performs enrichment & lookups, computes aggregated metrics, writes metrics back to MySQL, and tracks incremental progress using an offsets table.

This README gives step-by-step instructions to set up, run, and extend the project.

Table of contents

Project overview

Folder structure

Prerequisites

Configuration (conf/config.yml)

Database setup (SQL snippets)

How incremental loading & offsets work

Run locally (dev)

Run on cluster (production / spark-submit)

Logging, monitoring & troubleshooting

Tests & data generation

Deployment suggestions & improvements

FAQ

1. Project overview

This pipeline:

Reads incremental rows from MySQL tables (customers, policies, claims, payments) using a tracked offset (last processed id or timestamp).

Joins/enriches the data (policy ↔ customer ↔ claim ↔ payments).

Computes aggregated metrics (example: total claims / policy, total paid).

Writes metrics into MySQL (table insurance_metrics or agg_metrics_daily).

Updates offsets in etl_offsets table so next run only reads new data.

Includes logging and exception handling.

2. Folder structure
insurance-pyspark-etl/
├── conf/
│   └── config.yml
├── stages/
│   ├── ingest.py
│   ├── transform.py
│   └── writeback.py
├── utils/
│   ├── logger.py
│   ├── spark_session.py
│   ├── db_utils.py
│   └── offsets.py
├── main.py
├── scripts/
│   └── spark_submit.sh
├── sql/
│   └── create_offsets_table.sql
├── requirements.txt
└── README.md


3. Prerequisites

Java (JDK 8+ or 11) — required by Spark
Python 3.8+
Apache Spark 3.x installed (or Databricks / EMR / GCP Dataproc)
MySQL server (test or prod) accessible by Spark driver/executors
Network access between Spark cluster and MySQL port (3306)

Install Python dependencies:

pip install -r requirements.txt


Note: For Spark to connect to MySQL you must provide the MySQL JDBC driver. 
You can either:

Pass --packages mysql:mysql-connector-java:8.0.33 to spark-submit (recommended), or

Place the mysql-connector-java-x.x.x.jar in Spark jars/ directory or session classpath.

4. Configuration (conf/config.yml)


mysql:
  host: "127.0.0.1"
  port: 3306
  database: "insurance_db"
  user: "etl_user"
  password: "ChangeMe123!"

tables:
  customers:
    name: "customers"
    incremental_col: "customer_id"
    pk: "customer_id"
  policies:
    name: "policies"
    incremental_col: "policy_id"
    pk: "policy_id"
  claims:
    name: "claims"
    incremental_col: "claim_id"
    pk: "claim_id"
  payments:
    name: "payments"
    incremental_col: "payment_id"
    pk: "payment_id"

offsets_table: "etl_offsets"
metrics_table: "insurance_metrics"

app:
  app_name: "insurance_etl_job"


Security note: Do not commit DB credentials to source control. Use vault/secret manager or environment variables in production.

5. Database setup (SQL)

Create offsets and metrics tables. Example SQL (place in sql/create_offsets_table.sql):

CREATE TABLE IF NOT EXISTS etl_offsets (
  table_name VARCHAR(128) PRIMARY KEY,
  last_value VARCHAR(255),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS insurance_metrics (
  metric_date DATE,
  policy_id INT,
  total_claim_amount DECIMAL(18,2),
  total_claims INT,
  avg_payment_amount DECIMAL(18,2),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(metric_date, policy_id)
);


If you need base tables for testing, you can use the generated db_scripts.sql (the file previously created) which includes CREATE TABLE + inserts for customers, policies, claims, payments.

6. How incremental loading & offsets work

For each table we store an offset (e.g. max id or max updated_at) in etl_offsets.table_name.last_value.

On job start, pipeline reads the offset value and issues a JDBC read with WHERE incremental_col > last_value.

After processing, pipeline computes the maximum incremental_col seen and writes it back into etl_offsets (upsert).

This guarantees idempotent incremental reads (assuming primary key / timestamp monotonic increase).

Important: if using timestamps (e.g. updated_at), ensure timezone handling is consistent between MySQL and Spark.

7. Run locally (development)

Edit conf/config.yml to point to your local MySQL.

Ensure the jdbc jar or --packages available (see prerequisites).

Install dependencies:

pip install -r requirements.txt


Create required DB tables (offsets + metrics + test data):

mysql -u root -p insurance_db < sql/create_offsets_table.sql
# optionally run db_scripts.sql inserted earlier
mysql -u root -p insurance_db < path/to/db_scripts.sql


Run the pipeline with Python (local Spark):

python main.py


Running this way uses the Spark distribution available to your Python environment (ensure pyspark installed).

8. Run in production (spark-submit)

Example spark-submit command (YARN/cluster or local):

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name insurance_etl_job \
  --driver-memory 2g \
  --executor-memory 4g \
  --packages mysql:mysql-connector-java:8.0.33 \
  main.py


On standalone/local use --master local[*] for testing:

spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --packages mysql:mysql-connector-java:8.0.33 \
  main.py


If running on Databricks/EMR/GCP Dataproc, follow their job submission patterns and attach the MySQL JDBC jar as a cluster library.

9. Logging, monitoring & troubleshooting
Logging

Logs are produced by utils/logger.py and printed to stdout. Configure centralized logging for production (ELK/CloudWatch/Datadog).

logger.info() / logger.warning() / logger.error() are used across modules.

Monitoring

Add metrics export (Prometheus) or use job manager UI (YARN/Spark UI) to track job duration, shuffled bytes, and executor health.

Track errors or late jobs with alerting.

Troubleshooting

JDBC connection issues: check network, user/password, bind-address in MySQL, and JDBC driver availability.

No new data: ensure offsets are stored correctly and the offset column is monotonic.

Schema mismatch: enable schema evolution handling in transforms; validate column existence before reads.

Large joins: tune with broadcast joins (broadcast(df)) for small lookup tables or increase shuffle partitions.

10. Tests & data generation

Use db_scripts.sql (already created) to populate sample data: customers (100), policies (1000), claims (1000), payments (1000).

Add unit tests for transform logic with pytest:

Mock small DataFrames using pyspark.sql.SparkSession.builder.master("local[1]").getOrCreate().

Assert the transform & metric functions produce expected rows & aggregates.

11. Deployment suggestions & improvements

Secrets: Use HashiCorp Vault / AWS Secrets Manager / GCP Secret Manager for DB credentials. Inject into Spark job via environment variables or secure config provider.

Schema registry: Add a light schema registry or versioned schema files for each table.

Idempotency: Consider writing a job-run marker table to prevent double-processing for retry semantics.

Partitioning: Write metrics partitioned by metric_date to speed up downstream queries.

Unit & Integration tests: Add CI that runs transform tests and a small end-to-end job against a test MySQL container.

Airflow: Create DAG to schedule and monitor the job (pre/post checks, retries).

Containerize: Build a Docker image (Spark + job) for consistent runs in Kubernetes.

Data Quality: Integrate Great Expectations or custom checks to assert row counts & column constraints.

12. FAQ

Q: What if my incremental column is a timestamp?
A: Store ISO string in etl_offsets and compare using updated_at > 'YYYY-MM-DD HH:MM:SS'. Make sure timezone alignment is consistent.

Q: How to avoid reading entire table on first run?
A: Seed etl_offsets with an initial value (for ids: 0; for timestamps: a start date) or perform a controlled initial full load with a flag.

Q: What if schema changes?
A: Add safeguards in ingest and transform to check for required columns and fail-fast with clear error messages. Consider schema evolution plan.

Quick reference: useful commands

Install deps:

pip install -r requirements.txt


Run locally:

python main.py


Run via spark-submit:

spark-submit --master local[*] --packages mysql:mysql-connector-java:8.0.33 main.py


Create DB tables:

mysql -u root -p insurance_db < sql/create_offsets_table.sql
mysql -u root -p insurance_db < path/to/db_scripts.sql
