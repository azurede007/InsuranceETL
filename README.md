# Insurance PySpark ETL Pipeline

A production-ready **PySpark ETL pipeline** that extracts incremental
data from MySQL (customers, policies, claims, payments), enriches &
transforms the data, computes aggregated insurance metrics, writes
results back to MySQL, and maintains progress using an offsets table.

This README provides complete setup, usage, architecture, and
troubleshooting guidance.

## 📑 Table of Contents

1.  Project Overview
2.  Folder Structure
3.  Prerequisites
4.  Configuration (`conf/config.yml`)
5.  Database Setup
6.  Incremental Load & Offset Logic
7.  Run Locally
8.  Run in Production
9.  Logging & Monitoring


## 1. Project Overview

The ETL pipeline: - Reads incremental data using offsets stored in
MySQL. - Enriches and joins customers, policies, claims, payments. -
Computes aggregated metrics. - Writes metrics to MySQL. - Updates
offsets to maintain incremental processing.

## 2. Folder Structure

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
    ├── sql/
    │   └── db_scripts.sql
    ├── requirements.txt
    └── README.md

## 3. Prerequisites

-   Java 8/11\
-   Python 3.8+\
-   Apache Spark 3.x\
-   MySQL server\
-   Network access to MySQL port 3306

Install dependencies:

    pip install -r requirements.txt

## 4. Configuration (`conf/config.yml`)

Mysql connection info in the yaml format

## 5. Database Setup

Tables for offsets & metrics:

``` sql
CREATE TABLE IF NOT EXISTS etl_offsets (...);
CREATE TABLE IF NOT EXISTS insurance_metrics (...);
```

## 6. Incremental Load Logic

-   Read last offset.
-   Load only new rows.
-   Process, aggregate, write results.
-   Update offsets.

## 7. Run Locally

    python main.py

## 8. Run in Production (spark-submit)

    spark-submit --master local[*] --packages mysql:mysql-connector-java:8.0.33 main.py

## 9. Logging & Monitoring

Integrated logging + Spark UI for monitoring.

