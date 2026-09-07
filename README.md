# Banking Fraud Detection & ETL Pipeline

An end-to-end, **production-inspired data engineering pipeline** that ingests, transforms, and analyzes large-scale banking transaction data to identify fraudulent activity.

The pipeline is orchestrated using **Apache Airflow** and processes **6,362,620 transactions** from raw ingestion through transformation, fraud-risk feature engineering, and PostgreSQL loading for downstream SQL analysis.

## Problem Statement

Banks process millions of transactions daily and need automated systems to identify potentially fraudulent activity without manually reviewing every transaction.

This project simulates that workflow using the **PaySim synthetic transaction dataset**, implementing ingestion, transformation, feature engineering, data validation, orchestration, and analytical storage while keeping each stage reproducible and auditable.

## Architecture

```text
PaySim CSV
    |
    v
Python Ingestion
    |
    v
Data Transformation
(Python / PyArrow)
    |
    v
Curated Parquet Data
    |
    v
Fraud Detection &
Feature Engineering
    |
    v
Apache Airflow
Orchestration
    |
    v
PostgreSQL
fact_transactions
    |
    v
SQL Fraud Analysis
```

## Key Highlights

* **Engineered** an end-to-end ETL pipeline processing **6.3M+ banking transactions**, from raw CSV ingestion to a queryable fraud-analytics table.

* **Orchestrated** the complete workflow — extract, transform, fraud detection, and load — as a **4-task Apache Airflow DAG**, with all tasks executing successfully end-to-end.

* **Implemented fraud-risk feature engineering** including high-value transaction detection, balance-mismatch detection, composite risk scoring, and LOW / MEDIUM / HIGH risk classification.

* **Optimized PostgreSQL ingestion** by replacing row-based `executemany()` inserts with PostgreSQL bulk `COPY`, resolving connection timeouts during multi-million-row loading.

* **Used Parquet as a curated storage layer** between transformation and database loading, providing an efficient columnar format for analytics workflows.

* **Validated data integrity** by reconciling fraudulent and genuine transaction counts against the total loaded records, confirming zero record loss.

## Pipeline Flow

### 1. Extract / Ingestion

Raw PaySim transaction data is read from the project's data directory and prepared for downstream processing.

### 2. Transform

Transaction data is cleaned and transformed using **Python and PyArrow**, then persisted as curated Parquet files.

The transformation handles fields including:

* Transaction step
* Transaction type
* Transaction amount
* Origin and destination accounts
* Pre-transaction balances
* Post-transaction balances
* Fraud indicators

### 3. Fraud Detection & Feature Engineering

Derived fraud-risk features are generated for each transaction:

| Feature                 | Description                                                        |
| ----------------------- | ------------------------------------------------------------------ |
| `high_value_flag`       | Flags transactions above a configured high-value threshold         |
| `balance_mismatch_flag` | Flags inconsistencies between expected and actual account balances |
| `risk_score`            | Composite score based on fraud-related signals                     |
| `risk_level`            | Categorizes transactions as LOW, MEDIUM, or HIGH risk              |

### 4. Airflow Orchestration

The complete workflow runs as a single Airflow DAG:

```text
extract
   ↓
transform
   ↓
fraud_detection
   ↓
load_to_postgres
```

Airflow manages task dependencies, execution, retries, and workflow visibility.

### 5. PostgreSQL Loading

Curated Parquet data is bulk-loaded into the `fact_transactions` table using PostgreSQL's `COPY` command instead of row-by-row inserts.

This approach was selected to handle the multi-million-row dataset efficiently and avoid the connection timeout encountered with `executemany()`.

## Final Dataset Statistics

| Metric                  |     Count |
| ----------------------- | --------: |
| Total transactions      | 6,362,620 |
| Fraudulent transactions |     8,213 |
| Genuine transactions    | 6,354,407 |
| LOW risk                | 5,041,152 |
| MEDIUM risk             | 1,321,452 |
| HIGH risk               |        16 |

Fraudulent and genuine transaction counts reconcile exactly with the total:

```text
8,213 + 6,354,407 = 6,362,620
```

## Database Schema

### `fact_transactions`

| Column                    | Description                            |
| ------------------------- | -------------------------------------- |
| `step`                    | Simulation time step                   |
| `transaction_type`        | Type of transaction                    |
| `amount`                  | Transaction amount                     |
| `origin_account`          | Source account                         |
| `destination_account`     | Destination account                    |
| `old_balance_origin`      | Origin balance before transaction      |
| `new_balance_origin`      | Origin balance after transaction       |
| `old_balance_destination` | Destination balance before transaction |
| `new_balance_destination` | Destination balance after transaction  |
| `is_fraud`                | Fraud indicator                        |
| `is_flagged_fraud`        | Original PaySim fraud flag             |
| `high_value_flag`         | High-value transaction indicator       |
| `balance_mismatch_flag`   | Balance inconsistency indicator        |
| `risk_score`              | Calculated risk score                  |
| `risk_level`              | LOW / MEDIUM / HIGH                    |

## Sample SQL Analysis

### Overall Fraud Rate

```sql
SELECT
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS fraud_rate_percentage
FROM fact_transactions;
```

### Fraud Rate by Transaction Type

```sql
SELECT
    transaction_type,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS fraud_rate_percentage
FROM fact_transactions
GROUP BY transaction_type
ORDER BY fraud_rate_percentage DESC;
```

### Fraud by Risk Level

```sql
SELECT
    risk_level,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions
FROM fact_transactions
GROUP BY risk_level
ORDER BY fraud_transactions DESC;
```

### High-Value Fraudulent Transactions

```sql
SELECT
    transaction_type,
    amount,
    origin_account,
    destination_account,
    risk_score,
    risk_level
FROM fact_transactions
WHERE is_fraud = 1
  AND high_value_flag = 1
ORDER BY amount DESC
LIMIT 20;
```

## Project Structure

```text
Banking_Fraud_Pipeline/
│
├── airflow/
│   └── banking_pipeline_dag.py
│
├── config/
│   └── config.py
│
├── data/
│   ├── raw/
│   └── processed/
│       └── transactions_curated/
│
├── ingestion/
│   └── transaction_ingestion.py
│
├── load/
│   ├── load_to_postgres.py
│   └── load_to_postgres_backup.py
│
├── quality/
│   └── validation.py
│
├── spark/
│   ├── transaction_transform.py
│   └── fraud_detection.py
│
├── logs/
│
├── README.md
├── requirements.txt
└── .gitignore
```

> Note: The `spark/` directory contains processing scripts developed during the project. The currently validated end-to-end workflow uses Python/PyArrow for transformation and does not claim successful PySpark execution.

## Tech Stack

`Python` · `Pandas` · `PyArrow` · `Parquet` · `Apache Airflow` · `PostgreSQL` · `SQL` · `ETL` · `Data Validation` · `Feature Engineering`

## How to Run

Activate the Airflow environment:

```bash
source ~/airflow_venv/bin/activate
```

Start the Airflow services and trigger the DAG:

```text
banking_fraud_pipeline
```

Task sequence:

```text
extract -> transform -> fraud_detection -> load_to_postgres
```

After successful execution, validate the loaded data by querying the `fact_transactions` table in PostgreSQL.

## Roadmap

The project can be extended toward a cloud-native data engineering architecture:

* Migrate raw storage from local disk to **AWS S3**
* Replace local PostgreSQL with **Amazon Redshift Serverless**
* Provision infrastructure using **Terraform**
* Move large-scale PySpark processing to **AWS EMR**
* Add **Great Expectations** for automated data quality checks
* Build a **Streamlit** dashboard for fraud monitoring
* Normalize the transaction model into a proper **star schema**

  * `dim_account`
  * `dim_transaction_type`
  * `dim_date`
  * `fact_transactions`

## Project Outcome

This project demonstrates a complete, production-inspired data engineering workflow on a large banking transaction dataset, covering:

* Large-scale data ingestion
* Data transformation
* Parquet-based curated storage
* Fraud feature engineering
* Risk scoring
* Data validation
* Apache Airflow orchestration
* PostgreSQL bulk loading
* Analytical SQL

The complete pipeline successfully processed and verified **6,362,620 transactions** end-to-end.
