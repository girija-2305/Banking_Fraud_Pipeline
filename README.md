# Banking Fraud Detection & ETL Pipeline

An end-to-end, production-style data engineering pipeline that ingests, transforms, and analyzes large-scale banking transaction data to detect fraudulent activity — built to reflect how a real fraud-analytics platform would be engineered inside a bank.

Orchestrated entirely with **Apache Airflow**, the pipeline reliably processes **6,362,620 transactions** from raw ingestion through curated fraud-risk scoring, loading the final dataset into **PostgreSQL** for downstream SQL analysis.

## Problem Statement

Banks process millions of transactions daily and need automated systems to flag potentially fraudulent activity without manual review of every record. This project simulates that workflow using the PaySim synthetic transaction dataset — building the ingestion, transformation, feature engineering, and orchestration layers a real fraud-detection pipeline would require, while keeping every stage auditable and reproducible.

## Architecture

```text
PaySim CSV
    |
    v
Python Ingestion
    |
    v
Data Transformation (Python / PyArrow)
    |
    v
Parquet Curated Data
    |
    v
Fraud Detection & Feature Engineering
    |
    v
Apache Airflow Orchestration
    |
    v
PostgreSQL (fact_transactions)
    |
    v
SQL Fraud Analysis
```

## Key Highlights

* **Engineered** an end-to-end ETL pipeline processing **6.3M+ banking transactions**, from raw CSV ingestion to a queryable fraud-analytics table.
* **Orchestrated** the full workflow — extract, transform, fraud detection, load — as a 4-task DAG in **Apache Airflow**, with all tasks executing successfully end-to-end.
* **Designed and implemented fraud-risk feature engineering**, including high-value transaction flags, balance-mismatch detection, and a composite risk score (LOW / MEDIUM / HIGH).
* **Optimized PostgreSQL ingestion** by switching from row-by-row `executemany()` inserts to bulk `COPY`, eliminating connection timeouts and significantly improving load performance at scale.
* **Used Parquet as a curated storage layer** between transformation and loading, reducing I/O overhead and keeping the pipeline columnar and analytics-ready.
* **Validated data integrity** by reconciling fraud/genuine transaction counts against the total row count post-load, confirming zero data loss across the pipeline.

## Pipeline Flow

### 1. Extract / Ingestion
Raw PaySim transaction data is read from the project's data directory and staged for transformation.

### 2. Transform
Transaction data is cleaned and transformed using Python and PyArrow, then persisted as curated Parquet files. Fields handled include transaction step, type, amount, origin/destination accounts, pre- and post-transaction balances, and the fraud indicator.

### 3. Fraud Detection & Feature Engineering
Derived fraud-risk features are generated for each transaction:

| Feature | Description |
|---|---|
| `high_value_flag` | Flags transactions above a high-value threshold |
| `balance_mismatch_flag` | Flags inconsistencies between expected and actual account balances |
| `risk_score` | Composite score combining fraud signals |
| `risk_level` | Categorized as LOW, MEDIUM, or HIGH |

### 4. Airflow Orchestration
The complete workflow runs as a single DAG:

```text
extract -> transform -> fraud_detection -> load_to_postgres
```

All four tasks execute successfully end-to-end, with Airflow managing task dependencies, retries, and execution visibility.

### 5. PostgreSQL Loading
Curated Parquet output is bulk-loaded into the `fact_transactions` table using PostgreSQL's `COPY` command rather than row-by-row inserts — a deliberate choice to handle multi-million-row loads efficiently and avoid connection timeouts.

## Final Dataset Statistics

| Metric | Count |
|---|---:|
| Total transactions | 6,362,620 |
| Fraudulent transactions | 8,213 |
| Genuine transactions | 6,354,407 |
| LOW risk | 5,041,152 |
| MEDIUM risk | 1,321,452 |
| HIGH risk | 16 |

Fraud and genuine transaction counts reconcile exactly with the total:
```text
8,213 + 6,354,407 = 6,362,620
```

## Database Schema

### `fact_transactions`

| Column | Description |
|---|---|
| `step` | Simulation time step |
| `transaction_type` | Type of transaction |
| `amount` | Transaction amount |
| `origin_account` | Source account |
| `destination_account` | Destination account |
| `old_balance_origin` | Origin balance before transaction |
| `new_balance_origin` | Origin balance after transaction |
| `old_balance_destination` | Destination balance before transaction |
| `new_balance_destination` | Destination balance after transaction |
| `is_fraud` | Fraud indicator |
| `is_flagged_fraud` | Original PaySim fraud flag |
| `high_value_flag` | High-value transaction indicator |
| `balance_mismatch_flag` | Balance inconsistency indicator |
| `risk_score` | Calculated risk score |
| `risk_level` | LOW / MEDIUM / HIGH |

## Sample SQL Analysis

**Overall fraud rate:**
```sql
SELECT
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    ROUND(100.0 * SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_rate_percentage
FROM fact_transactions;
```

**Fraud rate by transaction type:**
```sql
SELECT
    transaction_type,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions,
    ROUND(100.0 * SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS fraud_rate_percentage
FROM fact_transactions
GROUP BY transaction_type
ORDER BY fraud_rate_percentage DESC;
```

**Fraud by risk level:**
```sql
SELECT
    risk_level,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions
FROM fact_transactions
GROUP BY risk_level
ORDER BY fraud_transactions DESC;
```

**High-value fraudulent transactions:**
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
├── airflow/
│   └── banking_pipeline_dag.py
├── config/
│   └── config.py
├── data/
│   ├── raw/
│   └── processed/
│       └── transactions_curated/
├── ingestion/
│   └── transaction_ingestion.py
├── load/
│   ├── load_to_postgres.py
│   └── load_to_postgres_backup.py
├── quality/
│   └── validation.py
├── spark/
│   ├── transaction_transform.py
│   └── fraud_detection.py
├── logs/
├── README.md
├── requirements.txt
└── .gitignore
```

## Tech Stack

`Python` · `Pandas` · `PyArrow` · `Parquet` · `Apache Airflow` · `PostgreSQL` · `SQL` · `ETL` · `Data Validation` · `Feature Engineering`

## How to Run

Activate the Airflow environment:
```bash
source ~/airflow_venv/bin/activate
```

Start Airflow services, then trigger the DAG:
```text
banking_fraud_pipeline
```

Task sequence:
```text
extract -> transform -> fraud_detection -> load_to_postgres
```

After successful execution, validate the load by querying `fact_transactions` directly in PostgreSQL or via the project's Python database connection.

## Roadmap

This project is being extended toward a full cloud-native architecture:

* Migrate raw storage from local disk to **AWS S3**
* Replace local PostgreSQL with **Amazon Redshift Serverless** as the analytics warehouse
* Provision infrastructure with **Terraform**
* Move PySpark processing to **AWS EMR**
* Add **Great Expectations** for automated data quality validation
* Build a **Streamlit** dashboard for fraud monitoring
* Normalize `fact_transactions` into a proper star schema (`dim_account`, `dim_transaction_type`, `dim_date`)

## Project Outcome

This project demonstrates a complete, production-style data engineering workflow on a large banking dataset — spanning ingestion, distributed transformation, fraud feature engineering, risk scoring, workflow orchestration, and analytical SQL querying — with all **6,362,620 transactions** successfully processed and verified end-to-end.
