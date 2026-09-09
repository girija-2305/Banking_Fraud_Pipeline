# Banking Fraud Detection & ETL Pipeline

An end-to-end, production-inspired data engineering pipeline that ingests, transforms, validates, analyzes, and stores large-scale banking transaction data for fraud-risk analysis.

The pipeline uses **AWS S3 for raw data storage**, **Apache Airflow for orchestration**, **Python/PyArrow for transformation**, and **PostgreSQL for analytical storage**.

The validated pipeline successfully processed **6,362,620 PaySim transactions** end-to-end.

---

## Problem Statement

Banks process millions of transactions and need automated systems to identify potentially fraudulent activity and prioritize high-risk transactions.

This project simulates a banking fraud data engineering workflow using the **PaySim synthetic transaction dataset**.

The pipeline demonstrates:

* Cloud-based raw data storage
* Data ingestion
* Data transformation
* Fraud feature engineering
* Data quality validation
* Workflow orchestration
* PostgreSQL bulk loading
* Fraud-risk analysis using SQL

---

## Architecture

```text
                    AWS S3
              Raw PaySim Dataset
                      |
                      v
              +----------------+
              | Download from  |
              |      S3        |
              +-------+--------+
                      |
                      v
              +----------------+
              | Transformation |
              | Python/PyArrow  |
              +-------+--------+
                      |
                      v
              +----------------+
              | Data Validation|
              +-------+--------+
                      |
                      v
              +----------------+
              |   PostgreSQL   |
              |fact_transactions|
              +-------+--------+
                      |
                      v
              +----------------+
              | Fraud Analysis |
              | Risk Scoring   |
              +----------------+

              Apache Airflow
              Orchestration
```

---

## Airflow DAG

The complete workflow is orchestrated using Apache Airflow.

```text
download_from_s3
        |
        v
transform_data
        |
        v
validate_data
        |
        v
load_to_postgres
        |
        v
fraud_analysis
```

All five tasks were successfully executed in the validated pipeline run.

---

## Key Highlights

* **Processed 6.36M+ banking transactions** from the PaySim dataset through an end-to-end ETL pipeline.

* **Integrated AWS S3** as the raw-data storage layer and implemented programmatic S3 ingestion using Python and Boto3.

* **Orchestrated five pipeline stages using Apache Airflow**, including S3 ingestion, transformation, validation, PostgreSQL loading, and fraud analysis.

* **Implemented fraud-risk feature engineering** using high-value transaction detection, balance-mismatch detection, composite risk scoring, and LOW/MEDIUM/HIGH risk classification.

* **Optimized PostgreSQL loading** using PostgreSQL bulk `COPY` to efficiently handle multi-million-row data loading.

* **Used Parquet as a curated storage layer** between transformation and database loading.

* **Validated data integrity** by reconciling transaction counts and verifying the final PostgreSQL dataset.

* Implemented AWS IAM access using a **least-privilege S3 policy** allowing only the required bucket listing, object read, and object write operations.

---

# Pipeline Components

## 1. AWS S3 — Raw Data Storage

The raw PaySim dataset is stored in an Amazon S3 bucket.

```text
S3 Bucket
│
├── raw/
│   └── PS_20174392719_1491204439457_log.csv.zip
│
├── processed/
│
└── fraud/
```

The pipeline downloads the raw dataset from S3 using **Boto3**.

The S3 download is implemented in:

```text
ingestion/s3_download.py
```

---

## 2. Data Ingestion

The `s3_download.py` script:

1. Connects to Amazon S3 using Boto3.
2. Reads the configured bucket and object key.
3. Downloads the raw PaySim ZIP file.
4. Stores it in the project's local raw-data directory.

Example S3 object:

```text
s3://<bucket>/raw/PS_20174392719_1491204439457_log.csv.zip
```

AWS credentials are managed outside the source code using the AWS CLI configuration.

---

## 3. Data Transformation

Transaction data is cleaned and transformed using **Python and PyArrow**.

The transformation creates a curated Parquet dataset for downstream processing.

Important transaction attributes include:

* Transaction step
* Transaction type
* Transaction amount
* Origin account
* Destination account
* Origin balance before transaction
* Origin balance after transaction
* Destination balance before transaction
* Destination balance after transaction
* Fraud indicator

Curated data is stored in:

```text
data/processed/transactions_curated/
```

---

## 4. Fraud Detection & Feature Engineering

The pipeline generates fraud-related features for every transaction.

| Feature                 | Description                                                       |
| ----------------------- | ----------------------------------------------------------------- |
| `high_value_flag`       | Identifies transactions above the configured high-value threshold |
| `balance_mismatch_flag` | Identifies inconsistencies between expected and actual balances   |
| `risk_score`            | Composite fraud-risk score                                        |
| `risk_level`            | Categorizes transactions as LOW, MEDIUM, or HIGH risk             |

The risk classification is performed by the fraud-analysis stage of the pipeline.

---

## 5. Data Validation

The validation stage checks the processed transaction data before database loading.

Validation includes checking:

* Transaction record counts
* Fraud/genuine record counts
* Required fields
* Data consistency
* Record reconciliation

The validation task must complete successfully before the data is loaded into PostgreSQL.

---

## 6. PostgreSQL Loading

The curated transaction data is loaded into PostgreSQL.

Target table:

```text
fact_transactions
```

PostgreSQL bulk `COPY` is used instead of inserting records individually.

This provides a more efficient loading approach for the multi-million-row PaySim dataset.

---

# Final Dataset Statistics

The completed pipeline loaded:

| Metric                  |         Count |
| ----------------------- | ------------: |
| Total transactions      | **6,362,620** |
| Fraudulent transactions |     **8,213** |
| Genuine transactions    | **6,354,407** |
| LOW risk                | **5,041,152** |
| MEDIUM risk             | **1,321,452** |
| HIGH risk               |        **16** |

Fraudulent and genuine transaction counts reconcile exactly:

```text
8,213 + 6,354,407 = 6,362,620
```

Risk-level counts also reconcile exactly:

```text
5,041,152 + 1,321,452 + 16 = 6,362,620
```

---

# Database Schema

## `fact_transactions`

| Column                    | Description                            |
| ------------------------- | -------------------------------------- |
| `transaction_id`          | Unique transaction identifier          |
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

---

# Sample SQL Analysis

## Overall Fraud Rate

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

## Fraud Rate by Transaction Type

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

## Fraud by Risk Level

```sql
SELECT
    risk_level,
    COUNT(*) AS total_transactions,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END) AS fraud_transactions
FROM fact_transactions
GROUP BY risk_level
ORDER BY fraud_transactions DESC;
```

## High-Value Fraudulent Transactions

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

---

# Project Structure

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
│   ├── s3_download.py
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

> Note: The `spark/` directory contains processing scripts developed during the project. The currently validated end-to-end workflow uses Python/PyArrow for transformation. PySpark execution is not claimed as part of the validated pipeline.

---

# Tech Stack

* **Python**
* **Pandas**
* **PyArrow**
* **Parquet**
* **Boto3**
* **Amazon S3**
* **Apache Airflow**
* **PostgreSQL**
* **SQL**
* **Data Validation**
* **Fraud Feature Engineering**
* **ETL**

---

# How to Run

Activate the Airflow environment:

```bash
source ~/airflow_venv/bin/activate
```

Start the Airflow scheduler:

```bash
airflow scheduler
```

Trigger the DAG:

```bash
airflow dags trigger banking_fraud_pipeline
```

The DAG executes the following workflow:

```text
download_from_s3
        ↓
transform_data
        ↓
validate_data
        ↓
load_to_postgres
        ↓
fraud_analysis
```

After successful execution, the final data can be verified in PostgreSQL using the `fact_transactions` table.

---

# AWS Configuration

The project uses:

* Amazon S3 for raw data storage
* IAM for controlled access
* AWS CLI for local authentication
* Boto3 for Python-based S3 access
* AWS Budget for cost monitoring

The S3 IAM policy follows a limited-access approach, allowing only the required:

```text
s3:ListBucket
s3:GetObject
s3:PutObject
```

No AWS credentials are stored in the source code.

---

# Future Enhancements

Possible future improvements include:

* Migrate transformation processing to **Apache Spark/PySpark**
* Run large-scale Spark processing using **AWS EMR**
* Replace local PostgreSQL with **Amazon Redshift Serverless**
* Provision AWS infrastructure using **Terraform**
* Add **Great Expectations** for advanced data-quality validation
* Build a **Streamlit** fraud-monitoring dashboard
* Introduce a proper **star-schema data warehouse**
* Add dimensional tables such as:

  * `dim_account`
  * `dim_transaction_type`
  * `dim_date`
  * `fact_transactions`
* Add automated CI/CD for pipeline testing and deployment

---

# Project Outcome

This project demonstrates an end-to-end data engineering workflow using a large banking transaction dataset.

The implemented pipeline covers:

* AWS S3 raw-data ingestion
* Large-scale transaction processing
* Python/PyArrow transformation
* Parquet-based curated storage
* Fraud feature engineering
* Risk scoring
* Data validation
* Apache Airflow orchestration
* PostgreSQL bulk loading
* Analytical SQL
* AWS IAM access control

The complete validated pipeline successfully processed and verified:

**6,362,620 banking transactions**

with the final risk distribution:

```text
LOW       5,041,152
MEDIUM    1,321,452
HIGH             16
```

The project provides a practical foundation for transitioning the pipeline toward a fully cloud-native data engineering architecture.
