import os
import psycopg2
from dotenv import load_dotenv
import pyarrow.parquet as pq
import glob

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

PARQUET_PATH = "data/processed/transactions_curated"

print("\n===== CONNECTING TO POSTGRESQL =====")

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

print("PostgreSQL connection successful")
cursor.execute("TRUNCATE TABLE fact_transactions RESTART IDENTITY")
conn.commit()
print("Existing data cleared")

print("\n===== LOADING CURATED DATA =====")

parquet_files = glob.glob(
    os.path.join(PARQUET_PATH, "part-*.parquet")
)

if not parquet_files:
    raise FileNotFoundError("No Parquet files found")

print(f"Parquet files found: {len(parquet_files)}")

columns = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "nameDest",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
    "high_value_flag",
    "balance_mismatch_flag",
    "risk_score",
    "risk_level",
]

insert_sql = """
INSERT INTO fact_transactions (
    step,
    transaction_type,
    amount,
    origin_account,
    destination_account,
    old_balance_origin,
    new_balance_origin,
    old_balance_destination,
    new_balance_destination,
    is_fraud,
    is_flagged_fraud,
    high_value_flag,
    balance_mismatch_flag,
    risk_score,
    risk_level
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

total_loaded = 0

for parquet_path in parquet_files:

    print(f"Reading: {parquet_path}")

    parquet_file = pq.ParquetFile(parquet_path)

    for batch in parquet_file.iter_batches(
        batch_size=10000,
        columns=columns
    ):
        rows = batch.to_pydict()

        data = list(zip(
            rows["step"],
            rows["type"],
            rows["amount"],
            rows["nameOrig"],
            rows["nameDest"],
            rows["oldbalanceOrg"],
            rows["newbalanceOrig"],
            rows["oldbalanceDest"],
            rows["newbalanceDest"],
            rows["isFraud"],
            rows["isFlaggedFraud"],
            rows["high_value_flag"],
            rows["balance_mismatch_flag"],
            rows["risk_score"],
            rows["risk_level"],
        ))

        cursor.executemany(insert_sql, data)
        conn.commit()

        total_loaded += len(data)

        print(f"Loaded records: {total_loaded}")

print("\n===== LOAD COMPLETED =====")
print(f"Total records loaded: {total_loaded}")

cursor.close()
conn.close()

print("PostgreSQL connection closed")