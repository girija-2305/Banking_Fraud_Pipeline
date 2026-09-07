import os
import io
import glob
import psycopg2
from dotenv import load_dotenv
import pyarrow.parquet as pq

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

PARQUET_PATH = "data/processed/transactions_curated"

print("\n===== CONNECTING TO POSTGRESQL =====", flush=True)

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

print("PostgreSQL connection successful", flush=True)

cursor.execute("TRUNCATE TABLE fact_transactions RESTART IDENTITY")
conn.commit()

print("Existing data cleared", flush=True)
print("\n===== LOADING CURATED DATA =====", flush=True)

parquet_files = glob.glob(
    os.path.join(PARQUET_PATH, "part-*.parquet")
)

if not parquet_files:
    raise FileNotFoundError("No Parquet files found")

print(f"Parquet files found: {len(parquet_files)}", flush=True)

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

copy_sql = """
COPY fact_transactions (
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
FROM STDIN WITH CSV
"""

total_loaded = 0

for parquet_path in parquet_files:

    print(f"Reading: {parquet_path}", flush=True)

    parquet_file = pq.ParquetFile(parquet_path)

    for batch in parquet_file.iter_batches(
        batch_size=50000,
        columns=columns
    ):

        rows = batch.to_pydict()

        buffer = io.StringIO()

        for row in zip(
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
        ):
            values = []

            for value in row:
                if value is None:
                    values.append("")
                else:
                    value = str(value).replace('"', '""')
                    values.append(f'"{value}"')

            buffer.write(",".join(values) + "\n")

        buffer.seek(0)

        cursor.copy_expert(copy_sql, buffer)
        conn.commit()

        total_loaded += len(rows["step"])

        print(
            f"Loaded records: {total_loaded}",
            flush=True
        )

print("\n===== LOAD COMPLETED =====", flush=True)
print(f"Total records loaded: {total_loaded}", flush=True)

cursor.close()
conn.close()

print("PostgreSQL connection closed", flush=True)