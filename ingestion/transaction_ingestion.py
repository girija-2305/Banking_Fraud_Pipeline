import os
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import DB_CONFIG

load_dotenv()

PARQUET_PATH = "data/processed/transactions_curated"
BATCH_SIZE = 10000

print("\n===== READING CURATED DATA =====")

df = pd.read_parquet(PARQUET_PATH)

print(f"Total curated records: {len(df)}")

# Generate transaction IDs
df.insert(0, "transaction_id", range(1, len(df) + 1))

# Rename columns to match PostgreSQL
df = df.rename(columns={
    "type": "transaction_type",
    "nameOrig": "origin_account",
    "nameDest": "destination_account",
    "oldbalanceOrg": "old_balance_origin",
    "newbalanceOrig": "new_balance_origin",
    "oldbalanceDest": "old_balance_destination",
    "newbalanceDest": "new_balance_destination",
    "isFraud": "is_fraud",
    "isFlaggedFraud": "is_flagged_fraud"
})

columns = [
    "transaction_id",
    "step",
    "transaction_type",
    "amount",
    "origin_account",
    "destination_account",
    "old_balance_origin",
    "new_balance_origin",
    "old_balance_destination",
    "new_balance_destination",
    "is_fraud",
    "is_flagged_fraud",
    "high_value_flag",
    "balance_mismatch_flag",
    "risk_score",
    "risk_level"
]

df = df[columns]

print("\n===== CONNECTING TO POSTGRESQL =====")

conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

print("PostgreSQL connection successful")

print("\n===== CLEARING EXISTING DATA =====")

cursor.execute("TRUNCATE TABLE fact_transactions RESTART IDENTITY")
conn.commit()

print("Existing fact_transactions data cleared")

insert_sql = """
INSERT INTO fact_transactions (
    transaction_id,
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
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s
)
"""

print("\n===== LOADING TRANSACTIONS =====")

total = len(df)

for start in range(0, total, BATCH_SIZE):
    batch = df.iloc[start:start + BATCH_SIZE]

    records = [
        tuple(row)
        for row in batch.itertuples(index=False, name=None)
    ]

    cursor.executemany(insert_sql, records)
    conn.commit()

    print(f"Loaded {min(start + BATCH_SIZE, total):,} / {total:,}")

cursor.close()
conn.close()

print("\n===== INGESTION COMPLETED SUCCESSFULLY =====")
