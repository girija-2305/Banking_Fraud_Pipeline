from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum

spark = (
    SparkSession.builder
    .appName("BankingDataQuality")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

input_path = "data/processed/transactions_curated"

print("\n===== READING CURATED DATA =====")

df = spark.read.parquet(input_path)

total_records = df.count()

print(f"Total records: {total_records}")

print("\n===== DATA QUALITY CHECKS =====")

# 1. Record count check
if total_records > 0:
    print("PASS: Record count is greater than zero")
else:
    print("FAIL: Dataset is empty")

# 2. Null value check
print("\n--- NULL VALUE CHECK ---")

null_counts = df.select(
    [
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ]
)

null_counts.show(truncate=False)

# 3. Negative amount check
negative_amounts = df.filter(col("amount") < 0).count()

if negative_amounts == 0:
    print("PASS: No negative transaction amounts")
else:
    print(f"FAIL: {negative_amounts} negative transaction amounts found")

# 4. Valid transaction type check
valid_types = [
    "CASH_IN",
    "CASH_OUT",
    "DEBIT",
    "PAYMENT",
    "TRANSFER"
]

invalid_types = df.filter(
    ~col("type").isin(valid_types)
).count()

if invalid_types == 0:
    print("PASS: All transaction types are valid")
else:
    print(f"FAIL: {invalid_types} invalid transaction types found")

# 5. Fraud label validation
invalid_fraud_labels = df.filter(
    ~col("isFraud").isin(0, 1)
).count()

if invalid_fraud_labels == 0:
    print("PASS: Fraud labels are valid")
else:
    print(f"FAIL: {invalid_fraud_labels} invalid fraud labels found")

# 6. Risk level validation
valid_risk_levels = [
    "LOW",
    "MEDIUM",
    "HIGH"
]

invalid_risk_levels = df.filter(
    ~col("risk_level").isin(valid_risk_levels)
).count()

if invalid_risk_levels == 0:
    print("PASS: All risk levels are valid")
else:
    print(f"FAIL: {invalid_risk_levels} invalid risk levels found")

# 7. Duplicate check
duplicate_count = (
    df.groupBy("step", "nameOrig", "nameDest", "amount")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_count == 0:
    print("PASS: No duplicate transaction combinations found")
else:
    print(f"WARNING: {duplicate_count} duplicate transaction combinations found")

print("\n===== DATA QUALITY SUMMARY =====")

print(f"Total records          : {total_records}")
print(f"Negative amounts       : {negative_amounts}")
print(f"Invalid transaction types: {invalid_types}")
print(f"Invalid fraud labels   : {invalid_fraud_labels}")
print(f"Invalid risk levels    : {invalid_risk_levels}")
print(f"Duplicate combinations : {duplicate_count}")

print("\n===== DATA QUALITY VALIDATION COMPLETED =====")

spark.stop()