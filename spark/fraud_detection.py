from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, round

spark = (
    SparkSession.builder
    .appName("BankingFraudDetection")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

input_path = "data/processed/transactions_curated"

print("\n===== READING CURATED TRANSACTIONS =====")

df = spark.read.parquet(input_path)

print(f"Total records: {df.count()}")

print("\n===== FRAUD TRANSACTION COUNT =====")

fraud_count = df.filter(col("isFraud") == 1).count()
normal_count = df.filter(col("isFraud") == 0).count()

print(f"Fraud transactions : {fraud_count}")
print(f"Normal transactions: {normal_count}")

print("\n===== FRAUD BY TRANSACTION TYPE =====")

(
    df.groupBy("type")
    .agg(
        count("*").alias("total_transactions"),
        sum("isFraud").alias("fraud_transactions")
    )
    .withColumn(
        "fraud_rate_percent",
        round(
            col("fraud_transactions") * 100 / col("total_transactions"),
            4
        )
    )
    .orderBy(col("fraud_transactions").desc())
    .show(truncate=False)
)

print("\n===== FRAUD AMOUNT SUMMARY =====")

(
    df.filter(col("isFraud") == 1)
    .agg(
        count("*").alias("fraud_transactions"),
        round(sum("amount"), 2).alias("total_fraud_amount")
    )
    .show(truncate=False)
)

print("\n===== HIGH RISK TRANSACTIONS =====")

(
    df.filter(col("risk_level") == "HIGH")
    .select(
        "step",
        "type",
        "amount",
        "isFraud",
        "risk_score",
        "risk_level"
    )
    .show(20, truncate=False)
)

print("\n===== FRAUD DETECTION COMPLETED SUCCESSFULLY =====")

spark.stop()