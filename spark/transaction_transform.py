import os

os.environ["HADOOP_HOME"] = "/mnt/c/hadoop"
os.environ["hadoop.home.dir"] = "/mnt/c/hadoop"
os.environ["HADOOP_OPTS"] = "-Djava.library.path=/mnt/c/hadoop/bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    round,
    abs as spark_abs
)


# ---------------------------------------------------------
# 1. Create Spark Session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("BankingFraudTransformation")
    .master("local[2]")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# 2. File paths
# ---------------------------------------------------------

input_path = "data/raw/PS_20174392719_1491204439457_log.csv"
output_path = "data/processed/transactions_curated"


# ---------------------------------------------------------
# 3. Read raw transaction data
# ---------------------------------------------------------

print("\n===== READING RAW TRANSACTIONS =====")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print(f"Raw record count: {df.count()}")


# ---------------------------------------------------------
# 4. Display source schema
# ---------------------------------------------------------

print("\n===== SOURCE SCHEMA =====")
df.printSchema()


# ---------------------------------------------------------
# 5. Select required columns
# ---------------------------------------------------------

df = df.select(
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud"
)


# ---------------------------------------------------------
# 6. Remove duplicate transactions
# ---------------------------------------------------------

before_dedup = df.count()

df = df.dropDuplicates()

after_dedup = df.count()

print("\n===== DUPLICATE CHECK =====")
print(f"Records before deduplication: {before_dedup}")
print(f"Records after deduplication : {after_dedup}")
print(f"Duplicates removed          : {before_dedup - after_dedup}")


# ---------------------------------------------------------
# 7. Handle invalid/null values
# ---------------------------------------------------------

df = (
    df
    .withColumn(
        "amount",
        when(col("amount").isNull() | (col("amount") < 0), lit(0.0))
        .otherwise(col("amount"))
    )
    .withColumn(
        "oldbalanceOrg",
        when(col("oldbalanceOrg").isNull(), lit(0.0))
        .otherwise(col("oldbalanceOrg"))
    )
    .withColumn(
        "newbalanceOrig",
        when(col("newbalanceOrig").isNull(), lit(0.0))
        .otherwise(col("newbalanceOrig"))
    )
    .withColumn(
        "oldbalanceDest",
        when(col("oldbalanceDest").isNull(), lit(0.0))
        .otherwise(col("oldbalanceDest"))
    )
    .withColumn(
        "newbalanceDest",
        when(col("newbalanceDest").isNull(), lit(0.0))
        .otherwise(col("newbalanceDest"))
    )
)


# ---------------------------------------------------------
# 8. Create balance change metrics
# ---------------------------------------------------------

df = (
    df
    .withColumn(
        "origin_balance_change",
        round(
            col("oldbalanceOrg") - col("newbalanceOrig"),
            2
        )
    )
    .withColumn(
        "destination_balance_change",
        round(
            col("newbalanceDest") - col("oldbalanceDest"),
            2
        )
    )
)


# ---------------------------------------------------------
# 9. Create transaction risk indicators
# ---------------------------------------------------------

df = (
    df
    .withColumn(
        "high_value_flag",
        when(col("amount") >= 200000, 1).otherwise(0)
    )
  .withColumn(
    "balance_mismatch_flag",
    when(
        col("type").isin("CASH_OUT", "TRANSFER")
        & (spark_abs(col("origin_balance_change") - col("amount")) > 0.01),
        1
    ).otherwise(0)
)
)


# ---------------------------------------------------------
# 10. Create risk score
# ---------------------------------------------------------

df = (
    df
    .withColumn(
        "risk_score",
        (
            when(col("type") == "TRANSFER", 40)
            .when(col("type") == "CASH_OUT", 25)
            .otherwise(0)
            + col("high_value_flag") * 25
            + col("isFlaggedFraud") * 50
        )
    )
    .withColumn(
        "risk_level",
        when(col("risk_score") >= 70, "HIGH")
        .when(col("risk_score") >= 40, "MEDIUM")
        .otherwise("LOW")
    )
)


# ---------------------------------------------------------
# 11. Show transformed data
# ---------------------------------------------------------

print("\n===== CURATED TRANSACTIONS =====")

df.select(
    "step",
    "type",
    "amount",
    "isFraud",
    "isFlaggedFraud",
    "high_value_flag",
    "balance_mismatch_flag",
    "risk_score",
    "risk_level"
).show(10, truncate=False)


# ---------------------------------------------------------
# 12. Fraud summary
# ---------------------------------------------------------

print("\n===== FRAUD SUMMARY =====")

df.groupBy("risk_level").count().orderBy("risk_level").show()


# ---------------------------------------------------------
# 13. Write curated dataset
# ---------------------------------------------------------

print("\n===== WRITING CURATED DATA =====")

(
    df
    .coalesce(2)
    .write
    .mode("overwrite")
    .parquet(output_path)
)

print(f"Curated data written to: {output_path}")


# ---------------------------------------------------------
# 14. Stop Spark
# ---------------------------------------------------------

spark.stop()

print("\n===== TRANSFORMATION COMPLETED SUCCESSFULLY =====")