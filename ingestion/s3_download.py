import os
import boto3
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import S3_BUCKET, S3_RAW_KEY


LOCAL_PATH = "data/raw/PS_20174392719_1491204439457_log.csv.zip"


print("\n===== S3 DOWNLOAD STARTED =====")

os.makedirs("data/raw", exist_ok=True)

s3 = boto3.client("s3")

s3.download_file(
    S3_BUCKET,
    S3_RAW_KEY,
    LOCAL_PATH
)

print(f"Downloaded from S3:")
print(f"  s3://{S3_BUCKET}/{S3_RAW_KEY}")

print(f"\nSaved locally to:")
print(f"  {LOCAL_PATH}")

print("\n===== S3 DOWNLOAD COMPLETED SUCCESSFULLY =====")
