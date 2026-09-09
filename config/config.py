import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": "192.168.128.1",
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}
S3_BUCKET = "banking-fraud-pipeline-girija-2026-473640421842-us-east-1-an"
S3_RAW_KEY = "raw/PS_20174392719_1491204439457_log.csv.zip"
