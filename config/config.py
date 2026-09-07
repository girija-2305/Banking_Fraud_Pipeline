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
