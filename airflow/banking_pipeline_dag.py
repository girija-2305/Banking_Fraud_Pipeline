from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/mnt/f/Banking_Fraud_Pipeline"
PYTHON_EXE = "/home/girija/airflow_venv/bin/python"


with DAG(
    dag_id="banking_fraud_pipeline",
    description="End-to-end banking fraud data pipeline",
    start_date=datetime(2026, 9, 3),
    schedule=None,
    catchup=False,
    tags=["banking", "fraud", "spark", "postgresql"],
) as dag:

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command=f"""
cd {PROJECT_DIR}
{PYTHON_EXE} spark/transaction_transform.py
""",
    )

    validate_data = BashOperator(
        task_id="validate_data",
        bash_command=f"""
cd {PROJECT_DIR}
{PYTHON_EXE} quality/validation.py
""",
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"""
cd {PROJECT_DIR}
{PYTHON_EXE} load/load_to_postgres.py
""",
    )

    fraud_analysis = BashOperator(
        task_id="fraud_analysis",
        bash_command=f"""
cd {PROJECT_DIR}
{PYTHON_EXE} spark/fraud_detection.py
""",
    )

    transform_data >> validate_data >> load_to_postgres >> fraud_analysis