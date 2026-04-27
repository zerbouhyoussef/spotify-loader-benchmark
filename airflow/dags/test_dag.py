from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'test_simple',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    task = BashOperator(
        task_id='hello',
        bash_command='echo "Hello World"'
    )
