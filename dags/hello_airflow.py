from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello from your first Airflow DAG!")
    print("If you can see this in the task logs, Airflow is working correctly.")


with DAG(
    dag_id="hello_airflow",
    description="Sanity check DAG - Phase 1 of clickstream pipeline project",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manual trigger only, no auto-scheduling yet
    catchup=False,
    tags=["phase1", "sanity-check"],
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )
