from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='simple_bash_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['example'],
) as dag:
    start_task = BashOperator(
        task_id='start_message',
        bash_command='echo "Starting the DAG!"',
    )

    print_date_task = BashOperator(
        task_id='print_current_date',
        bash_command='date',
    )

    end_task = BashOperator(
        task_id='end_message',
        bash_command='echo "DAG finished successfully!"',
    )

    start_task >> print_date_task >> end_task