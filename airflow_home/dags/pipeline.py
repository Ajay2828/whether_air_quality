from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys
import os
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
from src.extract.weather_api import extract_weather

with DAG(
    dag_id='weather_data_pipeline',
    start_date=datetime(2024, 11, 27),
    schedule_interval='30 6 * * *',
    catchup=False,
    tags=['weather', 'air_quality'],
) as dag:
    start_task = BashOperator(
        task_id='start_pipeline',
        bash_command='echo "Starting the pipeline!"',
    )

    weather_data_pull_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather,
        provide_context=True,
        
    )

    end_task = BashOperator(
        task_id='end_pipeline',
        bash_command='echo "Pipeline finished successfully!"',
    )

    start_task >> weather_data_pull_task >> end_task