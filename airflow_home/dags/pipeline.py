from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys
import os
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
from src.extract.weather_api import extract_weather
from src.extract.air_quality_api import extract_air_quality
from src.transform.transform import Tranformer

with DAG(
    dag_id='weather_air_data_pipeline',
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

    air_quality_data_pull_task = PythonOperator(
        task_id='extract_air_quality_data',
        python_callable=extract_air_quality,
        provide_context=True,
        
    )

    tranformation_task_silver = PythonOperator(
        task_id='tranform_data_silver',
        python_callable = Tranformer().tranformation_silver,
        provide_context=True,
    )

    tranformation_task_gold = PythonOperator(
        task_id='tranform_data_gold',
        python_callable = Tranformer().tranformation_gold,
        provide_context=True,
    )

    end_task = BashOperator(
        task_id='end_pipeline',
        bash_command='echo "Pipeline finished successfully!"',
    )

    start_task >> weather_data_pull_task >> air_quality_data_pull_task >> tranformation_task_silver >> tranformation_task_gold >> end_task