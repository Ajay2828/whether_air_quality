from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging



import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
from src.extract.weather_api import ApiPuller

logger = logging.getLogger(__name__)

def extract_weather(**context):
    api_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 12.9716,
        "longitude": 77.5946,
        "hourly": "temperature_2m,relativehumidity_2m"
    }

    api = ApiPuller(api_url=api_url, params=params)
    data = api._fetch_data(params)

    logger.info(data)

with DAG(
    dag_id='weather_data_pipeline',
    start_date=datetime(2023, 1, 1),
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