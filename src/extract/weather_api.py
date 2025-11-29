import requests
import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import time

load_dotenv()
logger = logging.getLogger(__name__)


class SourceConfig():
    def __init__(self):
        self.user = os.getenv('WEATHER_DB_USER')
        self.password = os.getenv('WEATHER_DB_PASSWORD')
        self.host = os.getenv('WEATHER_DB_HOST')
        self.port = os.getenv('WEATHER_DB_PORT')
        self.db = os.getenv('WEATHER_DB_NAME')

    def get_connection_string(self):    
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class ApiPuller():
    def __init__(self, api_url: str, params: dict = None):
        self.api_url = api_url
        self.params = params

    def fetch_data(self) -> dict:
        headers = {
            "Accept": "application/json",
            "User-Agent": "weather-air-quality-pipeline/1.0"
        }

        logger.info("Fetching weather data...")

        max_retries = 5
        base_sleep = 2
        max_sleep = 60
        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(self.api_url, headers=headers, params=self.params)
                response.raise_for_status()
                return response.json()
        

            except Exception as e:
                logger.error(f"API error: {e}")
                if attempt == max_retries:
                    logger.error("Max retries reached. Exiting.")
                    break
                sleep_time = min(base_sleep * (2 ** (attempt - 1)), max_sleep)
                logger.info(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)

        return None


def extract_weather():
    api_url = os.getenv('API_URL')

    params = {
        "latitude": 12.9716,
        "longitude": 77.5946,
        "hourly": "temperature_2m,relativehumidity_2m"
    }

    api = ApiPuller(api_url, params)
    data = api.fetch_data()

    if not data:
        raise Exception("API returned no data")

    hourly = data["hourly"]

    df = pd.DataFrame({
        "time": hourly["time"],
        "temperature_2m": hourly["temperature_2m"],
        "relativehumidity_2m": hourly["relativehumidity_2m"]
    })

    
    df["time"] = pd.to_datetime(df["time"], utc=True)

    df = df[df["time"] <= pd.Timestamp.utcnow()]

    engine = create_engine(SourceConfig().get_connection_string())
    df.to_sql("weather_hourly_raw", engine, schema="bronze", if_exists="append", index=False)

    logger.info("Weather data written to Postgres successfully")