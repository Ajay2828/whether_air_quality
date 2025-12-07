import logging
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta
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
    
class Tranformer():
    def tranformation_silver(self):
        engine = create_engine(SourceConfig().get_connection_string())

        yesterday = datetime.utcnow() - timedelta(days=1)
        sql_query = f"SELECT * FROM bronze.weather_hourly_raw WHERE time > '{yesterday}';"
        df = pd.read_sql_query(sql_query, con=engine)

        if df.empty:
            logger.warning("No data found in the weather_hourly_raw table.")
            return
        
        sql_query = f"SELECT * FROM bronze.air_quality_hourly_raw WHERE time > '{yesterday}';"
        df1 = pd.read_sql_query(sql_query, con=engine)

        if df1.empty:
            logger.warning("No data found in the air_quality_hourly_raw table.")
            return

        df_unique = df.drop_duplicates()
        df_unique1 = df1.drop_duplicates()

        merged_df = pd.concat([df_unique, df_unique1], axis=1)

        merged_df.to_sql('weather_air_quality_hourly', con=engine, schema='silver', if_exists='append', index=False)
        logger.info("Data transformation and loading to silver.weather_air_quality_hourly completed successfully.")

    def tranformation_gold(self):
        engine = create_engine(SourceConfig().get_connection_string())
        
        yesterday = datetime.utcnow() - timedelta(days=1)
        sql_query = f"SELECT * FROM silver.weather_air_quality_hourly  WHERE time > '{yesterday}';"
        df = pd.read_sql_query(sql_query, con=engine)

        if df.empty:
            logger.warning("No data found in the silver.weather_air_quality_hourly table.")
            return

        df['time'] = pd.to_datetime(df['time'])
        df['date'] = df['time'].dt.date
        df['hour'] = df['time'].dt.hour

        gold_df = df.groupby(['date', 'hour']).agg({
            'temperature': 'mean',
            'humidity': 'mean',
            'pressure': 'mean',
            'air_quality_index': 'mean'
        }).reset_index()

        gold_df.to_sql('weather_air_quality_daily', con=engine, schema='gold', if_exists='append', index=False)
        logger.info("Data transformation and loading to gold.weather_air_quality_daily completed successfully.")

