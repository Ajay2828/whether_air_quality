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
    def tranformation(self):
        engine = create_engine(SourceConfig().get_connection_string())

        
        yesterday = datetime.utcnow() - timedelta(days=1)
        datetime_str = yesterday.strftime('%Y-%m-%d %H:%M:%S')
        sql_query = f"SELECT * FROM bronze.weather_hourly_raw WHERE time > '{datetime_str}';"
        df = pd.read_sql_query(sql_query, con=engine)

        if df.empty:
            logger.warning("No data found in the weather_hourly_raw table.")
            return
        
        sql_query = f"SELECT * FROM bronze.air_quality_hourly_raw WHERE time > '{datetime_str}';"
        df1 = pd.read_sql_query(sql_query, con=engine)

        if df1.empty:
            logger.warning("No data found in the air_quality_hourly_raw table.")
            return

        logger.info(df.head())
        logger.info(df1.head())

# def tranform_init():
#     transformer = Tranformer()
#     transformer.tranformation()
#     logger.info("Data transformation completed successfully.")