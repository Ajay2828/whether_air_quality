import requests
import logging

logger = logging.getLogger(__name__)

class ApiPuller():
    def __init__(self, api_url: str, params: dict = None):
        self.api_url = api_url
        self.params = params

    def _fetch_data(self, params: dict) -> dict:
        headers = {
            "Accept": "application/json",
            "User-Agent": "weather-air-quality-pipeline/1.0"
        }

        logger.info(f"Fetching data...")

        try:
            response = requests.get(self.api_url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectTimeout:
            logger.error("Connection timed out error while connecting to the API.")

        except requests.exceptions.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")

        except requests.exceptions.RequestException as req_err:
            logger.error(f"Request exception occurred: {req_err}")

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")           
