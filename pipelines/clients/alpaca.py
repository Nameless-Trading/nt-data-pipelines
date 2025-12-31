from alpaca.data import StockHistoricalDataClient
import os
from dotenv import load_dotenv

load_dotenv()


def get_alpaca_client():
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    if not (api_key and secret_key):
        raise RuntimeError(
            f"""
            Environment variables not set:
                ALPACA_API_KEY: {api_key}
                ALPACA_SECRET_KEY: {secret_key}
            """
        )
    return StockHistoricalDataClient(api_key, secret_key)
