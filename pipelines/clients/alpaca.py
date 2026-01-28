import datetime as dt
import os

from alpaca.data import StockHistoricalDataClient
from alpaca.trading import GetOrdersRequest, TradingClient
from alpaca.trading.enums import QueryOrderStatus
from dotenv import load_dotenv

load_dotenv(override=True)


def get_alpaca_historical_stock_data_client():
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


def get_alpaca_trading_client():
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    paper = os.getenv("ALPACA_PAPER")

    if not (api_key and secret_key):
        raise RuntimeError(
            f"""
            Environment variables not set:
                ALPACA_API_KEY: {api_key}
                ALPACA_SECRET_KEY: {secret_key}
            """
        )
    return TradingClient(api_key, secret_key, paper=paper)
