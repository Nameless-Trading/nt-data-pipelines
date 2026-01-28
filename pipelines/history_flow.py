import datetime as dt
from zoneinfo import ZoneInfo

import bear_lake as bl
import polars as pl
from alpaca.data.enums import Adjustment
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from clients import get_alpaca_historical_stock_data_client, get_bear_lake_client
from prefect import flow, task
from rich import print
from utils import get_last_market_date
from variables import FACTORS, TIME_ZONE


@task
def get_tickers() -> list[str]:
    bear_lake_client = get_bear_lake_client()
    return (
        bear_lake_client.query(bl.table("universe").select("ticker").unique())["ticker"]
        .sort()
        .to_list()
    )


@task
def get_history_by_date(tickers: list[str], date_: dt.date) -> pl.DataFrame:
    ext_open = dt.time(4, 0, 0, tzinfo=ZoneInfo("America/New_York"))
    ext_close = dt.time(20, 0, 0, tzinfo=ZoneInfo("America/New_York"))

    start = dt.datetime.combine(date_, ext_open)
    end = dt.datetime.combine(date_, ext_close)

    alpaca_client = get_alpaca_historical_stock_data_client()

    request = StockBarsRequest(
        symbol_or_symbols=tickers,
        start=start,
        end=end,
        timeframe=TimeFrame(amount=1, unit=TimeFrameUnit.Minute),
        adjustment=Adjustment.ALL,
    )

    stock_bars = alpaca_client.get_stock_bars(request)

    return pl.from_pandas(stock_bars.df.reset_index()).rename({"symbol": "ticker"})


@task
def get_market_dates(start: dt.date, end: dt.date) -> list[dt.date]:
    bear_lake_client = get_bear_lake_client()

    return (
        bear_lake_client.query(
            bl.table("calendar").filter(pl.col("date").is_between(start, end))
        )["date"]
        .sort()
        .to_list()
    )


@task
def get_history(tickers: list[str], start: dt.date, end: dt.date):
    market_dates = get_market_dates(start, end)

    history_list = []
    for market_date in market_dates:
        history_list.append(get_history_by_date(tickers, market_date))

    return pl.concat(history_list)


@task
def upload_and_merge_history(history: pl.DataFrame, table_name: str):
    bear_lake_client = get_bear_lake_client()

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "timestamp": pl.Datetime,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "vwap": pl.Float64,
            "trade_count": pl.Float64,
        },
        partition_keys=None,
        primary_keys=["timestamp", "ticker"],
        mode="skip",
    )

    # Insert into table
    bear_lake_client.insert(name=table_name, data=history, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


@flow
def etf_history_backfill_flow():
    start = dt.date(2026, 1, 2)
    end = (dt.datetime.now(TIME_ZONE) - dt.timedelta(days=1)).date()
    tickers = FACTORS

    etf_history = get_history(tickers, start, end)

    upload_and_merge_history(etf_history, "etf_history")


@flow
def etf_history_daily_flow():
    last_market_date = get_last_market_date()
    yesterday = (dt.datetime.now(TIME_ZONE) - dt.timedelta(days=1)).date()
    tickers = FACTORS

    # Only get new data if yesterday was the last market date
    if last_market_date != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", last_market_date)
        print("Yesterday:", yesterday)
        return

    etf_history = get_history(tickers, last_market_date, last_market_date)

    upload_and_merge_history(etf_history, "etf_history")


@flow
def stock_history_backfill_flow():
    start = dt.date(2026, 1, 2)
    end = (dt.datetime.now(TIME_ZONE) - dt.timedelta(days=1)).date()
    tickers = get_tickers()

    stock_history = get_history(tickers, start, end)

    upload_and_merge_history(stock_history, "stock_history")


@flow
def stock_history_daily_flow():
    last_market_date = get_last_market_date()
    yesterday = (dt.datetime.now(TIME_ZONE) - dt.timedelta(days=1)).date()
    tickers = get_tickers()

    # Only get new data if yesterday was the last market date
    if last_market_date != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", last_market_date)
        print("Yesterday:", yesterday)
        return

    stock_history = get_history(tickers, last_market_date, last_market_date)

    upload_and_merge_history(stock_history, "stock_history")
