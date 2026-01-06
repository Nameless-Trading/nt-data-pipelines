from clients import get_alpaca_historical_stock_data_client, get_bear_lake_client
from alpaca.data.requests import StockBarsRequest
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
import datetime as dt
import polars as pl
from prefect import flow, task
from variables import TIME_ZONE
from utils import get_last_market_date
import bear_lake as bl


@task
def get_tickers() -> list[str]:
    bear_lake_client = get_bear_lake_client()
    return (
        bear_lake_client.query(bl.table("universe").select("ticker").unique())["ticker"]
        .sort()
        .to_list()
    )


@task
def get_stock_prices(
    tickers: list[str], start: dt.datetime, end: dt.datetime
) -> pl.DataFrame:
    alpaca_client = get_alpaca_historical_stock_data_client()

    stock_bars_request = StockBarsRequest(
        symbol_or_symbols=tickers,
        start=start,
        end=end,
        timeframe=TimeFrame(1, TimeFrameUnit.Day),
        adjustment=Adjustment.ALL,
        feed=DataFeed.IEX,
    )

    stock_prices_raw = alpaca_client.get_stock_bars(stock_bars_request)

    if not len(stock_prices_raw.df) > 0:
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "date": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "trade_count": pl.Float64,
                "vwap": pl.Float64,
            }
        )

    stock_prices_clean = pl.from_pandas(stock_prices_raw.df.reset_index()).select(
        pl.col("symbol").alias("ticker"),
        pl.col("timestamp").dt.date().alias("date"),
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
    )

    return stock_prices_clean


@task
def get_stock_prices_batches(
    tickers: list[str], start: dt.datetime, end: dt.datetime
) -> pl.DataFrame:
    years = range(start.year, end.year + 1)
    stock_prices_list = []
    for year in years:
        year_start = max(dt.datetime(year, 1, 1, 0, 0, 0, tzinfo=TIME_ZONE), start)
        year_end = min(dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=TIME_ZONE), end)

        stock_prices = get_stock_prices(tickers, year_start, year_end)

        stock_prices_list.append(stock_prices)

    return (
        pl.concat(stock_prices_list)
        .with_columns(pl.col("date").dt.year().alias("year"))
        .sort("date", "ticker")
    )


@task
def upload_and_merge_stock_prices_df(stock_prices_df: pl.DataFrame):
    bear_lake_client = get_bear_lake_client()
    table_name = "stock_prices"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "trade_count": pl.Float64,
            "vwap": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["date", "ticker"],
        mode="skip",
    )

    # Insert into table
    bear_lake_client.insert(name=table_name, data=stock_prices_df, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


@flow
def stock_prices_backfill_flow():
    start = dt.datetime(2017, 1, 1, tzinfo=TIME_ZONE)
    end = dt.datetime.today().replace(tzinfo=TIME_ZONE) - dt.timedelta(days=1)

    tickers = get_tickers()
    stock_prices_df = get_stock_prices_batches(tickers, start, end)
    upload_and_merge_stock_prices_df(stock_prices_df)


@flow
def stock_prices_daily_flow():
    last_market_date = get_last_market_date()
    yesterday = (dt.datetime.now(TIME_ZONE) - dt.timedelta(days=1)).date()

    # Only get new data if yesterday was the last market date
    if last_market_date != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", last_market_date)
        print("Yesterday:", yesterday)
        return

    start = dt.datetime.combine(yesterday, dt.time(0, 0, 0)).replace(tzinfo=TIME_ZONE)
    end = dt.datetime.combine(yesterday, dt.time(23, 59, 59)).replace(tzinfo=TIME_ZONE)

    tickers = get_tickers()
    stock_prices_df = get_stock_prices_batches(tickers, start, end)
    upload_and_merge_stock_prices_df(stock_prices_df)
