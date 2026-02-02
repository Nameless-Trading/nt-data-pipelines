import datetime as dt

import bear_lake as bl
import polars as pl
import yfinance as yf
from clients import get_bear_lake_client
from prefect import flow, get_run_logger, task
from utils import get_last_market_date
from variables import TIME_ZONE


@task
def get_tickers() -> list[str]:
    bear_lake_client = get_bear_lake_client()
    return (
        bear_lake_client.query(bl.table("universe").select("ticker").unique())["ticker"]
        .sort()
        .to_list()
    )


def _empty_schema() -> dict:
    """Return the expected schema for an empty DataFrame."""
    return {
        "ticker": pl.String,
        "date": pl.Date,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Float64,
    }


@task
def get_stock_prices_yfinance(
    tickers: list[str], start: dt.datetime, end: dt.datetime
) -> pl.DataFrame:
    """
    Fetch historical stock prices from yfinance.
    Downloads each ticker individually to avoid MultiIndex complexity.
    """
    logger = get_run_logger()

    if not tickers:
        return pl.DataFrame(schema=_empty_schema())

    # Convert to naive datetime (yfinance expects dates without timezone)
    start_naive = start.replace(tzinfo=None)
    end_naive = end.replace(tzinfo=None)

    all_data = []

    for ticker in tickers:
        try:
            # Download single ticker - returns simple DataFrame
            data = yf.download(
                tickers=ticker,
                start=start_naive,
                end=end_naive,
                progress=False,
                auto_adjust=True,  # Use adjusted prices
            )

            if data.empty:
                continue

            # Reset index to get Date as a column
            data = data.reset_index()

            # Convert to polars and add ticker column
            ticker_df = pl.from_pandas(data).select(
                pl.lit(ticker).alias("ticker"),
                pl.col("Date").dt.date().alias("date"),
                pl.col("Open").alias("open"),
                pl.col("High").alias("high"),
                pl.col("Low").alias("low"),
                pl.col("Close").alias("close"),
                pl.col("Volume").cast(pl.Float64).alias("volume"),
            )

            all_data.append(ticker_df)

        except Exception as e:
            logger.warning(f"Failed to fetch {ticker}: {e}")
            continue

    if not all_data:
        return pl.DataFrame(schema=_empty_schema())

    return pl.concat(all_data).sort("date", "ticker")


@task
def get_stock_prices_yfinance_batch(
    tickers: list[str], start: dt.datetime, end: dt.datetime, batch_size: int = 50
) -> pl.DataFrame:
    """
    Fetch stock prices using yf.download() with batched tickers.
    More efficient than individual downloads but handles MultiIndex properly.
    """
    logger = get_run_logger()

    if not tickers:
        return pl.DataFrame(schema=_empty_schema())

    start_naive = start.replace(tzinfo=None)
    end_naive = end.replace(tzinfo=None)

    all_data = []

    # Process in batches to avoid overwhelming the API
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        logger.info(f"Fetching batch {i // batch_size + 1}: {len(batch)} tickers")

        try:
            # Download batch of tickers
            data = yf.download(
                tickers=batch,
                start=start_naive,
                end=end_naive,
                progress=False,
                auto_adjust=True,
                group_by="ticker",  # Group by ticker for easier processing
            )

            if data.empty:
                continue

            # Handle single ticker case (no MultiIndex)
            if len(batch) == 1:
                ticker = batch[0]
                data = data.reset_index()
                ticker_df = pl.from_pandas(data).select(
                    pl.lit(ticker).alias("ticker"),
                    pl.col("Date").dt.date().alias("date"),
                    pl.col("Open").alias("open"),
                    pl.col("High").alias("high"),
                    pl.col("Low").alias("low"),
                    pl.col("Close").alias("close"),
                    pl.col("Volume").cast(pl.Float64).alias("volume"),
                )
                all_data.append(ticker_df)
                continue

            # Handle multiple tickers (MultiIndex columns grouped by ticker)
            for ticker in batch:
                try:
                    if ticker not in data.columns.get_level_values(0):
                        continue

                    ticker_data = data[ticker].copy()
                    ticker_data = ticker_data.reset_index()

                    # Skip if no data for this ticker
                    if ticker_data.empty or ticker_data["Close"].isna().all():
                        continue

                    ticker_df = pl.from_pandas(ticker_data).select(
                        pl.lit(ticker).alias("ticker"),
                        pl.col("Date").dt.date().alias("date"),
                        pl.col("Open").alias("open"),
                        pl.col("High").alias("high"),
                        pl.col("Low").alias("low"),
                        pl.col("Close").alias("close"),
                        pl.col("Volume").cast(pl.Float64).alias("volume"),
                    )

                    # Drop rows with null prices
                    ticker_df = ticker_df.drop_nulls(subset=["close"])

                    if not ticker_df.is_empty():
                        all_data.append(ticker_df)

                except Exception as e:
                    logger.warning(f"Failed to process {ticker} in batch: {e}")
                    continue

        except Exception as e:
            logger.error(f"Failed to fetch batch: {e}")
            continue

    if not all_data:
        return pl.DataFrame(schema=_empty_schema())

    return pl.concat(all_data).sort("date", "ticker")


@task
def get_stock_prices_yfinance_by_year(
    tickers: list[str], start: dt.datetime, end: dt.datetime
) -> pl.DataFrame:
    """
    Fetch stock prices in batches by year.
    """
    logger = get_run_logger()
    years = range(start.year, end.year + 1)
    stock_prices_list = []

    for year in years:
        year_start = max(dt.datetime(year, 1, 1, 0, 0, 0, tzinfo=TIME_ZONE), start)
        year_end = min(dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=TIME_ZONE), end)

        logger.info(f"Fetching year {year}: {year_start.date()} to {year_end.date()}")

        stock_prices = get_stock_prices_yfinance_batch(tickers, year_start, year_end)

        if not stock_prices.is_empty():
            stock_prices_list.append(stock_prices)

    if not stock_prices_list:
        return pl.DataFrame(schema=_empty_schema())

    return (
        pl.concat(stock_prices_list)
        .unique(subset=["ticker", "date"])
        .with_columns(pl.col("date").dt.year().alias("year"))
        .sort("date", "ticker")
    )


@task
def upload_and_merge_stock_prices_yfinance_df(stock_prices_df: pl.DataFrame):
    bear_lake_client = get_bear_lake_client()
    table_name = "stock_prices_yfinance"

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
def stock_prices_yfinance_backfill_flow():
    """
    Grabbing yfinance data from 2000 to present day.
    """
    start = dt.datetime(2000, 1, 1, tzinfo=TIME_ZONE)
    end = dt.datetime.now(TIME_ZONE) - dt.timedelta(days=1)

    tickers = get_tickers()
    stock_prices_df = get_stock_prices_yfinance_by_year(tickers, start, end)
    upload_and_merge_stock_prices_yfinance_df(stock_prices_df)


@flow
def stock_prices_yfinance_daily_flow():
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
    stock_prices_df = get_stock_prices_yfinance_by_year(tickers, start, end)
    upload_and_merge_stock_prices_yfinance_df(stock_prices_df)
