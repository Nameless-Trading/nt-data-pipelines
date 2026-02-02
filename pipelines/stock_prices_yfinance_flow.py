import datetime as dt

import bear_lake as bl
import polars as pl
import yfinance as yf
from clients import get_bear_lake_client
from prefect import flow, get_run_logger, task
from utils import get_last_market_date
from variables import TIME_ZONE


def to_yfinance_ticker(ticker: str) -> str:
    """Convert database ticker to yfinance format (BF.B -> BF-B)"""
    return ticker.replace(".", "-")


def to_db_ticker(yf_ticker: str) -> str:
    """Convert yfinance ticker back to database format (BF-B -> BF.B)"""
    return yf_ticker.replace("-", ".")


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
def get_tickers_with_date_range() -> pl.DataFrame:
    """Get tickers with their date ranges from universe."""
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("universe")
        .group_by("ticker")
        .agg(
            pl.col("date").min().alias("min_date"),
            pl.col("date").max().alias("max_date"),
        )
    )


@task
def get_stock_prices_yfinance(
    tickers_df: pl.DataFrame, start: dt.datetime, end: dt.datetime
) -> pl.DataFrame:
    """
    Fetch historical stock prices from yfinance.
    Uses date ranges from universe to skip delisted tickers and cap end dates.
    """
    logger = get_run_logger()

    if tickers_df.is_empty():
        return pl.DataFrame(schema=_empty_schema())

    # Convert to naive datetime (yfinance expects dates without timezone)
    start_naive = start.replace(tzinfo=None)
    end_naive = end.replace(tzinfo=None)

    all_data = []
    skipped = 0
    fetched = 0

    for row in tickers_df.iter_rows(named=True):
        ticker = row["ticker"]
        max_date = row["max_date"]

        # Skip if we're requesting data after this ticker was delisted
        if start.date() > max_date:
            skipped += 1
            continue

        # Cap the end date to when the ticker was last active
        effective_end_date = min(end.date(), max_date)
        effective_end_naive = dt.datetime.combine(
            effective_end_date, dt.time(23, 59, 59)
        )

        try:
            # Download single ticker (convert to yfinance format for API call)
            yf_ticker = to_yfinance_ticker(ticker)
            data = yf.download(
                tickers=yf_ticker,
                start=start_naive,
                end=effective_end_naive,
                progress=False,
                auto_adjust=True,
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
            fetched += 1

        except Exception as e:
            logger.warning(f"Failed to fetch {ticker}: {e}")
            continue

    logger.info(f"Fetched {fetched} tickers, skipped {skipped} delisted")

    if not all_data:
        return pl.DataFrame(schema=_empty_schema())

    return pl.concat(all_data).sort("date", "ticker")


@task
def get_stock_prices_yfinance_batch(
    tickers_df: pl.DataFrame, start: dt.datetime, end: dt.datetime, batch_size: int = 50
) -> pl.DataFrame:
    """
    Fetch stock prices using yf.download() with batched tickers.
    Uses date ranges from universe to filter and cap dates appropriately.
    """
    logger = get_run_logger()

    if tickers_df.is_empty():
        return pl.DataFrame(schema=_empty_schema())

    start_naive = start.replace(tzinfo=None)
    end_naive = end.replace(tzinfo=None)

    # Filter to only tickers active during this period
    active_tickers_df = tickers_df.filter(pl.col("max_date") >= start.date())
    
    if active_tickers_df.is_empty():
        logger.info(f"No active tickers for period {start.date()} to {end.date()}")
        return pl.DataFrame(schema=_empty_schema())

    # Build a lookup for max_date by ticker
    max_date_lookup = {
        row["ticker"]: row["max_date"] 
        for row in active_tickers_df.iter_rows(named=True)
    }
    
    tickers = list(max_date_lookup.keys())
    logger.info(f"Fetching {len(tickers)} active tickers (filtered from {len(tickers_df)})")

    all_data = []

    # Process in batches
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        # Convert to yfinance format for API call, keep mapping to restore later
        yf_batch = [to_yfinance_ticker(t) for t in batch]
        yf_to_db = {to_yfinance_ticker(t): t for t in batch}
        logger.info(f"Fetching batch {i // batch_size + 1}: {len(batch)} tickers")

        try:
            # Download batch of tickers (using yfinance format)
            data = yf.download(
                tickers=yf_batch,
                start=start_naive,
                end=end_naive,
                progress=False,
                auto_adjust=True,
                group_by="ticker",
            )

            if data.empty:
                continue

            # Handle single ticker case (no MultiIndex)
            if len(batch) == 1:
                db_ticker = batch[0]  # Original db format
                max_date = max_date_lookup[db_ticker]
                data = data.reset_index()
                
                ticker_df = pl.from_pandas(data).select(
                    pl.lit(db_ticker).alias("ticker"),
                    pl.col("Date").dt.date().alias("date"),
                    pl.col("Open").alias("open"),
                    pl.col("High").alias("high"),
                    pl.col("Low").alias("low"),
                    pl.col("Close").alias("close"),
                    pl.col("Volume").cast(pl.Float64).alias("volume"),
                )
                
                # Filter to only dates up to max_date
                ticker_df = ticker_df.filter(pl.col("date") <= max_date)
                
                if not ticker_df.is_empty():
                    all_data.append(ticker_df)
                continue

            # Handle multiple tickers (MultiIndex columns grouped by ticker)
            for yf_ticker in yf_batch:
                db_ticker = yf_to_db[yf_ticker]  # Map back to db format
                try:
                    if yf_ticker not in data.columns.get_level_values(0):
                        continue

                    max_date = max_date_lookup[db_ticker]
                    ticker_data = data[yf_ticker].copy()
                    ticker_data = ticker_data.reset_index()

                    if ticker_data.empty or ticker_data["Close"].isna().all():
                        continue

                    ticker_df = pl.from_pandas(ticker_data).select(
                        pl.lit(db_ticker).alias("ticker"),  # Use db format for storage
                        pl.col("Date").dt.date().alias("date"),
                        pl.col("Open").alias("open"),
                        pl.col("High").alias("high"),
                        pl.col("Low").alias("low"),
                        pl.col("Close").alias("close"),
                        pl.col("Volume").cast(pl.Float64).alias("volume"),
                    )

                    # Filter to only dates up to max_date and drop nulls
                    ticker_df = ticker_df.filter(pl.col("date") <= max_date).drop_nulls(
                        subset=["close"]
                    )

                    if not ticker_df.is_empty():
                        all_data.append(ticker_df)

                except Exception as e:
                    logger.warning(f"Failed to process {db_ticker} in batch: {e}")
                    continue

        except Exception as e:
            logger.error(f"Failed to fetch batch: {e}")
            continue

    if not all_data:
        return pl.DataFrame(schema=_empty_schema())

    return pl.concat(all_data).sort("date", "ticker")


@task
def get_stock_prices_yfinance_by_year(
    tickers_df: pl.DataFrame, start: dt.datetime, end: dt.datetime
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

        stock_prices = get_stock_prices_yfinance_batch(tickers_df, year_start, year_end)

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
    Uses universe date ranges to avoid fetching data for delisted tickers.
    """
    start = dt.datetime(2000, 1, 1, tzinfo=TIME_ZONE)
    end = dt.datetime.now(TIME_ZONE) - dt.timedelta(days=1)

    tickers_df = get_tickers_with_date_range()
    stock_prices_df = get_stock_prices_yfinance_by_year(tickers_df, start, end)
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

    tickers_df = get_tickers_with_date_range()
    stock_prices_df = get_stock_prices_yfinance_by_year(tickers_df, start, end)
    upload_and_merge_stock_prices_yfinance_df(stock_prices_df)

if __name__ == '__main__':
    from rich import print
    bear_lake_client = get_bear_lake_client()
    bear_lake_client.drop('stock_prices_yfinance')

    stock_prices_yfinance_backfill_flow()

    print(bear_lake_client.query(
        bl.table('stock_prices_yfinance')
    ))