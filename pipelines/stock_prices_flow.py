from clients import get_alpaca_historical_stock_data_client, get_clickhouse_client
from alpaca.data.requests import StockBarsRequest
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
import datetime as dt
import polars as pl
from prefect import flow, task
from variables import TIME_ZONE
import yfinance as yf


@task
def get_tickers() -> list[str]:
    clickhouse_client = get_clickhouse_client()

    universe_arrow = clickhouse_client.query_arrow(
        f"SELECT DISTINCT ticker FROM universe"
    )

    return pl.from_arrow(universe_arrow)["ticker"].sort().to_list()


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
                "date": pl.String,
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
        pl.col("timestamp").dt.date().cast(pl.String).alias("date"),
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

    return pl.concat(stock_prices_list).sort("date", "ticker")


@task
def upload_and_merge_stock_prices_df(stock_prices_df: pl.DataFrame):
    clickhouse_client = get_clickhouse_client()
    table_name = "stock_prices"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ticker String,
            date String,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume Float64,
            trade_count Float64,
            vwap Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, date)
        """
    )

    # Insert into table
    clickhouse_client.insert_df_arrow(table_name, stock_prices_df)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


@flow
def stock_prices_backfill_flow():
    start = dt.datetime(2017, 1, 1, tzinfo=TIME_ZONE)
    end = dt.datetime.today().replace(tzinfo=TIME_ZONE) - dt.timedelta(days=1)

    tickers = get_tickers()
    stock_prices_df = get_stock_prices_batches(tickers, start, end)
    upload_and_merge_stock_prices_df(stock_prices_df)


@task
def get_last_market_date() -> dt.date:
    clickhouse_client = get_clickhouse_client()
    last_market_date = clickhouse_client.query("SELECT MAX(date) FROM calendar")
    return dt.datetime.strptime(last_market_date.result_rows[0][0], "%Y-%m-%d").date()


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


@task
def get_stock_prices_yfinance(
    tickers: list[str], start: dt.datetime, end: dt.datetime
) -> pl.DataFrame:
    """
    Fetch historical stock prices from yfinance.
    Returns data in the same schema as Alpaca, with vwap and trade_count as null.
    """
    stock_prices_list = []

    for ticker in tickers:
        try:
            # Download historical data from yfinance
            # yfinance expects dates without timezone, so convert to naive datetime
            start_naive = start.replace(tzinfo=None)
            end_naive = end.replace(tzinfo=None)

            ticker_obj = yf.Ticker(ticker)
            hist = ticker_obj.history(start=start_naive, end=end_naive, interval="1d")

            if hist.empty:
                continue

            # Convert to polars DataFrame and format to match schema
            # yfinance returns Date as index, reset_index() makes it a column
            hist_reset = hist.reset_index()
            hist_df = pl.from_pandas(hist_reset)

            # Handle date column - yfinance may return it as "Date" or "Datetime"
            date_col = None
            for col in ["Date", "Datetime"]:
                if col in hist_df.columns:
                    date_col = col
                    break

            if date_col is None:
                continue

            # Rename columns to match schema
            stock_prices_clean = hist_df.select(
                pl.lit(ticker).alias("ticker"),
                pl.col(date_col).dt.date().cast(pl.String).alias("date"),
                pl.col("Open").alias("open"),
                pl.col("High").alias("high"),
                pl.col("Low").alias("low"),
                pl.col("Close").alias("close"),
                pl.col("Volume").cast(pl.Float64).alias("volume"),
                pl.lit(None, dtype=pl.Float64).alias("trade_count"),
                pl.lit(None, dtype=pl.Float64).alias("vwap"),
            )

            stock_prices_list.append(stock_prices_clean)

        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            continue

    if not stock_prices_list:
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "date": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "trade_count": pl.Float64,
                "vwap": pl.Float64,
            }
        )

    return pl.concat(stock_prices_list).sort("date", "ticker")


@task
def get_stock_prices_yfinance_batches(
    tickers: list[str], start: dt.datetime, end: dt.datetime
) -> pl.DataFrame:
    """
    Fetch stock prices in batches by year to avoid overwhelming yfinance API.
    """
    years = range(start.year, end.year + 1)
    stock_prices_list = []

    for year in years:
        year_start = max(dt.datetime(year, 1, 1, 0, 0, 0, tzinfo=TIME_ZONE), start)
        year_end = min(dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=TIME_ZONE), end)

        stock_prices = get_stock_prices_yfinance(tickers, year_start, year_end)

        if not stock_prices.is_empty():
            stock_prices_list.append(stock_prices)

    if not stock_prices_list:
        return pl.DataFrame(
            schema={
                "ticker": pl.String,
                "date": pl.String,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "trade_count": pl.Float64,
                "vwap": pl.Float64,
            }
        )

    return pl.concat(stock_prices_list).sort("date", "ticker")


@flow
def stock_prices_yfinance_backfill_flow():
    """
    Backfill historical stock prices from yfinance for older history (1980-2016).
    This complements the Alpaca data which starts from 2017.
    """
    # Start from 1980 or as far back as possible, end at 2016 (before Alpaca data starts)
    start = dt.datetime(1980, 1, 1, tzinfo=TIME_ZONE)
    end = dt.datetime(2016, 12, 31, 23, 59, 59, tzinfo=TIME_ZONE)

    tickers = get_tickers()
    stock_prices_df = get_stock_prices_yfinance_batches(tickers, start, end)
    """
    # upload_and_merge_stock_prices_df(stock_prices_df)
    or (more likely)
    create a separate table for yfinance data
    """
