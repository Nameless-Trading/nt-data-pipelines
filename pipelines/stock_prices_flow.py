from clients import get_alpaca_client, get_clickhouse_client
from alpaca.data.requests import StockBarsRequest
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
import datetime as dt
import polars as pl
from prefect import flow, task
from zoneinfo import ZoneInfo


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
    alpaca_client = get_alpaca_client()

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
        pl.col("timestamp")
        .dt.replace_time_zone("UTC")
        .dt.convert_time_zone("America/Denver")
        .dt.date()
        .cast(pl.String)
        .alias("date"),
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
        year_start = max(dt.datetime(year, 1, 1, tzinfo=start.tzinfo), start)
        year_end = min(dt.datetime(year, 12, 31, tzinfo=end.tzinfo), end)

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
    start = dt.datetime(2017, 1, 1, tzinfo=ZoneInfo("America/Denver"))
    end = dt.datetime.today().replace(tzinfo=ZoneInfo("America/Denver")) - dt.timedelta(
        days=1
    )

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
    yesterday = (
        dt.datetime.now(ZoneInfo("America/Denver"))
        - dt.timedelta(days=1)
    ).date()

    print("Last Market Date:", last_market_date)
    print("Yesterday:", yesterday)

    # Only get new data if yesterday was the last market date
    if last_market_date != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", last_market_date)
        print("Yesterday:", yesterday)
        return

    start = dt.datetime.combine(yesterday, dt.time(0, 0, 0)).replace(
        tzinfo=ZoneInfo("America/Denver")
    )
    end = dt.datetime.combine(yesterday, dt.time(23, 59, 59)).replace(
        tzinfo=ZoneInfo("America/Denver")
    )

    tickers = get_tickers()
    stock_prices_df = get_stock_prices_batches(tickers, start, end)

    upload_and_merge_stock_prices_df(stock_prices_df)
