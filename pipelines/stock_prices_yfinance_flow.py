# from clients import get_clickhouse_client
# import datetime as dt
# import polars as pl
# from prefect import flow, task, get_run_logger
# from variables import TIME_ZONE
# import yfinance as yf
# from utils import get_last_market_date


# @task
# def get_tickers() -> list[str]:
#     clickhouse_client = get_clickhouse_client()

#     universe_arrow = clickhouse_client.query_arrow(
#         f"SELECT DISTINCT ticker FROM universe"
#     )

#     return pl.from_arrow(universe_arrow)["ticker"].sort().to_list()


# @task
# def get_stock_prices_yfinance(
#     tickers: list[str], start: dt.datetime, end: dt.datetime
# ) -> pl.DataFrame:
#     """
#     Fetch historical stock prices from yfinance.
#     Returns data in the same schema as Alpaca, with vwap and trade_count as null.
#     """
#     logger = get_run_logger()

#     try:
#         # yfinance expects dates without timezone, so convert to naive datetime
#         start_naive = start.replace(tzinfo=None)
#         end_naive = end.replace(tzinfo=None)

#         # Download data for all tickers at once
#         data_raw = yf.download(tickers=tickers, start=start_naive, end=end_naive)

#         if data_raw.empty:
#             return pl.DataFrame(
#                 schema={
#                     "ticker": pl.String,
#                     "date": pl.String,
#                     "open": pl.Float64,
#                     "high": pl.Float64,
#                     "low": pl.Float64,
#                     "close": pl.Float64,
#                     "volume": pl.Float64,
#                 }
#             )

#         # Stack and reset index to get long format
#         data_clean = data_raw.stack(future_stack=True).reset_index()

#         # Convert to polars
#         hist_df = pl.from_pandas(data_clean)

#         # Rename columns to match schema
#         stock_prices_clean = hist_df.select(
#             pl.col("Ticker").alias("ticker"),
#             pl.col("Date").dt.date().cast(pl.String).alias("date"),
#             pl.col("Open").alias("open"),
#             pl.col("High").alias("high"),
#             pl.col("Low").alias("low"),
#             pl.col("Close").alias("close"),
#             pl.col("Volume").cast(pl.Float64).alias("volume"),
#         )

#         return stock_prices_clean.sort("date", "ticker")

#     except Exception as e:
#         logger.error(f"Error fetching data: {e}")
#         return pl.DataFrame(
#             schema={
#                 "ticker": pl.String,
#                 "date": pl.String,
#                 "open": pl.Float64,
#                 "high": pl.Float64,
#                 "low": pl.Float64,
#                 "close": pl.Float64,
#                 "volume": pl.Float64,
#             }
#         )


# @task
# def get_stock_prices_yfinance_batches(
#     tickers: list[str], start: dt.datetime, end: dt.datetime
# ) -> pl.DataFrame:
#     """
#     Fetch stock prices in batches by year using yf.download().
#     """
#     years = range(start.year, end.year + 1)
#     stock_prices_list = []

#     for year in years:
#         year_start = max(dt.datetime(year, 1, 1, 0, 0, 0, tzinfo=TIME_ZONE), start)
#         year_end = min(dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=TIME_ZONE), end)

#         stock_prices = get_stock_prices_yfinance(tickers, year_start, year_end)

#         if not stock_prices.is_empty():
#             stock_prices_list.append(stock_prices)

#     if not stock_prices_list:
#         return pl.DataFrame(
#             schema={
#                 "ticker": pl.String,
#                 "date": pl.String,
#                 "open": pl.Float64,
#                 "high": pl.Float64,
#                 "low": pl.Float64,
#                 "close": pl.Float64,
#                 "volume": pl.Float64,
#             }
#         )

#     return pl.concat(stock_prices_list).sort("date", "ticker")


# # based on existing patterns in other jobs
# @task
# def upload_and_merge_stock_prices_yfinance_df(stock_prices_df: pl.DataFrame):
#     clickhouse_client = get_clickhouse_client()
#     table_name = "stock_prices_yfinance"

#     # Create table if not exists
#     clickhouse_client.command(
#         f"""
#         CREATE TABLE IF NOT EXISTS {table_name} (
#             ticker String,
#             date String,
#             open Float64,
#             high Float64,
#             low Float64,
#             close Float64,
#             volume Float64
#         )
#         ENGINE = ReplacingMergeTree()
#         ORDER BY (ticker, date)
#         """
#     )

#     # Insert into table
#     clickhouse_client.insert_df_arrow(table_name, stock_prices_df)

#     # Optimize table (deduplicate)
#     clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


# @flow
# def stock_prices_yfinance_backfill_flow():
#     """
#     Grabbing yfinance data from 2000 to present day.
#     """
#     start = dt.datetime(2000, 1, 1, tzinfo=TIME_ZONE)
#     end = dt.datetime.now(TIME_ZONE)

#     tickers = get_tickers()
#     stock_prices_df = get_stock_prices_yfinance_batches(tickers, start, end)
#     upload_and_merge_stock_prices_yfinance_df(stock_prices_df)


# @flow
# def stock_prices_yfinance_daily_flow():
#     last_market_date = get_last_market_date()
#     yesterday = (dt.datetime.now(TIME_ZONE) - dt.timedelta(days=1)).date()

#     # Only get new data if yesterday was the last market date
#     if last_market_date != yesterday:
#         print("Market was not open yesterday!")
#         print("Last Market Date:", last_market_date)
#         print("Yesterday:", yesterday)
#         return

#     start = dt.datetime.combine(yesterday, dt.time(0, 0, 0)).replace(tzinfo=TIME_ZONE)
#     end = dt.datetime.combine(yesterday, dt.time(23, 59, 59)).replace(tzinfo=TIME_ZONE)

#     tickers = get_tickers()
#     stock_prices_df = get_stock_prices_yfinance_batches(tickers, start, end)
#     upload_and_merge_stock_prices_yfinance_df(stock_prices_df)
