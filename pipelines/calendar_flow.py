import polars as pl
import pandas as pd
import datetime as dt
import pandas_market_calendars as mcal
from clients import get_clickhouse_client
from prefect import task, flow

@task
def get_market_calendar(start: dt.date, end: dt.date) -> pl.DataFrame:
    # Get NYSE calendar (US stock market)
    nyse = mcal.get_calendar('NYSE')

    # Get schedule between dates
    schedule = nyse.schedule(start_date=start, end_date=end)

    # Convert to polars with additional info
    calendar_df = (
        pl.from_pandas(schedule.reset_index())
        .select(
            pl.col("index").cast(pl.Date).cast(pl.String).alias('date'),
        )
        .drop_nulls()
        .sort('date')
    )

    return calendar_df

@task
def upload_calendar_df(calendar_df: pl.DataFrame):
    table_name = "calendar"

    # Get ClickHouse client
    clickhouse_client = get_clickhouse_client()

    # Drop
    clickhouse_client.command(f"DROP TABLE IF EXISTS {table_name}")

    # Create
    clickhouse_client.command(
        f"""
        CREATE TABLE {table_name} (
            date String
        )
        ENGINE = MergeTree()
        ORDER BY date
        """
    )

    # Insert
    clickhouse_client.insert_df_arrow(table=table_name, df=calendar_df)

@flow
def calendar_backfill_flow(start: dt.date = dt.date(1957, 3, 1), end: dt.date = dt.date.today()):
    calendar_df = get_market_calendar(start, end)
    upload_calendar_df(calendar_df)
    