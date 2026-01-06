import polars as pl
import pandas as pd
import datetime as dt
import pandas_market_calendars as mcal
from clients import get_bear_lake_client
from prefect import task, flow


@task
def get_market_calendar(start: dt.date, end: dt.date) -> pl.DataFrame:
    # Get NYSE calendar (US stock market)
    nyse = mcal.get_calendar("NYSE")

    # Get schedule between dates
    schedule = nyse.schedule(start_date=start, end_date=end)

    # Convert to polars with additional info
    calendar_df = (
        pl.from_pandas(schedule.reset_index())
        .select(
            pl.col("index").cast(pl.Date).alias("date"),
        )
        .drop_nulls()
        .sort("date")
    )

    return calendar_df


@task
def upload_calendar_df(calendar_df: pl.DataFrame):
    table_name = "calendar"

    # Get ClickHouse client
    bear_lake_client = get_bear_lake_client()

    # Create
    bear_lake_client.create(
        name=table_name,
        schema={"date": pl.Date},
        partition_keys=None,
        primary_keys=["date"],
        mode="replace",
    )

    # Insert
    bear_lake_client.insert(name=table_name, data=calendar_df, mode="append")


@flow
def calendar_backfill_flow():
    start = dt.date(1957, 3, 1)
    end = dt.date.today() - dt.timedelta(days=1)  # yesterday
    calendar_df = get_market_calendar(start, end)
    upload_calendar_df(calendar_df)
