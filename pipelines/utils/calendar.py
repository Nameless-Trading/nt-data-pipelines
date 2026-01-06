from clients import get_bear_lake_client
import datetime as dt
import polars as pl
import bear_lake as bl


def get_last_market_date() -> dt.date:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(bl.table("calendar").select(pl.col("date").max()))[
        "date"
    ][0]


def get_trading_date_range(window: int) -> dt.date:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("calendar").sort("date", descending=True).select("date").head(window)
    )
