import polars as pl
import pandas as pd
import datetime as dt
import pandas_market_calendars as mcal
from prefect import flow, task

def get_market_calendar(start: dt.date, end: dt.date) -> pl.DataFrame:
    """
    Get stock market calendar (trading days) between two dates.

    Args:
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format

    Returns:
        Polars DataFrame with trading day information
    """

    # Get NYSE calendar (US stock market)
    nyse = mcal.get_calendar('NYSE')

    # Get schedule between dates
    schedule = nyse.schedule(start_date=start, end_date=end)

    # Convert to polars with additional info
    calendar_df = (
        pl.from_pandas(schedule.reset_index())
        .select(
            pl.col("index").cast(pl.Date).alias('date'),
        )
        .sort('date')
    )

    return calendar_df


def calendar_backfill_flow(start: dt.date = dt.date(1957, 3, 1), end: dt.date = dt.date.today()):
    calendar = get_market_calendar(start, end)
    print(calendar)

if __name__ == '__main__':
    calendar_backfill_flow()
