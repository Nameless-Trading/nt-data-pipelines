import polars as pl
import datetime as dt
from clients import get_bear_lake_client
from prefect import task, flow
from variables import WINDOW
from utils import get_trading_date_range, get_etf_returns


@task
def estimate_factor_covariances(etf_returns: pl.DataFrame) -> pl.DataFrame:
    factor_returns_pd = (
        etf_returns.sort("ticker", "date")
        .pivot(on="ticker", index="date", values="return")
        .to_pandas()
        .set_index("date")
    )

    factor_covariances = (
        pl.from_pandas(
            factor_returns_pd.rolling(window=WINDOW, min_periods=WINDOW)
            .cov()
            .reset_index()
        )
        .rename({"level_1": "factor_1"})
        .with_columns(pl.col("date").dt.date())
    )

    return factor_covariances


@task
def clean_factor_covariances(factor_covariances: pl.DataFrame) -> pl.DataFrame:
    return (
        factor_covariances.drop_nulls()
        .unpivot(
            index=["date", "factor_1"],
            variable_name="factor_2",
            value_name="covariance",
        )
        .sort("factor_1", "factor_2", "date")
        .with_columns(
            pl.col("covariance").ewm_mean(half_life=60).over("factor_1", "factor_2"),
            pl.col("date").dt.year().alias("year"),
        )
    )


@task
def upload_and_merge_factor_covariances(factor_covariances: pl.DataFrame):
    bear_lake_client = get_bear_lake_client()
    table_name = "factor_covariances"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "date": pl.Date,
            "year": pl.Int32,
            "factor_1": pl.String,
            "factor_2": pl.String,
            "covariance": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["date", "factor_1", "factor_2"],
        mode="skip",
    )

    # Insert data
    bear_lake_client.insert(name=table_name, data=factor_covariances, mode="append")

    # Optimize
    bear_lake_client.optimize(name=table_name)


@flow
def factor_covariances_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)

    etf_returns = get_etf_returns(start, end)

    factor_covariances = estimate_factor_covariances(etf_returns)
    factor_covariances_clean = clean_factor_covariances(factor_covariances)

    upload_and_merge_factor_covariances(factor_covariances_clean)


@flow
def factor_covariances_daily_flow():
    date_range = get_trading_date_range(window=WINDOW)

    start = date_range["date"].min()
    end = date_range["date"].max()

    yesterday = dt.date.today() - dt.timedelta(days=1)

    # Only get new data if yesterday was the last market date
    if end != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", end)
        print("Yesterday:", yesterday)
        return

    etf_returns = get_etf_returns(start, end)

    factor_covariances = estimate_factor_covariances(etf_returns)
    factor_covariances_clean = clean_factor_covariances(factor_covariances)

    upload_and_merge_factor_covariances(factor_covariances_clean)
