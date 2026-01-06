from prefect import task, flow
import polars as pl
import datetime as dt
from clients import get_bear_lake_client
from variables import TIME_ZONE
from utils import get_last_market_date, get_universe_returns


@task
def calculate_benchmark_weights(universe_returns: pl.DataFrame) -> pl.DataFrame:
    return universe_returns.select(
        "ticker",
        "date",
        pl.col("date").dt.year().alias("year"),
        pl.lit(1).truediv(pl.len()).over("date").alias("weight"),
    ).sort("ticker", "date")


@task
def calculate_benchmark_returns(
    universe_returns: pl.DataFrame, benchmark_weights: pl.DataFrame
) -> pl.DataFrame:
    return (
        benchmark_weights.join(
            other=universe_returns, on=["date", "ticker"], how="left"
        )
        .group_by("date")
        .agg(pl.col("return").mul(pl.col("weight")).sum())
        .sort("date")
    )


@task
def upload_and_merge_benchmark_weights(benchmark_weights: pl.DataFrame) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    table_name = "benchmark_weights"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "weight": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["date", "ticker"],
        mode="skip",
    )

    # Insert
    bear_lake_client.insert(name=table_name, data=benchmark_weights, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


@task
def upload_and_merge_benchmark_returns(benchmark_returns: pl.DataFrame) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    table_name = "benchmark_returns"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={"date": pl.Date, "return": pl.Float64},
        partition_keys=None,
        primary_keys=["date"],
        mode="skip",
    )

    # Insert
    bear_lake_client.insert(name=table_name, data=benchmark_returns, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


@flow
def benchmark_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)

    universe_returns = get_universe_returns(start, end)

    benchmark_weights = calculate_benchmark_weights(universe_returns)
    benchmark_returns = calculate_benchmark_returns(universe_returns, benchmark_weights)

    upload_and_merge_benchmark_weights(benchmark_weights)
    upload_and_merge_benchmark_returns(benchmark_returns)


@flow
def benchmark_daily_flow():
    last_market_date = get_last_market_date()
    yesterday = (dt.datetime.now(TIME_ZONE) - dt.timedelta(days=1)).date()

    # Only get new data if yesterday was the last market date
    if last_market_date != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", last_market_date)
        print("Yesterday:", yesterday)
        return

    universe_returns = get_universe_returns(yesterday, yesterday)

    benchmark_weights = calculate_benchmark_weights(universe_returns)
    benchmark_returns = calculate_benchmark_returns(universe_returns, benchmark_weights)

    upload_and_merge_benchmark_weights(benchmark_weights)
    upload_and_merge_benchmark_returns(benchmark_returns)
