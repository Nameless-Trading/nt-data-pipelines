import polars as pl
from clients import get_bear_lake_client
from prefect import task
from variables import IC


@task
def calculate_scores(signals: pl.DataFrame, signal_name: str) -> pl.DataFrame:
    return signals.select(
        "ticker",
        "date",
        pl.col("date").dt.year().alias("year"),
        pl.lit(signal_name).alias("signal"),
        pl.col("value")
        .sub(pl.col("value").mean())
        .truediv(pl.col("value").std())
        .alias("score"),
    )


@task
def calculate_alphas(
    scores: pl.DataFrame, idio_vol: pl.DataFrame, signal_name: str
) -> pl.DataFrame:
    return (
        scores.join(other=idio_vol, on=["ticker", "date"], how="left")
        .select(
            "ticker",
            "date",
            pl.col("date").dt.year().alias("year"),
            pl.lit(signal_name).alias("signal"),
            pl.lit(IC).mul(pl.col("score")).mul(pl.col("idio_vol")).alias("alpha"),
        )
        .sort("ticker", "date")
    )


@task
def upload_and_merge_signals(signals: pl.DataFrame) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    table_name = "signals"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "signal": pl.String,
            "value": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["ticker", "date", "signal"],
        mode="skip",
    )

    # Insert
    bear_lake_client.insert(name=table_name, data=signals, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


@task
def upload_and_merge_scores(scores: pl.DataFrame) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    table_name = "scores"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "signal": pl.String,
            "score": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["ticker", "date", "signal"],
        mode="skip",
    )

    # Insert
    bear_lake_client.insert(name=table_name, data=scores, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


@task
def upload_and_merge_alphas(alphas: pl.DataFrame) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    table_name = "alphas"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "signal": pl.String,
            "alpha": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["ticker", "date", "signal"],
        mode="skip",
    )

    # Insert
    bear_lake_client.insert(name=table_name, data=alphas, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)
