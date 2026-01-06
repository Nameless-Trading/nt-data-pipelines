from prefect import task, flow
import polars as pl
import datetime as dt
from clients import get_bear_lake_client
from variables import IC
from utils import get_trading_date_range, get_idio_vol, get_stock_returns, get_universe


@task
def calculate_signals(stock_returns: pl.DataFrame) -> pl.DataFrame:
    return (
        stock_returns.sort("ticker", "date")
        .select(
            "ticker",
            "date",
            pl.col("date").dt.year().alias("year"),
            pl.lit("reversal").alias("signal"),
            pl.col("return")
            .log1p()
            .rolling_sum(21)
            .mul(-1)
            .over("ticker")
            .alias("value"),
        )
        .drop_nulls()
        .sort("ticker", "date")
    )


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


@flow
def reversal_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)
    signal_name = "reversal"

    stock_returns = get_stock_returns(start, end)
    idio_vol = get_idio_vol(start, end)

    signals = calculate_signals(stock_returns)
    scores = calculate_scores(signals, signal_name)
    alphas = calculate_alphas(scores, idio_vol, signal_name)

    upload_and_merge_signals(signals)
    upload_and_merge_scores(scores)
    upload_and_merge_alphas(alphas)


@flow
def reversal_daily_flow():
    date_range = get_trading_date_range(window=21)

    start = date_range["date"].min()
    end = date_range["date"].max()

    yesterday = dt.date.today() - dt.timedelta(days=1)

    # Only get new data if yesterday was the last market date
    if end != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", end)
        print("Yesterday:", yesterday)
        return

    signal_name = "reversal"

    stock_returns = get_stock_returns(start, end)
    idio_vol = get_idio_vol(start, end)

    signals = calculate_signals(stock_returns).filter(pl.col("date").eq(end))
    scores = calculate_scores(signals, signal_name).filter(pl.col("date").eq(end))
    alphas = calculate_alphas(scores, idio_vol, signal_name).filter(
        pl.col("date").eq(end)
    )

    if not (len(signals) > 0 and len(scores) > 0 and len(alphas) > 0):
        raise ValueError("No values found!")

    upload_and_merge_signals(signals)
    upload_and_merge_scores(scores)
    upload_and_merge_alphas(alphas)
