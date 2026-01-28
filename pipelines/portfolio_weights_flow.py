import datetime as dt
import os

import polars as pl
import ray
from clients import get_bear_lake_client
from prefect import flow, task
from utils import (
    get_alphas,
    get_benchmark_weights,
    get_covariance_matrix,
    get_factor_covariances,
    get_factor_loadings,
    get_idio_vol,
    get_last_market_date,
    get_optimal_weights_dynamic,
)
from variables import TARGET_ACTIVE_RISK

# Suppress Ray GPU warning for CPU-only usage
os.environ["RAY_ACCEL_ENV_VAR_OVERRIDE_ON_ZERO"] = "0"


@ray.remote
def get_portfolio_weights_for_date_parallel(
    date_: str,
    alphas: pl.DataFrame,
    benchmark_weights: pl.DataFrame,
    factor_loadings: pl.DataFrame,
    factor_covariances: pl.DataFrame,
    idio_vol: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    alphas_slice = alphas.filter(pl.col("date").eq(date_)).sort("ticker")
    tickers = alphas_slice["ticker"].unique().sort().to_list()

    benchmark_weights_slice = benchmark_weights.filter(pl.col("date").eq(date_)).sort(
        "ticker"
    )

    factor_loadings_slice = factor_loadings.filter(pl.col("date").eq(date_)).sort(
        "ticker"
    )
    factor_covariances_slice = factor_covariances.filter(pl.col("date").eq(date_)).sort(
        "factor_1"
    )
    idio_vol_slice = idio_vol.filter(pl.col("date").eq(date_)).sort("ticker")

    covariance_matrix = get_covariance_matrix(
        tickers=tickers,
        factor_loadings=factor_loadings_slice,
        factor_covariances=factor_covariances_slice,
        idio_vol=idio_vol_slice,
    )

    optimal_weights, lambda_, active_risk = get_optimal_weights_dynamic(
        alphas=alphas_slice,
        covariance_matrix=covariance_matrix,
        benchmark_weights=benchmark_weights_slice,
        target_active_risk=TARGET_ACTIVE_RISK,
    )

    weights_df = optimal_weights.with_columns(pl.lit(date_).alias("date"))
    metrics_df = pl.DataFrame(
        {"lambda": [lambda_], "active_risk": [active_risk], "date": [str(date_)]}
    )

    return weights_df, metrics_df


@task
def get_portfolio_weights_for_date(
    date_: str,
    alphas: pl.DataFrame,
    benchmark_weights: pl.DataFrame,
    factor_loadings: pl.DataFrame,
    factor_covariances: pl.DataFrame,
    idio_vol: pl.DataFrame,
) -> pl.DataFrame:
    tickers = alphas["ticker"].unique().sort().to_list()
    covariance_matirx = get_covariance_matrix(
        tickers, factor_loadings, factor_covariances, idio_vol
    )

    optimal_weights, lambda_, active_risk = get_optimal_weights_dynamic(
        alphas=alphas,
        covariance_matrix=covariance_matirx,
        benchmark_weights=benchmark_weights,
        target_active_risk=TARGET_ACTIVE_RISK,
    )

    weights_df = optimal_weights.with_columns(
        pl.lit(date_).alias("date"), pl.lit(date_.year).alias("year")
    )
    metrics_df = pl.DataFrame(
        {"lambda": [lambda_], "active_risk": [active_risk], "date": [str(date_)]}
    )

    return weights_df, metrics_df


@task
def get_portfolio_weights_history(
    alphas: pl.DataFrame,
    benchmark_weights: pl.DataFrame,
    factor_loadings: pl.DataFrame,
    factor_covariances: pl.DataFrame,
    idio_vol: pl.DataFrame,
) -> tuple[pl.DataFrame]:
    ray.init(
        dashboard_host="0.0.0.0",
        dashboard_port=8265,
        ignore_reinit_error=True,
        num_cpus=os.cpu_count(),
    )

    dates = alphas["date"].unique().sort().to_list()

    # Put DataFrames in Ray's object store once to avoid repeated serialization
    alphas_ref = ray.put(alphas)
    benchmark_weights_ref = ray.put(benchmark_weights)
    factor_loadings_ref = ray.put(factor_loadings)
    factor_covariances_ref = ray.put(factor_covariances)
    idio_vol_ref = ray.put(idio_vol)

    # Create futures for parallel processing
    futures = [
        get_portfolio_weights_for_date_parallel.remote(
            date_,
            alphas_ref,
            benchmark_weights_ref,
            factor_loadings_ref,
            factor_covariances_ref,
            idio_vol_ref,
        )
        for date_ in dates
    ]

    # Get results
    results = ray.get(futures)

    # Unpack results
    weights_list = [r[0] for r in results]
    metrics_list = [r[1] for r in results]

    weights_df = pl.concat(weights_list).with_columns(
        pl.col("date").dt.year().alias("year")
    )
    metrics_df = pl.concat(metrics_list)

    return weights_df, metrics_df


@task
def upload_and_merge_portfolio_weights(portfolio_weights: pl.DataFrame):
    bear_lake_client = get_bear_lake_client()
    table_name = "portfolio_weights"

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

    # Insert into table
    bear_lake_client.insert(name=table_name, data=portfolio_weights, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


@task
def upload_and_merge_portfolio_metrics(portfolio_metrics: pl.DataFrame):
    bear_lake_client = get_bear_lake_client()
    table_name = "portfolio_metrics"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={"date": pl.Date, "lambda": pl.Float64, "active_risk": pl.Float64},
        partition_keys=None,
        primary_keys=["date"],
        mode="skip",
    )

    # Insert into table
    bear_lake_client.insert(name=table_name, data=portfolio_metrics, mode="append")

    # Optimize table (deduplicate)
    bear_lake_client.optimize(name=table_name)


@flow
def portfolio_weights_backfill_flow():
    start = dt.date(2022, 7, 29)
    end = dt.date.today() - dt.timedelta(days=1)

    alphas = get_alphas(start, end)
    benchmark_weights = get_benchmark_weights(start, end)
    factor_loadings = get_factor_loadings(start, end)
    factor_covariances = get_factor_covariances(start, end)
    idio_vol = get_idio_vol(start, end)

    portfolio_weights, portfolio_metrics = get_portfolio_weights_history(
        alphas, benchmark_weights, factor_loadings, factor_covariances, idio_vol
    )

    upload_and_merge_portfolio_weights(portfolio_weights)
    upload_and_merge_portfolio_metrics(portfolio_metrics)


@flow
def portfolio_weights_daily_flow():
    last_market_date = get_last_market_date()
    yesterday = dt.date.today() - dt.timedelta(days=1)

    # Only get new data if yesterday was the last market date
    if last_market_date != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", last_market_date)
        print("Yesterday:", yesterday)
        return

    alphas = get_alphas(last_market_date, last_market_date)
    benchmark_weights = get_benchmark_weights(last_market_date, last_market_date)
    factor_loadings = get_factor_loadings(last_market_date, last_market_date)
    factor_covariances = get_factor_covariances(last_market_date, last_market_date)
    idio_vol = get_idio_vol(last_market_date, last_market_date)

    portfolio_weights, portfolio_metrics = get_portfolio_weights_for_date(
        date_=last_market_date,
        alphas=alphas,
        benchmark_weights=benchmark_weights,
        factor_loadings=factor_loadings,
        factor_covariances=factor_covariances,
        idio_vol=idio_vol,
    )

    upload_and_merge_portfolio_weights(portfolio_weights)
    upload_and_merge_portfolio_metrics(portfolio_metrics)
