import datetime as dt
import polars as pl
from utils import (
    get_covariance_matrix,
    get_optimal_weights_dynamic,
    get_alphas,
    get_idio_vol,
    get_benchmark_weights,
    get_factor_covariances,
    get_factor_loadings,
)
from variables import TARGET_ACTIVE_RISK
from clients import get_clickhouse_client
from prefect import task, flow


@task
def get_portfolio_weights(
    date_: dt.date,
    alphas: pl.DataFrame,
    covariance_matrix: pl.DataFrame,
    benchmark_weights: pl.DataFrame,
) -> pl.DataFrame:
    optimal_weights, lambda_, active_risk = get_optimal_weights_dynamic(
        alphas=alphas,
        covariance_matrix=covariance_matrix,
        benchmark_weights=benchmark_weights,
        target_active_risk=TARGET_ACTIVE_RISK,
    )

    weights_df = optimal_weights.with_columns(pl.lit(str(date_)).alias("date"))
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
    dates = alphas["date"].unique().sort().to_list()

    weights_list = []
    metrics_list = []
    for date_ in dates:
        alphas_slice = alphas.filter(pl.col("date").eq(date_)).sort("ticker")
        benchmark_weights_slice = benchmark_weights.filter(
            pl.col("date").eq(date_)
        ).sort("ticker")

        factor_loadings_slice = factor_loadings.filter(pl.col("date").eq(date_)).sort(
            "ticker"
        )
        factor_covariances_slice = factor_covariances.filter(
            pl.col("date").eq(date_)
        ).sort("factor_1")
        idio_vol_slice = idio_vol.filter(pl.col("date").eq(date_)).sort("ticker")

        covariance_matrix = get_covariance_matrix(
            factor_loadings_slice, factor_covariances_slice, idio_vol_slice
        )

        weights_df, metrics_df = get_portfolio_weights(
            date_, alphas_slice, covariance_matrix, benchmark_weights_slice
        )

        weights_list.append(weights_df)
        metrics_list.append(metrics_df)

    metrics_df = pl.concat(metrics_list)
    weights_df = pl.concat(weights_list)

    return weights_df, metrics_df


@task
def upload_and_merge_portfolio_weights(portfolio_weights: pl.DataFrame):
    clickhouse_client = get_clickhouse_client()
    table_name = "portfolio_weights"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date String,
            ticker String,
            weight Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, date)
        """
    )

    # Insert into table
    clickhouse_client.insert_df_arrow(table_name, portfolio_weights)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


@task
def upload_and_merge_portfolio_metrics(portfolio_metrics: pl.DataFrame):
    clickhouse_client = get_clickhouse_client()
    table_name = "portfolio_metrics"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date String,
            lambda Float64,
            active_risk Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (date)
        """
    )

    # Insert into table
    clickhouse_client.insert_df_arrow(table_name, portfolio_metrics)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


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
    yesterday = dt.date.today() - dt.timedelta(days=1)

    alphas = get_alphas(yesterday, yesterday)
    benchmark_weights = get_benchmark_weights(yesterday, yesterday)
    factor_loadings = get_factor_loadings(yesterday, yesterday)
    factor_covariances = get_factor_covariances(yesterday, yesterday)
    idio_vol = get_idio_vol(yesterday, yesterday)

    covariance_matrix = get_covariance_matrix(
        factor_loadings, factor_covariances, idio_vol
    )

    portfolio_weights, portfolio_metrics = get_portfolio_weights(
        date_=yesterday,
        alphas=alphas,
        covariance_matrix=covariance_matrix,
        benchmark_weights=benchmark_weights,
    )

    upload_and_merge_portfolio_weights(portfolio_weights)
    upload_and_merge_portfolio_metrics(portfolio_metrics)
