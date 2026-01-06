import datetime as dt
import polars as pl
from clients import get_bear_lake_client
import bear_lake as bl
from prefect import task


@task
def get_universe(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("universe")
        .filter(pl.col("date").is_between(start, end))
        .select("date", "ticker")
        .sort("ticker", "date")
    )


@task
def get_universe_returns(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("universe")
        .join(other=bl.table("stock_returns"), on=["date", "ticker"], how="left")
        .filter(pl.col("date").is_between(start, end))
        .select("date", "ticker", "return")
        .sort("ticker", "date")
    )


@task
def get_stock_returns(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("stock_returns")
        .filter(pl.col("date").is_between(start, end))
        .select("date", "ticker", "return")
        .sort("ticker", "date")
    )


@task
def get_etf_returns(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("etf_returns")
        .filter(pl.col("date").is_between(start, end))
        .select("date", "ticker", "return")
        .sort("ticker", "date")
    )


@task
def get_alphas(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("universe")
        .join(other=bl.table("alphas"), on=["date", "ticker"], how="left")
        .filter(pl.col("date").is_between(start, end), pl.col("alpha").is_not_null())
        .select("date", "ticker", "alpha")
        .sort("ticker", "date")
    )


@task
def get_benchmark_weights(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("universe")
        .join(other=bl.table("benchmark_weights"), on=["date", "ticker"], how="left")
        .filter(pl.col("date").is_between(start, end))
        .select("date", "ticker", "weight")
        .sort("ticker", "date")
    )


@task
def get_benchmark_returns(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()

    return bear_lake_client.query(
        bl.table("benchmark_returns").filter(pl.col("date").is_between(start, end))
    )


@task
def get_factor_loadings(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("universe")
        .join(other=bl.table("factor_loadings"), on=["date", "ticker"], how="left")
        .filter(pl.col("date").is_between(start, end), pl.col("loading").is_not_null())
        .select("date", "ticker", "factor", "loading")
        .sort("ticker", "date")
    )


@task
def get_factor_covariances(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("factor_covariances")
        .filter(pl.col("date").is_between(start, end))
        .select("date", "factor_1", "factor_2", "covariance")
        .sort("date")
    )


@task
def get_idio_vol(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("universe")
        .join(other=bl.table("idio_vol"), on=["date", "ticker"], how="left")
        .filter(pl.col("date").is_between(start, end), pl.col("idio_vol").is_not_null())
        .select("date", "ticker", "idio_vol")
        .sort("ticker", "date")
    )


@task
def get_portfolio_weights(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("portfolio_weights")
        .filter(pl.col("date").is_between(start, end))
        .select("date", "ticker", "weight")
        .sort("ticker", "date")
    )


@task
def get_prices(start: dt.date, end: dt.date) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(
        bl.table("universe")
        .join(other=bl.table("stock_prices"), on=["date", "ticker"], how="left")
        .filter(pl.col("date").is_between(start, end))
        .select("date", "ticker", "close")
        .sort("ticker", "date")
    )
