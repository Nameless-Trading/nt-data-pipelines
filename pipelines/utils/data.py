import datetime as dt
import polars as pl
from clients import get_clickhouse_client


def get_alphas(start: dt.date, end: dt.date) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()

    data_arrow = clickhouse_client.query_arrow(
        f"""
        SELECT 
            u.date,
            u.ticker,
            a.alpha
        FROM universe u
        INNER JOIN alphas a ON u.date = a.date AND u.ticker = a.ticker
        WHERE u.date BETWEEN '{start}' AND '{end}'
        """
    )

    return pl.from_arrow(data_arrow).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )


def get_benchmark_weights(start: dt.date, end: dt.date) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()

    data_arrow = clickhouse_client.query_arrow(
        f"""
        SELECT 
            u.date,
            u.ticker,
            b.weight
        FROM universe u
        INNER JOIN benchmark_weights b ON u.date = b.date AND u.ticker = b.ticker
        WHERE u.date BETWEEN '{start}' AND '{end}'
        """
    )

    return pl.from_arrow(data_arrow).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )


def get_factor_loadings(start: dt.date, end: dt.date) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()

    data_arrow = clickhouse_client.query_arrow(
        f"""
        SELECT 
            u.date,
            u.ticker,
            f.factor,
            f.loading
        FROM universe u
        INNER JOIN factor_loadings f ON u.date = f.date AND u.ticker = f.ticker
        WHERE u.date BETWEEN '{start}' AND '{end}'
        """
    )

    return pl.from_arrow(data_arrow).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )


def get_factor_covariances(start: dt.date, end: dt.date) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()

    data_arrow = clickhouse_client.query_arrow(
        f"""
        SELECT 
            date,
            factor_1,
            factor_2,
            covariance
        FROM factor_covariances
        WHERE date BETWEEN '{start}' AND '{end}'
        """
    )

    return pl.from_arrow(data_arrow).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )


def get_idio_vol(start: dt.date, end: dt.date) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()

    data_arrow = clickhouse_client.query_arrow(
        f"""
        SELECT 
            u.date,
            u.ticker,
            i.idio_vol
        FROM universe u
        INNER JOIN idio_vol i ON u.date = i.date AND u.ticker = i.ticker
        WHERE u.date BETWEEN '{start}' AND '{end}'
        """
    )

    return pl.from_arrow(data_arrow).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )
