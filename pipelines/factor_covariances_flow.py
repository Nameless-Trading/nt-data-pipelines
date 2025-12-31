import polars as pl
import datetime as dt
from clients import get_clickhouse_client
from prefect import task, flow
from variables import WINDOW, FACTORS


@task
def get_etf_returns(start: dt.date, end: dt.date) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()

    etf_returns_arrow = clickhouse_client.query_arrow(
        f"""SELECT * FROM etf_returns WHERE date BETWEEN '{start}' AND '{end}'"""
    )

    return (
        pl.from_arrow(etf_returns_arrow)
        .with_columns(pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
        .filter(pl.col("ticker").is_in(FACTORS))
    )


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
            pl.col("date").cast(pl.String),
        )
    )


@task
def upload_and_merge_factor_covariances(factor_covariances: pl.DataFrame):
    clickhouse_client = get_clickhouse_client()
    table_name = "factor_covariances"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date String,
            factor_1 String,
            factor_2 String,
            covariance Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (date, factor_1, factor_2)
        """
    )

    # Insert into master table
    clickhouse_client.insert_df_arrow(table_name, factor_covariances)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


@flow
def factor_covariances_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date(2025, 12, 29)

    etf_returns = get_etf_returns(start, end)

    factor_covariances = estimate_factor_covariances(etf_returns)
    factor_covariances_clean = clean_factor_covariances(factor_covariances)

    upload_and_merge_factor_covariances(factor_covariances_clean)
