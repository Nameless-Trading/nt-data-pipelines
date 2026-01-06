from clients import get_bear_lake_client
from prefect import task, flow
import bear_lake as bl
import polars as pl


@task
def materialize_stock_returns():
    bear_lake_client = get_bear_lake_client()
    table_name = "stock_returns"

    # Get stock returns
    stock_returns = bear_lake_client.query(
        bl.table("stock_prices")
        .sort("ticker", "date")
        .select(
            "ticker",
            "date",
            pl.col("date").dt.year().alias("year"),
            pl.col("close").pct_change().over("ticker").alias("return"),
        )
        .drop_nulls()
        .sort("ticker", "date")
    )

    # Create or replace table
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "return": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["ticker", "date"],
        mode="replace",
    )

    # Insert data
    bear_lake_client.insert(name=table_name, data=stock_returns, mode="append")


@task
def materialize_etf_returns():
    bear_lake_client = get_bear_lake_client()
    table_name = "etf_returns"

    # Get stock returns
    stock_returns = bear_lake_client.query(
        bl.table("etf_prices")
        .sort("ticker", "date")
        .select(
            "ticker",
            "date",
            pl.col("date").dt.year().alias("year"),
            pl.col("close").pct_change().over("ticker").alias("return"),
        )
        .drop_nulls()
        .sort("ticker", "date")
    )

    # Create or replace table
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "return": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["ticker", "date"],
        mode="replace",
    )

    # Insert data
    bear_lake_client.insert(name=table_name, data=stock_returns, mode="append")


@flow
def returns_backfill_flow():
    materialize_stock_returns()
    materialize_etf_returns()
