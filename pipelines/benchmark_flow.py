from prefect import task, flow
import polars as pl
import datetime as dt
from clients import get_clickhouse_client
from variables import TIME_ZONE


@task
def get_universe_returns(start: dt.date, end: dt.date) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()

    stock_returns_arrow = clickhouse_client.query_arrow(
        f"""
        SELECT
            u.ticker,
            u.date,
            s.return
        FROM universe u
        LEFT JOIN stock_returns s ON u.date = s.date AND u.ticker = s.ticker
        WHERE u.date BETWEEN '{start}' AND '{end}'
        ORDER BY u.ticker, u.date
        """
    )

    return pl.from_arrow(stock_returns_arrow)


@task
def get_benchmark_weights(universe_returns: pl.DataFrame) -> pl.DataFrame:
    return universe_returns.select(
        "ticker", "date", pl.lit(1).truediv(pl.len()).over("date").alias("weight")
    ).sort("ticker", "date")


@task
def get_benchmark_returns(
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
    clickhouse_client = get_clickhouse_client()
    table_name = "benchmark_weights"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ticker String,
            date String,
            weight Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, date)
        """
    )

    # Insert
    clickhouse_client.insert_df_arrow(table_name, benchmark_weights)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


@task
def upload_and_merge_benchmark_returns(benchmark_returns: pl.DataFrame) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()
    table_name = "benchmark_returns"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date String,
            return Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (date)
        """
    )

    # Insert
    clickhouse_client.insert_df_arrow(table_name, benchmark_returns)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


@flow
def benchmark_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)

    universe_returns = get_universe_returns(start, end)

    benchmark_weights = get_benchmark_weights(universe_returns)
    benchmark_returns = get_benchmark_returns(universe_returns, benchmark_weights)

    upload_and_merge_benchmark_weights(benchmark_weights)
    upload_and_merge_benchmark_returns(benchmark_returns)


@task
def get_last_market_date() -> dt.date:
    clickhouse_client = get_clickhouse_client()
    last_market_date = clickhouse_client.query("SELECT MAX(date) FROM calendar")
    return dt.datetime.strptime(last_market_date.result_rows[0][0], "%Y-%m-%d").date()


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

    start = dt.datetime.combine(yesterday, dt.time(0, 0, 0)).replace(tzinfo=TIME_ZONE)
    end = dt.datetime.combine(yesterday, dt.time(23, 59, 59)).replace(tzinfo=TIME_ZONE)

    universe_returns = get_universe_returns(start, end)

    benchmark_weights = get_benchmark_weights(universe_returns)
    benchmark_returns = get_benchmark_returns(universe_returns, benchmark_weights)

    upload_and_merge_benchmark_weights(benchmark_weights)
    upload_and_merge_benchmark_returns(benchmark_returns)
