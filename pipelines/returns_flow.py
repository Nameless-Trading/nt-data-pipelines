from clients import get_clickhouse_client
from prefect import task, flow


@task
def materialize_stock_returns():
    clickhouse_client = get_clickhouse_client()
    clickhouse_client.command(
        """
        CREATE OR REPLACE TABLE stock_returns
        ENGINE = MergeTree()
        ORDER BY (ticker, date)
        AS
        WITH transform AS (
            SELECT
                ticker,
                date,
                close,
                lag(close, 1, close) OVER (PARTITION BY ticker ORDER BY date) AS previous_day_close,
                close / previous_day_close - 1 AS return
            FROM stock_prices
        )
        SELECT
            ticker,
            date,
            return
        FROM transform;
        """
    )


@task
def materialize_etf_returns():
    clickhouse_client = get_clickhouse_client()
    clickhouse_client.command(
        """
        CREATE OR REPLACE TABLE etf_returns
        ENGINE = MergeTree()
        ORDER BY (ticker, date)
        AS
        WITH transform AS (
            SELECT
                ticker,
                date,
                close,
                lag(close, 1, close) OVER (PARTITION BY ticker ORDER BY date) AS previous_day_close,
                close / previous_day_close - 1 AS return
            FROM etf_prices
        )
        SELECT
            ticker,
            date,
            return
        FROM transform;
        """
    )


@flow
def returns_backfill_flow():
    materialize_stock_returns()
    materialize_etf_returns()
