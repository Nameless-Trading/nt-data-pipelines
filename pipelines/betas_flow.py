import polars as pl
from clients import get_clickhouse_client
import datetime as dt
from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm
from tqdm import tqdm
from prefect import task, flow
from variables import WINDOW, DISABLE_TQDM


@task
def get_stock_returns(start: dt.date, end: dt.date) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()

    stock_returns_arrow = clickhouse_client.query_arrow(
        f"""SELECT * FROM stock_returns WHERE date BETWEEN '{start}' AND '{end}'"""
    )

    return pl.from_arrow(stock_returns_arrow).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )


@task
def get_benchmark_returns(start: dt.date, end: dt.date) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()

    benchmark_returns_arrow = clickhouse_client.query_arrow(
        f"""SELECT * FROM benchmark_returns WHERE date BETWEEN '{start}' AND '{end}'"""
    )

    return pl.from_arrow(benchmark_returns_arrow).select(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"),
        pl.col("return").alias("benchmark_return"),
    )


@task
def estimate_regression(
    stock_returns: pl.DataFrame, benchmark_returns: pl.DataFrame
) -> pl.DataFrame:
    df = stock_returns.join(
        other=benchmark_returns,
        how="left",
        on="date",
    )

    results = []
    for ticker in tqdm(
        df["ticker"].unique(), desc="Estimating benchmark betas", disable=DISABLE_TQDM
    ):
        ticker_df = df.filter(pl.col("ticker") == ticker).sort("date")

        if len(ticker_df) < WINDOW:
            continue

        y = ticker_df["return"].to_pandas()
        X = sm.add_constant(ticker_df.select("benchmark_return").to_pandas())

        rolling_model = RollingOLS(y, X, window=WINDOW).fit()

        result = pl.DataFrame(
            {
                "ticker": ticker,
                "date": ticker_df["date"],
                "return": ticker_df["return"],
                "alpha": rolling_model.params["const"],
                "benchmark_return": ticker_df["benchmark_return"],
                "beta": rolling_model.params["benchmark_return"],
            }
        )
        results.append(result)

    results = pl.concat(results).with_columns(
        pl.col("return")
        .sub(pl.col("alpha") + pl.col("beta").mul(pl.col("benchmark_return")))
        .alias("residual")
    )

    return results.select("ticker", "date", "beta")


@task
def clean_betas(betas: pl.DataFrame) -> pl.DataFrame:
    return (
        betas.drop_nulls()
        .sort("ticker", "date")
        .select(
            "ticker",
            pl.col("date").cast(pl.String),
            pl.col("beta").alias("historical_beta"),
            pl.col("beta")
            .ewm_mean(half_life=60)
            .over("ticker")
            .alias("predicted_beta"),
        )
    )


@task
def upload_and_merge_betas(betas: pl.DataFrame) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()
    table_name = "betas"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ticker String,
            date String,
            historical_beta Float64,
            predicted_beta Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, date)
        """
    )

    # Insert into table in batches of 1,000,000 rows
    batch_size = 1_000_000
    total_rows = len(betas)

    for i in tqdm(
        range(0, total_rows, batch_size), desc="Inserting batches", disable=DISABLE_TQDM
    ):
        batch = betas.slice(i, batch_size)
        clickhouse_client.insert_df_arrow(table_name, batch)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


@flow
def betas_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)

    stock_returns = get_stock_returns(start, end)
    benchmark_returns = get_benchmark_returns(start, end)

    betas_raw = estimate_regression(stock_returns, benchmark_returns)

    betas = clean_betas(betas_raw)

    upload_and_merge_betas(betas)


@task
def get_trading_date_range(window: int) -> dt.date:
    clickhouse_client = get_clickhouse_client()
    date_range_arrow = clickhouse_client.query_arrow(
        f"SELECT date FROM calendar ORDER BY date DESC LIMIT {window}"
    )
    return pl.from_arrow(date_range_arrow).with_columns(
        pl.col("date").str.strptime(pl.Date, "%Y-%m-%d")
    )


@flow
def betas_daily_flow():
    date_range = get_trading_date_range(window=WINDOW * 2)

    start = date_range["date"].min()
    end = date_range["date"].max()

    yesterday = dt.date.today() - dt.timedelta(days=1)

    # Only get new data if yesterday was the last market date
    if end != yesterday:
        return

    stock_returns = get_stock_returns(start, end)
    benchmark_returns = get_benchmark_returns(start, end)

    betas_raw = estimate_regression(stock_returns, benchmark_returns)

    betas = clean_betas(betas_raw).filter(pl.col("date").eq(str(end)))

    upload_and_merge_betas(betas)
