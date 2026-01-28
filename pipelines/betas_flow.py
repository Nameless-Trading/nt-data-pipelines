import datetime as dt

import polars as pl
import statsmodels.api as sm
from clients import get_bear_lake_client
from prefect import flow, task
from statsmodels.regression.rolling import RollingOLS
from tqdm import tqdm
from utils import get_benchmark_returns, get_stock_returns, get_trading_date_range
from variables import DISABLE_TQDM, WINDOW


@task
def estimate_regression(
    stock_returns: pl.DataFrame, benchmark_returns: pl.DataFrame
) -> pl.DataFrame:
    df = stock_returns.join(
        other=benchmark_returns.rename({"return": "benchmark_return"}),
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
            "date",
            pl.col("date").dt.year().alias("year"),
            pl.col("beta").alias("historical_beta"),
            pl.col("beta")
            .ewm_mean(half_life=60)
            .over("ticker")
            .alias("predicted_beta"),
        )
    )


@task
def upload_and_merge_betas(betas: pl.DataFrame) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    table_name = "betas"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "historical_beta": pl.Float64,
            "predicted_beta": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["ticker", "date"],
        mode="skip",
    )

    # Insert data
    bear_lake_client.insert(name=table_name, data=betas, mode="append")

    # Optimize
    bear_lake_client.optimize(name=table_name)


@flow
def betas_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)

    stock_returns = get_stock_returns(start, end)
    benchmark_returns = get_benchmark_returns(start, end)

    betas_raw = estimate_regression(stock_returns, benchmark_returns)

    betas = clean_betas(betas_raw)

    upload_and_merge_betas(betas)


@flow
def betas_daily_flow():
    date_range = get_trading_date_range(window=WINDOW * 2)

    start = date_range["date"].min()
    end = date_range["date"].max()

    yesterday = dt.date.today() - dt.timedelta(days=1)

    # Only get new data if yesterday was the last market date
    if end != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", end)
        print("Yesterday:", yesterday)
        return

    stock_returns = get_stock_returns(start, end)
    benchmark_returns = get_benchmark_returns(start, end)

    betas_raw = estimate_regression(stock_returns, benchmark_returns)

    betas = clean_betas(betas_raw).filter(pl.col("date").eq(end))

    upload_and_merge_betas(betas)
