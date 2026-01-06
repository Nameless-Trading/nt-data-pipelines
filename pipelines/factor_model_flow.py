import polars as pl
from clients import get_bear_lake_client
import datetime as dt
from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm
from tqdm import tqdm
from prefect import task, flow
from variables import WINDOW, FACTORS, DISABLE_TQDM
from utils import get_trading_date_range, get_stock_returns, get_etf_returns


@task
def estimate_regression(
    stock_returns: pl.DataFrame, etf_returns: pl.DataFrame
) -> tuple[pl.DataFrame]:
    df = stock_returns.join(
        other=etf_returns.pivot(on="ticker", index="date", values="return"),
        how="left",
        on="date",
    )

    results = []
    for ticker in tqdm(
        df["ticker"].unique(), desc="Estimating factor loadings", disable=DISABLE_TQDM
    ):
        ticker_df = df.filter(pl.col("ticker") == ticker).sort("date")

        if len(ticker_df) < WINDOW:
            result = pl.DataFrame(
                {
                    "ticker": ticker,
                    "date": ticker_df["date"],
                    "return": ticker_df["return"],
                    "alpha": None,
                }
                | {factor: ticker_df[factor] for factor in FACTORS}
                | {f"B_{factor}": None for factor in FACTORS}
            )
            continue

        y = ticker_df["return"].to_pandas()
        X = sm.add_constant(ticker_df.select(FACTORS).to_pandas())

        rolling_model = RollingOLS(y, X, window=WINDOW).fit()

        result = pl.DataFrame(
            {
                "ticker": ticker,
                "date": ticker_df["date"],
                "return": ticker_df["return"],
                "alpha": rolling_model.params["const"],
            }
            | {factor: ticker_df[factor] for factor in FACTORS}
            | {f"B_{factor}": rolling_model.params[factor] for factor in FACTORS}
        )
        results.append(result)

    results = pl.concat(results).with_columns(
        pl.col("return")
        .sub(
            pl.col("alpha")
            + pl.sum_horizontal(
                pl.col(factor).mul(pl.col(f"B_{factor}")) for factor in FACTORS
            )
        )
        .alias("residual")
    )

    betas = results.select("ticker", "date", *[f"B_{factor}" for factor in FACTORS])
    residuals = results.select("ticker", "date", "residual")

    return betas, residuals


@task
def clean_factor_loadings(factor_loadings: pl.DataFrame) -> pl.DataFrame:
    return (
        factor_loadings.unpivot(
            index=["ticker", "date"], variable_name="factor", value_name="loading"
        )
        .sort("ticker", "date")
        .with_columns(
            pl.col("factor").replace({f"B_{factor}": factor for factor in FACTORS})
        )
        .with_columns(
            pl.col("loading").ewm_mean(half_life=60).over("ticker", "factor"),
            pl.col("date").dt.year().alias("year"),
        )
    )


@task
def clean_idio_vol(residuals: pl.DataFrame) -> pl.DataFrame:
    return residuals.sort("ticker", "date").select(
        "ticker",
        "date",
        pl.col("date").dt.year().alias("year"),
        pl.col("residual")
        .rolling_std(window_size=WINDOW)
        .ewm_mean(half_life=60)
        .over("ticker")
        .alias("idio_vol"),
    )


@task
def upload_and_merge_factor_loadings(factor_loadings: pl.DataFrame) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    table_name = "factor_loadings"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "factor": pl.String,
            "loading": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["date", "ticker", "factor"],
        mode="skip",
    )

    # Insert data
    bear_lake_client.insert(name=table_name, data=factor_loadings, mode="append")

    # Optimize
    bear_lake_client.optimize(name=table_name)


@task
def upload_and_merge_idio_vol(idio_vol: pl.DataFrame) -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    table_name = "idio_vol"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "idio_vol": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["date", "ticker"],
        mode="skip",
    )

    # Insert data
    bear_lake_client.insert(name=table_name, data=idio_vol, mode="append")

    # Optimize
    bear_lake_client.optimize(name=table_name)


@flow
def factor_model_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)

    stock_returns = get_stock_returns(start, end)
    etf_returns = get_etf_returns(start, end)

    betas, residuals = estimate_regression(stock_returns, etf_returns)

    factor_loadings = clean_factor_loadings(betas)
    idio_vol = clean_idio_vol(residuals)

    upload_and_merge_factor_loadings(factor_loadings)
    upload_and_merge_idio_vol(idio_vol)


@flow
def factor_model_daily_flow():
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
    etf_returns = get_etf_returns(start, end)

    betas, residuals = estimate_regression(stock_returns, etf_returns)

    factor_loadings = clean_factor_loadings(betas).filter(pl.col("date").eq(end))
    idio_vol = clean_idio_vol(residuals).filter(pl.col("date").eq(end))

    upload_and_merge_factor_loadings(factor_loadings)
    upload_and_merge_idio_vol(idio_vol)
