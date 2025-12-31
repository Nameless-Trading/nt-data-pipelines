import polars as pl
from clients import get_clickhouse_client
import datetime as dt
from statsmodels.regression.rolling import RollingOLS
import statsmodels.api as sm
from tqdm import tqdm
from prefect import task, flow
from variables import WINDOW, FACTORS, DISABLE_TQDM


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
        .drop_nulls()
        .sort("ticker", "date")
        .with_columns(
            pl.col("factor").replace({f"B_{factor}": factor for factor in FACTORS})
        )
        .with_columns(
            pl.col("loading").ewm_mean(half_life=60).over("ticker", "factor"),
            pl.col("date").cast(pl.String),
        )
    )


@task
def clean_idio_vol(residuals: pl.DataFrame) -> pl.DataFrame:
    return (
        residuals.drop_nulls()
        .sort("ticker", "date")
        .select(
            "ticker",
            pl.col("date").cast(pl.String),
            pl.col("residual")
            .rolling_std(window_size=WINDOW)
            .ewm_mean(half_life=60)
            .over("ticker")
            .alias("idio_vol"),
        )
    )


@task
def upload_and_merge_factor_loadings(factor_loadings: pl.DataFrame) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()
    table_name = "factor_loadings"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ticker String,
            date String,
            factor String,
            loading Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, date, factor)
        """
    )

    # Insert into table in batches of 1,000,000 rows
    batch_size = 1_000_000
    total_rows = len(factor_loadings)

    for i in tqdm(
        range(0, total_rows, batch_size), desc="Inserting batches", disable=DISABLE_TQDM
    ):
        batch = factor_loadings.slice(i, batch_size)
        clickhouse_client.insert_df_arrow(table_name, batch)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


@task
def upload_and_merge_idio_vol(idio_vol: pl.DataFrame) -> pl.DataFrame:
    clickhouse_client = get_clickhouse_client()
    table_name = "idio_vol"

    # Create table if not exists
    clickhouse_client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ticker String,
            date String,
            idio_vol Float64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (ticker, date)
        """
    )

    # Insert into table in batches of 1,000,000 rows
    batch_size = 1_000_000
    total_rows = len(idio_vol)

    for i in tqdm(
        range(0, total_rows, batch_size), desc="Inserting batches", disable=DISABLE_TQDM
    ):
        batch = idio_vol.slice(i, batch_size)
        clickhouse_client.insert_df_arrow(table_name, batch)

    # Optimize table (deduplicate)
    clickhouse_client.command(f"OPTIMIZE TABLE {table_name} FINAL")


@flow
def factor_model_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date(2025, 12, 29)

    stock_returns = get_stock_returns(start, end)
    etf_returns = get_etf_returns(start, end)

    betas, residuals = estimate_regression(stock_returns, etf_returns)

    factor_loadings = clean_factor_loadings(betas)
    idio_vol = clean_idio_vol(residuals)

    upload_and_merge_factor_loadings(factor_loadings)
    upload_and_merge_idio_vol(idio_vol)


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
def factor_model_daily_flow():
    date_range = get_trading_date_range(window=WINDOW * 2)

    start = date_range["date"].min()
    end = date_range["date"].max()

    yesterday = dt.date.today() - dt.timedelta(days=1)

    # Only get new data if yesterday was the last market date
    if end != yesterday:
        return

    stock_returns = get_stock_returns(start, end)
    etf_returns = get_etf_returns(start, end)

    betas, residuals = estimate_regression(stock_returns, etf_returns)

    factor_loadings = clean_factor_loadings(betas).filter(pl.col("date").eq(str(end)))
    idio_vol = clean_idio_vol(residuals).filter(pl.col("date").eq(str(end)))

    upload_and_merge_factor_loadings(factor_loadings)
    upload_and_merge_idio_vol(idio_vol)
