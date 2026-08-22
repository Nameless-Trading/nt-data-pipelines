import datetime as dt

import polars as pl
from prefect import flow, task
from utils import (calculate_alphas, calculate_scores, get_idio_vol,
                   get_stock_returns, get_trading_date_range,
                   upload_and_merge_alphas, upload_and_merge_scores,
                   upload_and_merge_signals)
from variables import MOMENTUM_LAG, MOMENTUM_WINDOW

SIGNAL_NAME = "momentum"

# Trading days of history needed to observe the signal on a given date
LOOKBACK = MOMENTUM_WINDOW + MOMENTUM_LAG


@task
def calculate_signals(stock_returns: pl.DataFrame) -> pl.DataFrame:
    # 12-1 momentum: 11-month cumulative log return, lagged 1 month.
    return (
        stock_returns.sort("ticker", "date")
        .select(
            "ticker",
            "date",
            pl.col("date").dt.year().alias("year"),
            pl.lit(SIGNAL_NAME).alias("signal"),
            pl.col("return")
            .log1p()
            .rolling_sum(MOMENTUM_WINDOW)
            .shift(MOMENTUM_LAG)
            .over("ticker")
            .alias("value"),
        )
        .drop_nulls()
        .sort("ticker", "date")
    )


@flow
def momentum_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)

    stock_returns = get_stock_returns(start, end)
    idio_vol = get_idio_vol(start, end)

    signals = calculate_signals(stock_returns)
    scores = calculate_scores(signals, SIGNAL_NAME)
    alphas = calculate_alphas(scores, idio_vol, SIGNAL_NAME)

    upload_and_merge_signals(signals)
    upload_and_merge_scores(scores)
    upload_and_merge_alphas(alphas)


@flow
def momentum_daily_flow():
    date_range = get_trading_date_range(window=LOOKBACK)

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
    idio_vol = get_idio_vol(start, end)

    signals = calculate_signals(stock_returns).filter(pl.col("date").eq(end))
    scores = calculate_scores(signals, SIGNAL_NAME).filter(pl.col("date").eq(end))
    alphas = calculate_alphas(scores, idio_vol, SIGNAL_NAME).filter(
        pl.col("date").eq(end)
    )

    if not (len(signals) > 0 and len(scores) > 0 and len(alphas) > 0):
        raise ValueError("No values found!")

    upload_and_merge_signals(signals)
    upload_and_merge_scores(scores)
    upload_and_merge_alphas(alphas)
