import datetime as dt

import polars as pl
from prefect import flow, task
from utils import (get_last_market_date, get_signal_alphas,
                   upload_and_merge_alphas)
from variables import COMPOSITE_WEIGHTS, PRODUCTION_SIGNAL

SIGNAL_NAME = PRODUCTION_SIGNAL


@task
def calculate_composite_alphas(
    component_alphas: dict[str, pl.DataFrame],
) -> pl.DataFrame:
    # Weighted blend of the component alphas on the shared ticker/date grid.
    # An inner join keeps only dates where every component is available.
    blended = None
    for signal_name, alphas in component_alphas.items():
        weight = COMPOSITE_WEIGHTS[signal_name]
        contribution = alphas.select(
            "ticker",
            "date",
            pl.col("alpha").mul(weight).alias("alpha"),
        )
        if blended is None:
            blended = contribution
        else:
            blended = blended.join(
                contribution, on=["ticker", "date"], how="inner", suffix="_other"
            ).select(
                "ticker",
                "date",
                pl.col("alpha").add(pl.col("alpha_other")).alias("alpha"),
            )

    return blended.select(
        "ticker",
        "date",
        pl.col("date").dt.year().alias("year"),
        pl.lit(SIGNAL_NAME).alias("signal"),
        "alpha",
    ).sort("ticker", "date")


@flow
def composite_backfill_flow():
    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)

    component_alphas = {
        signal_name: get_signal_alphas(start, end, signal_name)
        for signal_name in COMPOSITE_WEIGHTS
    }

    alphas = calculate_composite_alphas(component_alphas)

    upload_and_merge_alphas(alphas)


@flow
def composite_daily_flow():
    last_market_date = get_last_market_date()
    yesterday = dt.date.today() - dt.timedelta(days=1)

    # Only get new data if yesterday was the last market date
    if last_market_date != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", last_market_date)
        print("Yesterday:", yesterday)
        return

    component_alphas = {
        signal_name: get_signal_alphas(last_market_date, last_market_date, signal_name)
        for signal_name in COMPOSITE_WEIGHTS
    }

    alphas = calculate_composite_alphas(component_alphas)

    if len(alphas) == 0:
        raise ValueError("No values found!")

    upload_and_merge_alphas(alphas)
