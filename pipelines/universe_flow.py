import pandas as pd
import polars as pl
import requests
import os
import io
from dotenv import load_dotenv
from prefect import task, flow
from clients import get_bear_lake_client
import bear_lake as bl

load_dotenv()


@task
def get_wikipedia_data() -> tuple[pd.DataFrame]:
    # Get Wikipedia user agent
    wikipedia_user_agent = os.getenv("WIKIPEDIA_USER_AGENT")

    if not wikipedia_user_agent:
        raise ValueError("WIKIPEDIA_USER_AGENT not set!")

    # Request Wikipedia tables
    wikipedia_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": wikipedia_user_agent}

    response = requests.get(wikipedia_url, headers=headers)
    response.raise_for_status()

    # Read the HTML tables from Wikipedia
    [current_constituents_df, constituent_changes_df] = pd.read_html(
        io.StringIO(response.text)
    )

    return current_constituents_df, constituent_changes_df


@task
def get_calendar_data() -> pl.DataFrame:
    bear_lake_client = get_bear_lake_client()
    return bear_lake_client.query(bl.table("calendar"))


@task
def clean_current_constituents_df(
    current_constituents_df: pd.DataFrame,
) -> pl.DataFrame:
    return (
        pl.from_pandas(current_constituents_df)
        .select(pl.col("Symbol").alias("ticker"))
        .drop_nulls("ticker")
        .sort("ticker")
    )


@task
def clean_constituent_changes_df(constituent_changes_df: pd.DataFrame) -> pl.DataFrame:
    # Split into Added and Removed dataframes
    added_df = constituent_changes_df[["Effective Date", "Added", "Reason"]].copy()
    added_df.columns = added_df.columns.droplevel(0)  # Remove first level
    added_df.columns = ["Effective Date", "Ticker", "Security", "Reason"]
    added_df["Action"] = "Added"

    removed_df = constituent_changes_df[["Effective Date", "Removed", "Reason"]].copy()
    removed_df.columns = removed_df.columns.droplevel(0)
    removed_df.columns = ["Effective Date", "Ticker", "Security", "Reason"]
    removed_df["Action"] = "Removed"

    # Stack them
    stacked_df = pd.concat([added_df, removed_df], ignore_index=True)

    # Clean
    return (
        pl.from_pandas(stacked_df)
        .rename({col: col.replace(" ", "_").lower() for col in stacked_df.columns})
        .with_columns(pl.col("effective_date").str.strptime(pl.Date, "%B %d, %Y"))
        .drop_nulls("ticker")
    )


@task
def construct_universe(
    current_constituents_df: pl.DataFrame,
    constituent_changes_df: pl.DataFrame,
    calendar_df: pl.DataFrame,
) -> pl.DataFrame:
    # Start with current constituents as a set
    constituents = set(current_constituents_df["ticker"].to_list())

    # Get sorted calendar dates (descending)
    calendar_dates = sorted(calendar_df["date"].to_list(), reverse=True)

    # Group changes by date for efficient lookup
    changes_by_date = constituent_changes_df.group_by("effective_date").agg(
        [pl.col("ticker"), pl.col("action")]
    )
    changes_dict = {
        row["effective_date"]: {"tickers": row["ticker"], "actions": row["action"]}
        for row in changes_by_date.iter_rows(named=True)
    }

    # Store results as we iterate backwards
    snapshots = []

    for date in calendar_dates:
        # Record current state
        snapshots.append(
            {
                "date": date,
                "ticker": sorted(list(constituents)),  # Sort for consistency
            }
        )

        # Apply changes for this date in reverse
        if date in changes_dict:
            tickers = changes_dict[date]["tickers"]
            actions = changes_dict[date]["actions"]

            for ticker, action in zip(tickers, actions):
                if action == "Added":
                    # If it was added on this date, remove it (going backwards)
                    constituents.discard(ticker)
                elif action == "Removed":
                    # If it was removed on this date, add it back (going backwards)
                    constituents.add(ticker)

    universe_df = (
        pl.DataFrame(snapshots)
        .explode("ticker")
        .with_columns(pl.col("date").dt.year().alias("year"))
        .sort("date", "ticker")
    )

    return universe_df


@task
def upload_universe_df(universe_df: pl.DataFrame):
    table_name = "universe"

    # Get ClickHouse client
    bear_lake_client = get_bear_lake_client()

    # Create
    bear_lake_client.create(
        name=table_name,
        schema={"date": pl.Date, "year": pl.Int32, "ticker": pl.String},
        partition_keys=["year"],
        primary_keys=["date", "ticker"],
        mode="replace",
    )

    # Insert
    bear_lake_client.insert(name=table_name, data=universe_df, mode="append")


@flow
def universe_backfill_flow():
    current_constituents_df, constituent_changes_df = get_wikipedia_data()
    calendar_df = get_calendar_data()

    current_constituents_df_clean = clean_current_constituents_df(
        current_constituents_df
    )
    constituent_changes_df_clean = clean_constituent_changes_df(constituent_changes_df)

    universe_df = construct_universe(
        current_constituents_df_clean, constituent_changes_df_clean, calendar_df
    )

    upload_universe_df(universe_df)
