import pandas as pd
import polars as pl
import requests
import os
import io
from dotenv import load_dotenv
from prefect import task, flow
from clients import get_clickhouse_client

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
    clickhouse_client = get_clickhouse_client()

    calendar_arrow = clickhouse_client.query_arrow("SELECT * FROM calendar")

    calendar_df = (
        pl.from_arrow(calendar_arrow)
        .with_columns(
            pl.col('date').str.strptime(pl.Date, "%Y-%m-%d")
        )
    )

    return calendar_df
@task
def clean_current_constituents_df(current_constituents_df: pd.DataFrame) -> pl.DataFrame:
    return (
        pl.from_pandas(current_constituents_df)
        .rename(
            {
                col: col.replace(" ", "_").replace("-", "").lower()
                for col in current_constituents_df.columns
            }
        )
        .rename({'symbol': 'ticker'})
        .with_columns(
            pl.col("date_added").str.strptime(pl.Date, "%Y-%m-%d"), pl.col("cik").cast(pl.String)
        )
        .drop("founded")
        .drop_nulls('ticker')
    )
@task
def clean_constituent_changes_df(constituent_changes_df: pd.DataFrame) -> pl.DataFrame:
    # Split into Added and Removed dataframes
    added_df = constituent_changes_df[['Effective Date', 'Added', 'Reason']].copy()
    added_df.columns = added_df.columns.droplevel(0)  # Remove first level
    added_df.columns = ['Effective Date', 'Ticker', 'Security', 'Reason']
    added_df['Action'] = 'Added'

    removed_df = constituent_changes_df[['Effective Date', 'Removed', 'Reason']].copy()
    removed_df.columns = removed_df.columns.droplevel(0)
    removed_df.columns = ['Effective Date', 'Ticker', 'Security', 'Reason']
    removed_df['Action'] = 'Removed'

    # Stack them
    stacked_df = pd.concat([added_df, removed_df], ignore_index=True)

    # Clean
    return (
        pl.from_pandas(stacked_df)
        .rename({
            col: col.replace(" ", "_").lower() for col in stacked_df.columns
        })
        .with_columns(
            pl.col('effective_date').str.strptime(pl.Date, "%B %d, %Y")
        )
        .drop_nulls('ticker')
    )

@task
def construct_universe(current_constituents_df: pl.DataFrame, constituent_changes_df: pl.DataFrame, calendar_df: pl.DataFrame) -> pl.DataFrame:
    # Start with current constituents as a set
    constituents = set(current_constituents_df['ticker'].to_list())

    # Get sorted calendar dates (descending)
    calendar_dates = sorted(calendar_df['date'].to_list(), reverse=True)

    # Group changes by date for efficient lookup
    changes_by_date = (
        constituent_changes_df
        .group_by('effective_date')
        .agg([
            pl.col('ticker'),
            pl.col('action')
        ])
    )
    changes_dict = {
        row['effective_date']: {
            'tickers': row['ticker'],
            'actions': row['action']
        }
        for row in changes_by_date.iter_rows(named=True)
    }

    # Store results as we iterate backwards
    snapshots = []

    for date in calendar_dates:
        # Record current state
        snapshots.append({
            'date': date,
            'ticker': sorted(list(constituents)),  # Sort for consistency
        })

        # Apply changes for this date in reverse
        if date in changes_dict:
            tickers = changes_dict[date]['tickers']
            actions = changes_dict[date]['actions']

            for ticker, action in zip(tickers, actions):
                if action == 'Added':
                    # If it was added on this date, remove it (going backwards)
                    constituents.discard(ticker)
                elif action == 'Removed':
                    # If it was removed on this date, add it back (going backwards)
                    constituents.add(ticker)

    universe_df = (
        pl.DataFrame(snapshots)
        .explode('ticker')
        .cast({
            'date': pl.String,
            'ticker': pl.String
        })
        .sort('date', 'ticker')
    )

    return universe_df

@task
def upload_universe_df(universe_df: pl.DataFrame):
    table_name = 'universe'

    # Get clickhouse client
    clickhouse_client = get_clickhouse_client()

    # Drop
    clickhouse_client.command(f"DROP TABLE IF EXISTS {table_name}")

    # Create
    clickhouse_client.command(
        f"""
        CREATE TABLE {table_name} (
            date String,
            ticker String
        )
        ENGINE = MergeTree()
        ORDER BY date
        """
    )

    # Insert in batches to avoid timeout
    batch_size = 1_000_000
    total_rows = len(universe_df)

    for i in range(0, total_rows, batch_size):
        batch_df = universe_df.slice(i, batch_size)
        clickhouse_client.insert_df_arrow(
            table=table_name,
            df=batch_df,
            settings={'insert_quorum': 'auto'}
        )
        print(f"Inserted batch {i//batch_size + 1}: rows {i} to {min(i + batch_size, total_rows)}")

@flow
def universe_backfill_flow():
    current_constituents_df, constituent_changes_df = get_wikipedia_data()
    calendar_df = get_calendar_data()

    current_constituents_df_clean = clean_current_constituents_df(current_constituents_df)
    constituent_changes_df_clean = clean_constituent_changes_df(constituent_changes_df)

    universe_df = construct_universe(current_constituents_df_clean, constituent_changes_df_clean, calendar_df)

    upload_universe_df(universe_df)