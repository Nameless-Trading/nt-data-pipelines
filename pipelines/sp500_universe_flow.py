import pandas as pd
import polars as pl
import requests
import os
import io
from dotenv import load_dotenv

load_dotenv()

wikipedia_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
headers = {"User-Agent": os.getenv("WIKIPEDIA_USER_AGENT")}

response = requests.get(wikipedia_url, headers=headers)
response.raise_for_status()

# Read the HTML tables from Wikipedia (first table contains S&P 500 companies)
[current_constituents_df, constituent_changes_df] = pd.read_html(
    io.StringIO(response.text)
)

current_constituents_clean_df = (
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
)
current_constituents_clean_df.write_parquet("current_constituents.parquet")

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
constituent_changes_clean_df = (
    pl.from_pandas(stacked_df)
    .rename({
        col: col.replace(" ", "_").lower() for col in stacked_df.columns
    })
    .with_columns(
        pl.col('effective_date').str.strptime(pl.Date, "%B %d, %Y")
    )
)

constituent_changes_clean_df.write_parquet("constituent_changes.parquet")