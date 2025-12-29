import polars as pl
import datetime as dt

# Load data
current_constituents = pl.read_parquet("current_constituents.parquet").filter(pl.col('ticker').is_not_null())
constituent_changes = pl.read_parquet("constituent_changes.parquet").filter(pl.col('ticker').is_not_null())
calendar = pl.read_parquet("calendar.parquet")

# Start with current constituents as a set
constituents = set(current_constituents['ticker'].to_list())

# Get sorted calendar dates (descending)
calendar_dates = sorted(calendar['date'].to_list(), reverse=True)

# Group changes by date for efficient lookup
changes_by_date = (
    constituent_changes
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

universe = pl.DataFrame(snapshots).explode('ticker').sort('date', 'ticker')

print(universe)