"""
Test script for yfinance backfill flow.
This script tests the yfinance backfill with a small subset of tickers and a short date range.
"""

import sys
from pathlib import Path

# Add pipelines directory to path so imports work
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "pipelines"))

import datetime as dt
from stock_prices_flow import (
    get_stock_prices_yfinance,
    get_stock_prices_yfinance_batches,
)
from variables import TIME_ZONE


def test_yfinance_single_ticker():
    print("Testing yfinance with single ticker (AAPL)...")

    start = dt.datetime(2015, 1, 1, tzinfo=TIME_ZONE)
    end = dt.datetime(2015, 12, 31, 23, 59, 59, tzinfo=TIME_ZONE)

    tickers = ["AAPL"]
    result = get_stock_prices_yfinance(tickers, start, end)

    print(f"Fetched {len(result)} rows")
    print("\nSample data:")
    print(result.head())
    print("\nSchema:")
    print(result.schema)
    print("\nNull counts:")
    print(result.null_count())

    return result


def test_yfinance_multiple_tickers():
    print("\nTesting yfinance with multiple tickers...")

    start = dt.datetime(2015, 1, 1, tzinfo=TIME_ZONE)
    end = dt.datetime(2015, 12, 31, 23, 59, 59, tzinfo=TIME_ZONE)

    tickers = ["AAPL", "MSFT", "GOOGL"]
    result = get_stock_prices_yfinance_batches(tickers, start, end)

    print(f"Fetched {len(result)} rows")
    print("\nSample data:")
    print(result.head(10))
    print("\nUnique tickers:")
    print(result["ticker"].unique().to_list())

    return result


def test_yfinance_old_data():
    print("\nTesting yfinance with older data (1990)...")

    start = dt.datetime(1990, 1, 1, tzinfo=TIME_ZONE)
    end = dt.datetime(1990, 12, 31, 23, 59, 59, tzinfo=TIME_ZONE)

    tickers = ["AAPL"]
    result = get_stock_prices_yfinance(tickers, start, end)

    print(f"Fetched {len(result)} rows")
    if len(result) > 0:
        print("\nDate range:")
        print(f"Earliest: {result['date'].min()}")
        print(f"Latest: {result['date'].max()}")
        print("\nSample data:")
        print(result.head())
    return result


def test_without_upload():
    print("\nTesting full yfinance backfill flow (without upload)...")

    start = dt.datetime(2015, 1, 1, tzinfo=TIME_ZONE)
    end = dt.datetime(2015, 12, 31, 23, 59, 59, tzinfo=TIME_ZONE)

    tickers = ["AAPL", "MSFT", "GOOGL"]
    result = get_stock_prices_yfinance_batches(tickers, start, end)

    print(f"Total rows: {len(result)}")
    print("\nData summary:")
    print(result.describe())

    expected_columns = [
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_count",
        "vwap",
    ]
    assert (
        list(result.columns) == expected_columns
    ), f"Columns don't match! Got: {result.columns}"

    assert result["vwap"].null_count() == len(result), "vwap should be all null"
    assert result["trade_count"].null_count() == len(
        result
    ), "trade_count should be all null"

    print("\n✓ Schema validation passed!")
    print("✓ vwap and trade_count are null as expected!")

    return result


if __name__ == "__main__":
    print("=" * 60)
    print("Testing yfinance backfill functionality")
    print("=" * 60)

    test_yfinance_single_ticker()
    test_yfinance_multiple_tickers()
    test_yfinance_old_data()
    test_without_upload()

    print("\n" + "=" * 60)
    print("All tests completed!")
    print("=" * 60)
    print("\nTo test with actual upload to ClickHouse, run:")
    print("  python -m pipelines.stock_prices_flow")
    print("\nOr run the Prefect flow directly:")
    print(
        "  from pipelines.stock_prices_flow import stock_prices_yfinance_backfill_flow"
    )
    print("  stock_prices_yfinance_backfill_flow()")
