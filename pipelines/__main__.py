from calendar_flow import calendar_backfill_flow
from universe_flow import universe_backfill_flow
from stock_prices_flow import stock_prices_backfill_flow, stock_prices_daily_flow

# from stock_prices_yfinance_flow import (
#     stock_prices_yfinance_backfill_flow,
#     stock_prices_yfinance_daily_flow,
# )
from etf_prices_flow import etf_prices_backfill_flow, etf_prices_daily_flow
from returns_flow import returns_backfill_flow
from factor_model_flow import factor_model_backfill_flow, factor_model_daily_flow
from factor_covariances_flow import (
    factor_covariances_backfill_flow,
    factor_covariances_daily_flow,
)
from reversal_flow import reversal_backfill_flow, reversal_daily_flow
from benchmark_flow import benchmark_backfill_flow, benchmark_daily_flow
from betas_flow import betas_backfill_flow, betas_daily_flow
from portfolio_weights_flow import (
    portfolio_weights_daily_flow,
)
from trading_flow import trading_daily_flow
from prefect import flow, serve
from prefect.schedules import Cron


@flow
def daily_flow():
    calendar_backfill_flow()
    universe_backfill_flow()  # Depends on calendar
    stock_prices_daily_flow()  # Depends on universe
    # stock_prices_yfinance_daily_flow()  # Depends on universe
    etf_prices_daily_flow()  # Depends on calendar
    returns_backfill_flow()  # Depends on stock_prices and etf_prices
    factor_model_daily_flow()  # Depends on stock_returns and etf_returns
    factor_covariances_daily_flow()  # Depends on etf_returns
    reversal_daily_flow()  # Depends on stock_returns and factor_model
    benchmark_daily_flow()  # Depends on stock_returns
    betas_daily_flow()  # Depends on stock_returns and benchmark_returns
    portfolio_weights_daily_flow()  # Depends on everything


@flow
def backfill_flow():
    calendar_backfill_flow()
    universe_backfill_flow()  # Depends on calendar
    stock_prices_backfill_flow()  # Depends on universe
    # stock_prices_yfinance_backfill_flow()  # Depends on universe
    etf_prices_backfill_flow()  # Depends on calendar
    returns_backfill_flow()  # Depends on stock_prices and etf_prices
    factor_model_backfill_flow()  # Depends on stock_returns and etf_returns
    factor_covariances_backfill_flow()  # Depends on etf_returns
    reversal_backfill_flow()  # Depends on stock_returns and factor_model
    benchmark_backfill_flow()  # Depends on stock_returns
    betas_backfill_flow()  # Depends on stock_returns and benchmark_returns


if __name__ == "__main__":
    serve(
        daily_flow.to_deployment(
            name="daily-flow", schedule=Cron("0 2 * * *", timezone="America/Denver")
        ),
        trading_daily_flow.to_deployment(
            name="trading-daily-flow",
            schedule=Cron("30 7 * * *", timezone="America/Denver"),
        ),
        backfill_flow.to_deployment(name="backfill-flow"),
        calendar_backfill_flow.to_deployment(name="calendar-backfill-flow"),
        universe_backfill_flow.to_deployment(name="universe-backfill-flow"),
        stock_prices_backfill_flow.to_deployment(name="stock-prices-backfill-flow"),
        # stock_prices_yfinance_backfill_flow.to_deployment(
        #     name="stock-prices-yfinance-backfill-flow"
        # ),
        etf_prices_backfill_flow.to_deployment(name="etf-prices-backfill-flow"),
        returns_backfill_flow.to_deployment(name="returns-backfill-flow"),
        factor_model_backfill_flow.to_deployment(name="factor-model-backfill-flow"),
        factor_covariances_backfill_flow.to_deployment(
            name="factor-covariances-backfill-flow"
        ),
        reversal_backfill_flow.to_deployment(name="reversal-backfill-flow"),
        benchmark_backfill_flow.to_deployment(name="benchmark-backfill-flow"),
        betas_backfill_flow.to_deployment(name="betas-backfill-flow"),
    )
