from benchmark_flow import benchmark_backfill_flow, benchmark_daily_flow
from betas_flow import betas_backfill_flow, betas_daily_flow
from calendar_flow import calendar_backfill_flow
from etf_prices_flow import etf_prices_backfill_flow, etf_prices_daily_flow
from factor_covariances_flow import (factor_covariances_backfill_flow,
                                     factor_covariances_daily_flow)
from factor_model_flow import (factor_model_backfill_flow,
                               factor_model_daily_flow)
from history_flow import etf_history_daily_flow, stock_history_daily_flow
from portfolio_history_flow import portfolio_history_daily_flow
from portfolio_weights_flow import portfolio_weights_daily_flow
from prefect import flow, serve
from prefect.schedules import Cron
from returns_flow import returns_backfill_flow
from reversal_flow import reversal_backfill_flow, reversal_daily_flow
from stock_prices_flow import (stock_prices_backfill_flow,
                               stock_prices_daily_flow)
from trading_flow import trading_daily_flow
from universe_flow import universe_backfill_flow
from utils.slack_failure_handler import create_failure_handler


@flow(on_failure=[create_failure_handler("daily_flow")])
def daily_flow():
    calendar_backfill_flow()
    universe_backfill_flow()  # Depends on calendar
    stock_prices_daily_flow()  # Depends on universe
    etf_prices_daily_flow()  # Depends on calendar
    returns_backfill_flow()  # Depends on stock_prices and etf_prices
    factor_model_daily_flow()  # Depends on stock_returns and etf_returns
    factor_covariances_daily_flow()  # Depends on etf_returns
    reversal_daily_flow()  # Depends on stock_returns and factor_model
    benchmark_daily_flow()  # Depends on stock_returns
    betas_daily_flow()  # Depends on stock_returns and benchmark_returns
    portfolio_weights_daily_flow()  # Depends on everything
    portfolio_history_daily_flow()
    etf_history_daily_flow()
    stock_history_daily_flow()


@flow
def backfill_flow():
    calendar_backfill_flow()
    universe_backfill_flow()  # Depends on calendar
    stock_prices_backfill_flow()  # Depends on universe
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
    )
