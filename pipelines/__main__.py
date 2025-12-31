from calendar_flow import calendar_backfill_flow
from universe_flow import universe_backfill_flow
from stock_prices_flow import stock_prices_backfill_flow, stock_prices_daily_flow
from etf_prices_flow import etf_prices_backfill_flow, etf_prices_daily_flow
from returns_flow import returns_backfill_flow
from prefect import flow, serve
from prefect.schedules import Cron


@flow
def daily_flow():
    calendar_backfill_flow()
    universe_backfill_flow()
    stock_prices_daily_flow()
    etf_prices_daily_flow()
    returns_backfill_flow()


if __name__ == "__main__":
    serve(
        daily_flow.to_deployment(
            name="daily-flow", schedule=Cron("0 2 * * *", timezone="America/Denver")
        ),
        calendar_backfill_flow.to_deployment(name="calendar-backfill-flow"),
        universe_backfill_flow.to_deployment(name="universe-backfill-flow"),
        stock_prices_backfill_flow.to_deployment(name="stock-prices-backfill-flow"),
        etf_prices_backfill_flow.to_deployment(name="etf-prices-backfill-flow"),
        returns_backfill_flow.to_deployment(name="returns-backfill-flow"),
    )
