import datetime as dt
import time
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal
import polars as pl
from alpaca.trading import GetOrdersRequest, MarketOrderRequest
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from clients import get_alpaca_trading_client
from prefect import flow, get_run_logger, task
from utils import get_portfolio_weights
from utils.slack_daily_summary import send_daily_trading_summary
from utils.slack_failure_handler import create_failure_handler


@task
def get_account_value():
    alpaca_client = get_alpaca_trading_client()
    account = alpaca_client.get_account()
    return float(account.equity)


@task
def get_target_notionals(weights: pl.DataFrame, account_value: float) -> pl.DataFrame:
    return weights.select(
        "ticker",
        pl.col("weight").mul(pl.lit(account_value)).round(2).alias("target_notional"),
    ).sort("target_notional", descending=True)


@task
def get_current_notionals() -> pl.DataFrame:
    alpaca_client = get_alpaca_trading_client()

    positions_raw = alpaca_client.get_all_positions()

    positions_clean = pl.DataFrame(
        {"ticker": position.symbol, "current_notional": float(position.market_value)}
        for position in positions_raw
    ).sort("current_notional", descending=True)

    return positions_clean


@task
def get_notional_deltas(
    target_notionals: pl.DataFrame,
    current_notionals: pl.DataFrame,
    positions_to_close: list[str],
) -> pl.DataFrame:
    return (
        target_notionals.join(other=current_notionals, on="ticker", how="full")
        .select(
            pl.max_horizontal("ticker", "ticker_right").alias("ticker"),
            pl.col("target_notional").fill_null(0),
            pl.col("current_notional").fill_null(0),
        )
        .select(
            "ticker",
            pl.col("target_notional")
            .sub(pl.col("current_notional"))
            .round(2)
            .alias("notional_delta"),
        )
        .filter(
            pl.col("notional_delta").abs().ge(1),
            pl.col("ticker").is_in(positions_to_close).not_(),
        )
        .sort("notional_delta", descending=True)
    )


def get_positions_to_close(
    target_notionals: pl.DataFrame, current_notionals: pl.DataFrame
) -> list[str]:
    return (
        target_notionals.filter(
            pl.col("target_notional").le(0),
            pl.col("ticker").is_in(current_notionals["ticker"].to_list()),
        )["ticker"]
        .unique()
        .sort()
        .to_list()
    )


@task
def get_open_orders() -> pl.DataFrame:
    alpaca_client = get_alpaca_trading_client()

    filter = GetOrdersRequest(status=QueryOrderStatus.OPEN)

    orders_raw = alpaca_client.get_orders(filter)

    return orders_raw


@task
def cancel_all_orders():
    alpaca_client = get_alpaca_trading_client()

    alpaca_client.cancel_orders()


@task
def place_order(ticker: str, notional_delta: float):
    alpaca_client = get_alpaca_trading_client()

    side = OrderSide.SELL if notional_delta < 0 else OrderSide.BUY
    notional = abs(notional_delta)

    print(f"Executing {side} @ MKT {notional} of {ticker}")
    order_data = MarketOrderRequest(
        symbol=ticker, notional=notional, side=side, time_in_force=TimeInForce.DAY
    )

    alpaca_client.submit_order(order_data=order_data)


@task
def close_positions(positions_to_close: list[str]):
    for ticker in positions_to_close:
        alpaca_client = get_alpaca_trading_client()

        alpaca_client.close_position(
            symbol_or_asset_id=ticker,
        )


@task
def place_all_orders(notional_deltas: pl.DataFrame):
    for ticker, notional_delta in notional_deltas.iter_rows():
        place_order(ticker, notional_delta)


@task
def wait_for_orders_to_fill(
    max_wait_minutes: int = 10, check_interval_seconds: int = 60
) -> bool:
    """
    Poll until all open orders are filled or max wait time is reached.

    Returns True if all orders filled, False if timed out with orders still open.
    """
    logger = get_run_logger()
    logger.info("Waiting for orders to fill...")

    alpaca_client = get_alpaca_trading_client()
    elapsed_time = 0

    while elapsed_time < max_wait_minutes * 60:
        filter = GetOrdersRequest(status=QueryOrderStatus.OPEN)
        open_orders = alpaca_client.get_orders(filter)

        if len(open_orders) == 0:
            logger.info(f"All orders filled after {elapsed_time} seconds")
            return True

        logger.info(
            f"Still have {len(open_orders)} open orders, waiting {check_interval_seconds}s..."
        )
        time.sleep(check_interval_seconds)
        elapsed_time += check_interval_seconds

    logger.warning(
        f"Reached max wait time of {max_wait_minutes} minutes, some orders may still be open"
    )
    return False


@task
def get_todays_filled_orders() -> list[dict]:
    """
    Get all filled orders for today (since market open).
    """
    logger = get_run_logger()
    alpaca_client = get_alpaca_trading_client()

    # Get today's date at market open (9:30 AM ET)
    today = dt.datetime.now(ZoneInfo("America/New_York")).date()
    market_open = dt.datetime.combine(
        today, dt.time(9, 30), tzinfo=ZoneInfo("America/New_York")
    )

    filter = GetOrdersRequest(
        status=QueryOrderStatus.CLOSED,
        after=market_open,
        until=dt.datetime.now(ZoneInfo("America/New_York")),
    )

    orders = alpaca_client.get_orders(filter)

    filled_orders = []
    for order in orders:
        if (
            order.filled_at is not None
            and order.filled_qty
            and float(order.filled_qty) > 0
        ):
            filled_orders.append(
                {
                    "ticker": order.symbol,
                    "side": order.side.value,
                    "filled_qty": float(order.filled_qty),
                    "filled_avg_price": (
                        float(order.filled_avg_price) if order.filled_avg_price else 0
                    ),
                    "notional": (
                        float(order.filled_qty) * float(order.filled_avg_price)
                        if order.filled_avg_price
                        else 0
                    ),
                    "filled_at": order.filled_at,
                    "order_id": order.id,
                }
            )

    logger.info(f"Found {len(filled_orders)} filled orders for today")
    return filled_orders


@task
def send_fill_status_to_slack(filled_orders: list[dict]):
    """
    Send a Slack notification with the provided filled orders.
    """
    logger = get_run_logger()

    try:
        alpaca_client = get_alpaca_trading_client()
        account = alpaca_client.get_account()
        account_value = float(account.equity)

        if len(filled_orders) > 0:
            send_daily_trading_summary(filled_orders, account_value)
            logger.info("Sent Slack notification for daily trading summary")
        else:
            logger.warning("No filled orders found for today")
    except Exception as e:
        logger.error(
            f"Failed to send Slack notification for daily trading summary: {e}"
        )


@task
def get_last_market_date() -> list[dt.date]:
    nyse = mcal.get_calendar("NYSE")
    today = dt.datetime.now().date()

    # Look back 10 days to ensure we catch the last trading day
    schedule = nyse.schedule(start_date=today - dt.timedelta(days=10), end_date=today)

    # Filter out today and get the last trading day
    valid_dates = schedule.index[schedule.index.date < today]

    return valid_dates[-1].date()


@task
def market_is_open(today: dt.date) -> bool:
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=today, end_date=today)
    return len(schedule) > 0


@flow(on_failure=[create_failure_handler("trading_daily_flow")])
def trading_daily_flow():
    last_trading_date = get_last_market_date()
    today = dt.datetime.now(ZoneInfo("America/New_York")).date()

    if not market_is_open(today):
        print("Market is not open today!")
        print("Ending flow.")
        return

    weights = get_portfolio_weights(last_trading_date, last_trading_date)

    if not len(weights) > 0:
        raise RuntimeError("Portfolio weights appear to not be empty!")

    open_orders = get_open_orders()

    if len(open_orders) > 0:
        cancel_all_orders()

    account_value = get_account_value()
    current_notionals = get_current_notionals()

    target_notionals = get_target_notionals(weights, account_value)

    positions_to_close = get_positions_to_close(target_notionals, current_notionals)

    notional_deltas = get_notional_deltas(
        target_notionals, current_notionals, positions_to_close
    )

    close_positions(positions_to_close)
    place_all_orders(notional_deltas)

    wait_for_orders_to_fill()
    filled_orders = get_todays_filled_orders()
    send_fill_status_to_slack(filled_orders)
