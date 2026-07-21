import datetime as dt
import time
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal
import polars as pl
from alpaca.trading import GetOrdersRequest, MarketOrderRequest
from alpaca.trading.enums import (OrderSide, OrderStatus, QueryOrderStatus,
                                  TimeInForce)
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
    return weights.with_columns(
        pl.col('weight').clip(lower_bound=0)
    ).select(
        "ticker",
        pl.col("weight").mul(pl.lit(account_value)).round(2).alias("target_notional"),
    ).sort("target_notional", descending=True)


@task
def get_current_notionals() -> pl.DataFrame:
    alpaca_client = get_alpaca_trading_client()

    positions_raw = alpaca_client.get_all_positions()

    positions_clean = pl.DataFrame(
        [
            {
                "ticker": position.symbol,
                "current_notional": float(position.market_value or 0),
            }
            for position in positions_raw
        ],
        schema={"ticker": pl.String, "current_notional": pl.Float64},
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
def place_order(ticker: str, notional_delta: float) -> str:
    alpaca_client = get_alpaca_trading_client()

    side = OrderSide.SELL if notional_delta < 0 else OrderSide.BUY
    notional = abs(notional_delta)

    print(f"Executing {side} @ MKT {notional} of {ticker}")
    order_data = MarketOrderRequest(
        symbol=ticker, notional=notional, side=side, time_in_force=TimeInForce.DAY
    )

    order = alpaca_client.submit_order(order_data=order_data)
    return str(order.id)


@task
def close_positions(positions_to_close: list[str]) -> list[str]:
    order_ids = []
    for ticker in positions_to_close:
        alpaca_client = get_alpaca_trading_client()

        order = alpaca_client.close_position(
            symbol_or_asset_id=ticker,
        )
        order_ids.append(str(order.id))

    return order_ids


@task
def place_all_orders(notional_deltas: pl.DataFrame) -> list[str]:
    order_ids = []
    for ticker, notional_delta in notional_deltas.iter_rows():
        order_ids.append(place_order(ticker, notional_delta))

    return order_ids


@task
def wait_for_orders_to_fill(
    order_ids: list[str],
    max_wait_minutes: int = 10,
    check_interval_seconds: int = 15,
    initial_delay_seconds: int = 10,
) -> bool:
    """
    Poll the specific orders we submitted until every one reaches a terminal
    state (filled / canceled / rejected / expired), or ``max_wait_minutes`` is
    reached.

    We track the exact order IDs rather than counting how many orders show up as
    OPEN vs CLOSED. Right after submission Alpaca reports new orders as
    ``pending_new`` / ``accepted`` (not OPEN), and the buys are funded by the
    proceeds of the sells so they routinely fill later. Counting OPEN orders let
    the daily summary snapshot a partial rebalance: once the sells cleared OPEN
    but before the buys appeared there, a "0 open orders" reading fired and the
    summary showed far fewer buys than sells. Watching each submitted order until
    it is terminal removes that race entirely.

    Returns True if all tracked orders reached a terminal state, False on timeout.
    """
    logger = get_run_logger()

    if not order_ids:
        logger.info("No orders to wait for")
        return True

    logger.info(f"Waiting for {len(order_ids)} orders to reach a terminal state...")

    alpaca_client = get_alpaca_trading_client()

    terminal_states = {
        OrderStatus.FILLED,
        OrderStatus.CANCELED,
        OrderStatus.EXPIRED,
        OrderStatus.REJECTED,
        OrderStatus.REPLACED,
        OrderStatus.DONE_FOR_DAY,
    }

    # Give just-submitted orders time to register before the first poll.
    time.sleep(initial_delay_seconds)
    elapsed_time = initial_delay_seconds

    while True:
        pending = []
        filled_count = 0
        for order_id in order_ids:
            order = alpaca_client.get_order_by_id(order_id)
            if order.status in terminal_states:
                if order.status == OrderStatus.FILLED:
                    filled_count += 1
            else:
                pending.append(order_id)

        if not pending:
            logger.info(
                f"All {len(order_ids)} orders terminal ({filled_count} filled) "
                f"after {elapsed_time} seconds"
            )
            return True

        if elapsed_time >= max_wait_minutes * 60:
            logger.warning(
                f"Reached max wait time of {max_wait_minutes} minutes with "
                f"{len(pending)} orders still not terminal "
                f"({filled_count}/{len(order_ids)} filled)"
            )
            return False

        logger.info(
            f"{filled_count}/{len(order_ids)} filled, {len(pending)} still pending, "
            f"waiting {check_interval_seconds}s..."
        )
        time.sleep(check_interval_seconds)
        elapsed_time += check_interval_seconds


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

    if len(weights) == 0:
        raise RuntimeError(
            f"No portfolio weights found for {last_trading_date}. "
            "The daily flow likely did not generate weights for that date."
        )

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

    close_order_ids = close_positions(positions_to_close)
    order_ids = place_all_orders(notional_deltas)

    wait_for_orders_to_fill(close_order_ids + order_ids)
    filled_orders = get_todays_filled_orders()
    send_fill_status_to_slack(filled_orders)
