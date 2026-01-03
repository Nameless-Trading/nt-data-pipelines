from utils import get_portfolio_weights
import datetime as dt
import polars as pl
from clients import get_alpaca_trading_client
from alpaca.trading import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from prefect import task, flow


@task
def get_account_value():
    alpaca_client = get_alpaca_trading_client()
    account = alpaca_client.get_account()
    return float(account.equity)


@task
def get_notionals(weights: pl.DataFrame, account_value: float) -> pl.DataFrame:
    return (
        weights.select(
            "ticker", pl.col("weight").mul(pl.lit(account_value)).alias("notional")
        )
        .filter(pl.col("notional").ge(1))
        .sort("notional", descending=True)
    )


@task
def place_order(ticker: str, notional_delta: float):
    alpaca_client = get_alpaca_trading_client()

    side = OrderSide.SELL if notional_delta < 0 else OrderSide.BUY
    notional = abs(notional_delta)

    order_data = MarketOrderRequest(
        symbol=ticker, notional=notional, side=side, time_in_force=TimeInForce.DAY
    )

    alpaca_client.submit_order(order_data=order_data)


@task
def place_all_orders(notional_deltas: pl.DataFrame):
    for ticker, notional_delta in notional_deltas.iter_rows():
        place_order(ticker, notional_delta)


@flow
def trading_daily_flow():
    yesterday = dt.date.today() - dt.timedelta(days=1)

    account_value = get_account_value()
    weights = get_portfolio_weights(yesterday, yesterday)
    notionals = get_notionals(weights, account_value)

    # TODO: Compute notional deltas
    # TODO: Filter out notional deltas < $1

    place_all_orders(notionals)


if __name__ == "__main__":
    trading_daily_flow()
