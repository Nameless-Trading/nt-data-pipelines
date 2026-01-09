import datetime as dt
from pipelines.clients import get_alpaca_trading_client
from alpaca.trading import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus


def get_alpaca_filled_orders(after: dt.datetime):
    alpaca_client = get_alpaca_trading_client()

    current_time = dt.datetime.now()
    filter = GetOrdersRequest(
        status=QueryOrderStatus.CLOSED,
        after=after,
        until=current_time,
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

    return filled_orders
