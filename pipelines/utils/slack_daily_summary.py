"""Daily trading summary for Slack."""

import os
from typing import Optional

from clients import get_alpaca_trading_client, get_slack_client
from slack_sdk.errors import SlackApiError


def get_current_positions() -> list[dict]:
    """Get current positions with ticker and value."""
    alpaca_client = get_alpaca_trading_client()
    positions = alpaca_client.get_all_positions()

    position_list = [
        {
            "ticker": pos.symbol,
            "value": float(pos.market_value) if pos.market_value else 0,
        }
        for pos in positions
    ]

    # Sort by value descending
    return sorted(position_list, key=lambda x: x["value"], reverse=True)


def categorize_trades(filled_orders: list[dict]) -> dict:
    """Categorize trades into buys and sells, find top trades by value."""
    buys = [o for o in filled_orders if o["side"] == "buy"]
    sells = [o for o in filled_orders if o["side"] == "sell"]

    # Get top 3 buys and sells by notional
    top_buys = sorted(buys, key=lambda x: x["notional"], reverse=True)[:3]
    top_sells = sorted(sells, key=lambda x: x["notional"], reverse=True)[:3]

    return {
        "buys": buys,
        "sells": sells,
        "top_buys": top_buys,
        "top_sells": top_sells,
        "total_buys_notional": sum(o["notional"] for o in buys),
        "total_sells_notional": sum(o["notional"] for o in sells),
        "total_notional": sum(o["notional"] for o in filled_orders),
    }


def send_daily_trading_summary(
    filled_orders: list[dict],
    account_value: float,
    previous_account_value: Optional[float] = None,
) -> None:
    """Send enhanced daily trading summary to Slack."""
    client = get_slack_client()
    channel = os.getenv("SLACK_CHANNEL")

    if not channel:
        raise RuntimeError("SLACK_CHANNEL environment variable not set")

    if not filled_orders:
        message = {
            "channel": channel,
            "text": "✅ No trades executed today",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "✅ *No trades executed today*\n\nPortfolio value: $"
                        + f"{account_value:,.2f}",
                    },
                },
            ],
        }
        try:
            client.chat_postMessage(**message)
        except SlackApiError as e:
            raise RuntimeError(f"Error sending Slack message: {e.response['error']}")
        return

    # Get positions and categorize trades
    positions = get_current_positions()
    top_5_positions = positions[:5]
    trade_summary = categorize_trades(filled_orders)

    # Calculate day P&L
    day_pnl = account_value - (previous_account_value or account_value)
    day_pnl_pct = (
        (day_pnl / previous_account_value * 100) if previous_account_value else 0
    )

    # Build blocks
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "📊 Daily Trading Summary"},
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Portfolio Value*\n${account_value:,.2f}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Day P&L*\n${day_pnl:,.2f} ({day_pnl_pct:+.2f}%)"
                    if previous_account_value
                    else f"*Trades Executed*\n{len(filled_orders)}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Total Volume*\n${trade_summary['total_notional']:,.2f}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Positions*\n{len(positions)} open",
                },
            ],
        },
        {"type": "divider"},
    ]

    # Trade summary section
    trade_lines = []
    if trade_summary["buys"]:
        trade_lines.append(
            f"*Buys:* {len(trade_summary['buys'])} · ${trade_summary['total_buys_notional']:,.2f}"
        )
    if trade_summary["sells"]:
        trade_lines.append(
            f"*Sells:* {len(trade_summary['sells'])} · ${trade_summary['total_sells_notional']:,.2f}"
        )

    if trade_lines:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(trade_lines)},
            }
        )
        blocks.append({"type": "divider"})

    # Top 3 Buys section
    if trade_summary["top_buys"]:
        buy_lines = []
        for i, trade in enumerate(trade_summary["top_buys"], 1):
            buy_lines.append(
                f"{i}. {trade['filled_qty']:.2f} `{trade['ticker']}` @ ${trade['filled_avg_price']:.2f} = ${trade['notional']:,.2f}"
            )

        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Top 3 Buys*\n" + "\n".join(buy_lines),
                },
            }
        )

    # Top 3 Sells section
    if trade_summary["top_sells"]:
        sell_lines = []
        for i, trade in enumerate(trade_summary["top_sells"], 1):
            sell_lines.append(
                f"{i}. {trade['filled_qty']:.2f} `{trade['ticker']}` @ ${trade['filled_avg_price']:.2f} = ${trade['notional']:,.2f}"
            )

        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Top 3 Sells*\n" + "\n".join(sell_lines),
                },
            }
        )

    # Top 5 positions section
    if top_5_positions:
        position_lines = [f"*Top {min(5, len(positions))} Positions*"]
        for i, pos in enumerate(top_5_positions, 1):
            position_lines.append(f"{i}. `{pos['ticker']}`: ${pos['value']:,.2f}")

        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(position_lines)},
            }
        )

    message = {
        "channel": channel,
        "text": "📊 Daily Trading Summary",
        "blocks": blocks,
    }

    try:
        client.chat_postMessage(**message)
    except SlackApiError as e:
        raise RuntimeError(f"Error sending Slack message: {e.response['error']}")
