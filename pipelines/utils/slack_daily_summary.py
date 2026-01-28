"""Enhanced daily trading summary for Slack."""

import os
from typing import Optional

from alpaca.trading import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
from clients import get_alpaca_trading_client, get_slack_client
from slack_sdk.errors import SlackApiError


def get_current_positions_with_weights(account_value: float) -> list[dict]:
    """Get current positions with calculated weights and values."""
    alpaca_client = get_alpaca_trading_client()
    positions = alpaca_client.get_all_positions()
    
    position_list = []
    for pos in positions:
        # All Alpaca Position fields are strings, convert to float
        qty = float(pos.qty) if pos.qty else 0
        cost_basis = float(pos.cost_basis) if pos.cost_basis else 0
        market_value = float(pos.market_value) if pos.market_value else 0
        current_price = float(pos.current_price) if pos.current_price else 0
        unrealized_pl = float(pos.unrealized_pl) if pos.unrealized_pl else 0
        unrealized_plpc = float(pos.unrealized_plpc) if pos.unrealized_plpc else 0
        
        # Calculate entry price from cost basis / qty
        entry_price = cost_basis / qty if qty > 0 else 0
        
        position_list.append({
            "ticker": pos.symbol,
            "qty": qty,
            "value": market_value,
            "weight": market_value / account_value if account_value > 0 else 0,
            "entry_price": entry_price,
            "current_price": current_price,
            "unrealized_pl": unrealized_pl,
            "unrealized_plpc": unrealized_plpc,
        })
    
    # Sort by value descending
    return sorted(position_list, key=lambda x: x["value"], reverse=True)


def categorize_trades(filled_orders: list[dict]) -> dict:
    """Categorize trades into buys and sells, find largest trades."""
    buys = [o for o in filled_orders if o["side"] == "buy"]
    sells = [o for o in filled_orders if o["side"] == "sell"]
    
    # Get largest trades by notional
    all_trades_sorted = sorted(filled_orders, key=lambda x: x["notional"], reverse=True)
    largest_trades = all_trades_sorted[:3]  # Top 3
    
    return {
        "buys": buys,
        "sells": sells,
        "largest_trades": largest_trades,
        "total_buys_notional": sum(o["notional"] for o in buys),
        "total_sells_notional": sum(o["notional"] for o in sells),
        "total_notional": sum(o["notional"] for o in filled_orders),
    }


def get_position_changes(filled_orders: list[dict]) -> dict:
    """Identify new positions and exited positions from today's trades."""
    buys_by_ticker = {}
    sells_by_ticker = {}
    
    for order in filled_orders:
        ticker = order["ticker"]
        qty = order["filled_qty"]
        price = order["filled_avg_price"]
        notional = order["notional"]
        
        if order["side"] == "buy":
            if ticker not in buys_by_ticker:
                buys_by_ticker[ticker] = {"qty": 0, "total_notional": 0, "prices": []}
            buys_by_ticker[ticker]["qty"] += qty
            buys_by_ticker[ticker]["total_notional"] += notional
            buys_by_ticker[ticker]["prices"].append(price)
        else:  # sell
            if ticker not in sells_by_ticker:
                sells_by_ticker[ticker] = {"qty": 0, "total_notional": 0, "prices": []}
            sells_by_ticker[ticker]["qty"] += qty
            sells_by_ticker[ticker]["total_notional"] += notional
            sells_by_ticker[ticker]["prices"].append(price)
    
    # Determine initiated and exited positions
    initiated = []
    exited = []
    
    for ticker, buy_data in buys_by_ticker.items():
        # If we bought but didn't sell this ticker, it's initiated
        if ticker not in sells_by_ticker:
            avg_price = buy_data["total_notional"] / buy_data["qty"] if buy_data["qty"] > 0 else 0
            initiated.append({
                "ticker": ticker,
                "qty": buy_data["qty"],
                "avg_price": avg_price,
                "notional": buy_data["total_notional"]
            })
    
    for ticker, sell_data in sells_by_ticker.items():
        # If we sold but didn't buy this ticker, it's exited
        if ticker not in buys_by_ticker:
            avg_price = sell_data["total_notional"] / sell_data["qty"] if sell_data["qty"] > 0 else 0
            exited.append({
                "ticker": ticker,
                "qty": sell_data["qty"],
                "avg_price": avg_price,
                "notional": sell_data["total_notional"]
            })
    
    return {
        "initiated": initiated,
        "exited": exited
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
                        "text": "✅ *No trades executed today*\n\nPortfolio value: $" + f"{account_value:,.2f}",
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
    positions = get_current_positions_with_weights(account_value)
    top_5_positions = positions[:5]
    trade_summary = categorize_trades(filled_orders)
    position_changes = get_position_changes(filled_orders)
    
    # Calculate day P&L
    day_pnl = account_value - (previous_account_value or account_value)
    day_pnl_pct = (day_pnl / previous_account_value * 100) if previous_account_value else 0
    
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
                    "text": f"*Day P&L*\n${day_pnl:,.2f} ({day_pnl_pct:+.2f}%)" if previous_account_value else f"*Trades Executed*\n{len(filled_orders)}",
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
        trade_lines.append(f"*Buys:* {len(trade_summary['buys'])} · ${trade_summary['total_buys_notional']:,.2f}")
    if trade_summary["sells"]:
        trade_lines.append(f"*Sells:* {len(trade_summary['sells'])} · ${trade_summary['total_sells_notional']:,.2f}")
    
    if trade_lines:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(trade_lines)},
        })
        blocks.append({"type": "divider"})
    
    # Largest trades section
    if trade_summary["largest_trades"]:
        largest_lines = []
        for i, trade in enumerate(trade_summary["largest_trades"], 1):
            emoji = "📈" if trade["side"] == "buy" else "📉"
            largest_lines.append(
                f"{emoji} {i}. {trade['side'].upper()} {trade['filled_qty']:.0f} {trade['ticker']} @ ${trade['filled_avg_price']:.2f} = ${trade['notional']:,.2f}"
            )
        
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Largest Trades*\n" + "\n".join(largest_lines)},
        })
    
    # Initiated positions section
    if position_changes["initiated"]:
        initiated_lines = [f"*New Positions ({len(position_changes['initiated'])})*"]
        for pos in position_changes["initiated"]:
            initiated_lines.append(
                f"✅ {pos['ticker']}: {pos['qty']:.0f} shares @ ${pos['avg_price']:.2f} = ${pos['notional']:,.2f}"
            )
        
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(initiated_lines)},
        })
    
    # Exited positions section
    if position_changes["exited"]:
        exited_lines = [f"*Positions Exited ({len(position_changes['exited'])})*"]
        for pos in position_changes["exited"]:
            exited_lines.append(
                f"❌ {pos['ticker']}: {pos['qty']:.0f} shares @ ${pos['avg_price']:.2f} = ${pos['notional']:,.2f}"
            )
        
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(exited_lines)},
        })
    
    # Top 5 positions section
    if top_5_positions:
        position_lines = [f"*Top {min(5, len(positions))} Positions*"]
        for i, pos in enumerate(top_5_positions, 1):
            pl_emoji = "📈" if pos["unrealized_pl"] >= 0 else "📉"
            position_lines.append(
                f"{i}. {pos['ticker']}: {pos['qty']:.0f} @ ${pos['current_price']:.2f} = ${pos['value']:,.2f} ({pos['weight']*100:.1f}%) {pl_emoji} {pos['unrealized_plpc']:+.2f}%"
            )
        
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(position_lines)},
        })
    
    message = {
        "channel": channel,
        "text": "📊 Daily Trading Summary",
        "blocks": blocks,
    }
    
    try:
        client.chat_postMessage(**message)
    except SlackApiError as e:
        raise RuntimeError(f"Error sending Slack message: {e.response['error']}")
