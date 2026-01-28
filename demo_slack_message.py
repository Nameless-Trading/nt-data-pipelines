"""Demo script to show what the Slack message looks like."""

import json
import sys
from pathlib import Path
from unittest.mock import Mock, patch

# Add pipelines to path
sys.path.insert(0, str(Path(__file__).parent / "pipelines"))

from utils.slack_daily_summary import categorize_trades, get_position_changes


def demo_slack_message():
    """Generate and display a sample Slack message."""
    
    # Sample trades from a trading day
    filled_orders = [
        {
            "side": "buy",
            "notional": 4850.00,
            "ticker": "AAPL",
            "filled_qty": 25,
            "filled_avg_price": 194.00,
            "filled_at": None,
            "order_id": "1"
        },
        {
            "side": "buy",
            "notional": 3120.00,
            "ticker": "MSFT",
            "filled_qty": 10,
            "filled_avg_price": 312.00,
            "filled_at": None,
            "order_id": "2"
        },
        {
            "side": "sell",
            "notional": 2850.00,
            "ticker": "TSLA",
            "filled_qty": 15,
            "filled_avg_price": 190.00,
            "filled_at": None,
            "order_id": "3"
        },
        {
            "side": "buy",
            "notional": 5600.00,
            "ticker": "NVDA",
            "filled_qty": 20,
            "filled_avg_price": 280.00,
            "filled_at": None,
            "order_id": "4"
        },
        {
            "side": "buy",
            "notional": 2100.00,
            "ticker": "GOOGL",
            "filled_qty": 10,
            "filled_avg_price": 210.00,
            "filled_at": None,
            "order_id": "5"
        },
        {
            "side": "sell",
            "notional": 1950.00,
            "ticker": "META",
            "filled_qty": 5,
            "filled_avg_price": 390.00,
            "filled_at": None,
            "order_id": "6"
        },
        {
            "side": "sell",
            "notional": 2850.00,
            "ticker": "AMD",
            "filled_qty": 30,
            "filled_avg_price": 95.00,
            "filled_at": None,
            "order_id": "7"
        },
    ]
    
    # Sample positions (current portfolio)
    positions = [
        {
            "ticker": "AAPL",
            "qty": 150,
            "value": 29100,
            "weight": 0.277,
            "entry_price": 194.0,
            "current_price": 194.0,
            "unrealized_pl": 450,
            "unrealized_plpc": 0.0158
        },
        {
            "ticker": "NVDA",
            "qty": 120,
            "value": 33600,
            "weight": 0.320,
            "entry_price": 280.0,
            "current_price": 280.0,
            "unrealized_pl": 1200,
            "unrealized_plpc": 0.0370
        },
        {
            "ticker": "MSFT",
            "qty": 75,
            "value": 23400,
            "weight": 0.223,
            "entry_price": 312.0,
            "current_price": 312.0,
            "unrealized_pl": 750,
            "unrealized_plpc": 0.0330
        },
        {
            "ticker": "GOOGL",
            "qty": 50,
            "value": 10500,
            "weight": 0.100,
            "entry_price": 210.0,
            "current_price": 210.0,
            "unrealized_pl": 300,
            "unrealized_plpc": 0.0294
        },
        {
            "ticker": "AMZN",
            "qty": 25,
            "value": 3520,
            "weight": 0.034,
            "entry_price": 140.8,
            "current_price": 140.8,
            "unrealized_pl": 100,
            "unrealized_plpc": 0.0297
        },
    ]
    
    account_value = 104950.00
    previous_account_value = 103520.00
    day_pnl = account_value - previous_account_value
    day_pnl_pct = (day_pnl / previous_account_value * 100)
    
    # Categorize trades and get position changes
    trade_summary = categorize_trades(filled_orders)
    position_changes = get_position_changes(filled_orders)
    
    # Build the message blocks
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "📊 Daily Trading Summary"}
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Portfolio Value*\n${account_value:,.2f}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Day P&L*\n${day_pnl:,.2f} ({day_pnl_pct:+.2f}%)"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Total Volume*\n${trade_summary['total_notional']:,.2f}"
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Positions*\n{len(positions)} open"
                },
            ]
        },
        {"type": "divider"}
    ]
    
    # Trade summary
    trade_lines = []
    if trade_summary["buys"]:
        trade_lines.append(f"*Buys:* {len(trade_summary['buys'])} · ${trade_summary['total_buys_notional']:,.2f}")
    if trade_summary["sells"]:
        trade_lines.append(f"*Sells:* {len(trade_summary['sells'])} · ${trade_summary['total_sells_notional']:,.2f}")
    
    if trade_lines:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(trade_lines)}
        })
        blocks.append({"type": "divider"})
    
    # Largest trades
    if trade_summary["largest_trades"]:
        largest_lines = []
        for i, trade in enumerate(trade_summary["largest_trades"], 1):
            emoji = "📈" if trade["side"] == "buy" else "📉"
            largest_lines.append(
                f"{emoji} {i}. {trade['side'].upper()} {trade['filled_qty']:.0f} {trade['ticker']} @ ${trade['filled_avg_price']:.2f} = ${trade['notional']:,.2f}"
            )
        
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Largest Trades*\n" + "\n".join(largest_lines)}
        })
    
    # Initiated positions
    if position_changes["initiated"]:
        initiated_lines = [f"*New Positions ({len(position_changes['initiated'])})*"]
        for pos in position_changes["initiated"]:
            initiated_lines.append(
                f"✅ {pos['ticker']}: {pos['qty']:.0f} shares @ ${pos['avg_price']:.2f} = ${pos['notional']:,.2f}"
            )
        
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(initiated_lines)}
        })
    
    # Exited positions
    if position_changes["exited"]:
        exited_lines = [f"*Positions Exited ({len(position_changes['exited'])})*"]
        for pos in position_changes["exited"]:
            exited_lines.append(
                f"❌ {pos['ticker']}: {pos['qty']:.0f} shares @ ${pos['avg_price']:.2f} = ${pos['notional']:,.2f}"
            )
        
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(exited_lines)}
        })
    
    # Top 5 positions
    if positions:
        position_lines = [f"*Top {min(5, len(positions))} Positions*"]
        for i, pos in enumerate(positions[:5], 1):
            pl_emoji = "📈" if pos["unrealized_pl"] >= 0 else "📉"
            position_lines.append(
                f"{i}. {pos['ticker']}: {pos['qty']:.0f} @ ${pos['current_price']:.2f} = ${pos['value']:,.2f} ({pos['weight']*100:.1f}%) {pl_emoji} {pos['unrealized_plpc']:+.2f}%"
            )
        
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(position_lines)}
        })
    
    # Print formatted message
    print("\n" + "="*80)
    print("SLACK MESSAGE PREVIEW")
    print("="*80 + "\n")
    
    # Header
    print("📊 Daily Trading Summary\n")
    
    # Stats
    print(f"Portfolio Value: ${account_value:,.2f}")
    print(f"Day P&L: ${day_pnl:,.2f} ({day_pnl_pct:+.2f}%)")
    print(f"Total Volume: ${trade_summary['total_notional']:,.2f}")
    print(f"Positions: {len(positions)} open\n")
    
    # Trade breakdown
    print("─" * 80)
    if trade_summary["buys"]:
        print(f"Buys: {len(trade_summary['buys'])} · ${trade_summary['total_buys_notional']:,.2f}")
    if trade_summary["sells"]:
        print(f"Sells: {len(trade_summary['sells'])} · ${trade_summary['total_sells_notional']:,.2f}")
    print()
    
    # Largest trades
    print("─" * 80)
    print("Largest Trades\n")
    for i, trade in enumerate(trade_summary["largest_trades"], 1):
        emoji = "📈" if trade["side"] == "buy" else "📉"
        print(f"{emoji} {i}. {trade['side'].upper():4} {trade['filled_qty']:5.0f} {trade['ticker']:5} @ ${trade['filled_avg_price']:7.2f} = ${trade['notional']:>10,.2f}")
    print()
    
    # New positions
    if position_changes["initiated"]:
        print("─" * 80)
        print(f"New Positions ({len(position_changes['initiated'])})\n")
        for pos in position_changes["initiated"]:
            print(f"✅ {pos['ticker']:6} {pos['qty']:5.0f} shares @ ${pos['avg_price']:7.2f} = ${pos['notional']:>10,.2f}")
        print()
    
    # Exited positions
    if position_changes["exited"]:
        print("─" * 80)
        print(f"Positions Exited ({len(position_changes['exited'])})\n")
        for pos in position_changes["exited"]:
            print(f"❌ {pos['ticker']:6} {pos['qty']:5.0f} shares @ ${pos['avg_price']:7.2f} = ${pos['notional']:>10,.2f}")
        print()
    
    # Top 5 positions
    print("─" * 80)
    print(f"Top {min(5, len(positions))} Positions\n")
    for i, pos in enumerate(positions[:5], 1):
        pl_emoji = "📈" if pos["unrealized_pl"] >= 0 else "📉"
        print(f"{i}. {pos['ticker']:6} {pos['qty']:5.0f} @ ${pos['current_price']:7.2f} = ${pos['value']:>10,.2f} ({pos['weight']*100:5.1f}%) {pl_emoji} {pos['unrealized_plpc']:+6.2f}%")
    
    print("\n" + "="*80 + "\n")
    
    # Also print the raw blocks JSON for reference
    print("RAW SLACK BLOCKS (JSON):")
    print(json.dumps(blocks, indent=2))


if __name__ == "__main__":
    demo_slack_message()
