from slack_sdk.web.slack_response import SlackResponse
from pipelines.clients import get_slack_client
from slack_sdk.errors import SlackApiError
import os


def send_actual_trades_summary(filled_orders: list) -> SlackResponse:
    client = get_slack_client()
    channel = os.getenv("SLACK_CHANNEL")

    if not channel:
        raise RuntimeError(
            "SLACK_CHANNEL environment variable not set and no channel provided"
        )

    if not filled_orders:
        message = {
            "channel": channel,
            "text": "✅ No trades executed today",
        }
        try:
            response = client.chat_postMessage(**message)
            return response
        except SlackApiError as e:
            raise RuntimeError(f"Error sending Slack message: {e.response['error']}")

    trade_lines = []
    for order in filled_orders:
        emoji = "📈" if order["side"] == "buy" else "📉"
        trade_lines.append(
            f"{emoji} {order['side'].upper()} {order['filled_qty']:.2f} shares of {order['ticker']} @ ${order['filled_avg_price']:.2f} = ${order['notional']:,.2f}"
        )

    trades_text = "\n".join(trade_lines)
    total_notional = sum(order["notional"] for order in filled_orders)

    message = {
        "channel": channel,
        "text": f"✅ Executed Trades Summary - {len(filled_orders)} trades filled",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "✅ Executed Trades Report"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Total Trades Executed:* {len(filled_orders)}\n*Total Notional:* ${total_notional:,.2f}",
                },
            },
            {"type": "divider"},
            {"type": "section", "text": {"type": "mrkdwn", "text": trades_text}},
        ],
    }

    try:
        response = client.chat_postMessage(**message)
        return response
    except SlackApiError as e:
        raise RuntimeError(f"Error sending Slack message: {e.response['error']}")
