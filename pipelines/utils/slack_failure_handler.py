"""Slack failure handler for Prefect flows."""

import os
import traceback
from typing import Optional

from clients import get_slack_client
from slack_sdk.errors import SlackApiError


def send_flow_failure_notification(
    flow_name: str,
    error: Exception,
    context: Optional[dict] = None,
) -> None:
    """
    Send a Slack notification when a flow fails.
    
    Args:
        flow_name: Name of the failed flow
        error: The exception that was raised
        context: Optional context dict with additional info (e.g., run_id, parameters)
    """
    client = get_slack_client()
    channel = os.getenv("SLACK_CHANNEL")
    
    if not channel:
        raise RuntimeError("SLACK_CHANNEL environment variable not set")
    
    # Format the traceback
    tb_str = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    
    # Truncate traceback if it's too long (Slack has message limits)
    # Keep it under ~2000 chars to fit in a code block
    if len(tb_str) > 2000:
        tb_str = tb_str[-1950:] + "\n... (truncated)"
    
    # Build context info
    context_lines = []
    if context:
        if "run_id" in context:
            context_lines.append(f"*Run ID:* `{context['run_id']}`")
        if "parameters" in context:
            params_str = str(context["parameters"])
            if len(params_str) > 200:
                params_str = params_str[:197] + "..."
            context_lines.append(f"*Parameters:* `{params_str}`")
    
    # Build the message blocks
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🚨 Flow Failed: {flow_name}"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Error Type:* `{type(error).__name__}`\n*Error Message:* {str(error)}",
            },
        },
    ]
    
    # Add context if available
    if context_lines:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(context_lines),
            },
        })
    
    # Add divider and traceback
    blocks.extend([
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Traceback:*\n```" + tb_str + "```",
            },
        },
    ])
    
    message = {
        "channel": channel,
        "text": f"🚨 Flow Failed: {flow_name}",
        "blocks": blocks,
    }
    
    try:
        client.chat_postMessage(**message)
    except SlackApiError as e:
        # If we can't send to Slack, at least log it
        print(f"Failed to send Slack notification: {e.response['error']}")
        raise


def create_failure_handler(flow_name: str):
    """
    Create a flow-level failure handler for a Prefect flow.
    
    Usage:
        @flow(on_failure=create_failure_handler("my_flow"))
        def my_flow():
            ...
    
    Args:
        flow_name: Name of the flow (for the Slack message)
    
    Returns:
        A callable handler function for use with @flow decorator
    """
    def failure_handler(flow, flow_run, state):
        """Handle flow failure and send Slack notification."""
        try:
            # Extract error info from the state
            error = state.result(raise_on_failure=False)
            if isinstance(error, Exception):
                exc = error
            else:
                # If not an exception, try to get the traceback from the state
                exc = Exception(str(error))
            
            # Build context info
            context = {
                "run_id": str(flow_run.id) if flow_run else "unknown",
                "parameters": flow_run.parameters if flow_run else {},
            }
            
            send_flow_failure_notification(flow_name, exc, context)
        except Exception as e:
            # Don't let the handler failure break the flow
            print(f"Error in failure handler: {e}")
    
    return failure_handler
