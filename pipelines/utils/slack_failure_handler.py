"""Slack failure handler for Prefect flows using hook pattern."""

import os
import traceback
from typing import Optional

from clients import get_slack_client
from prefect.flows import FlowHook
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


class SlackFailureHook(FlowHook):
    """Prefect FlowHook that sends Slack notifications on flow failure."""
    
    def __init__(self, flow_name: str):
        """
        Initialize the hook with a flow name.
        
        Args:
            flow_name: Name of the flow for Slack messages
        """
        super().__init__()
        self.flow_name = flow_name
    
    async def hook_fn(self, flow, flow_run, state):
        """
        Hook function called when a flow fails.
        
        Args:
            flow: The Prefect Flow object
            flow_run: The Prefect FlowRun object
            state: The state object containing failure info
        """
        try:
            # Extract error from the state
            # In Prefect 3.0+, failed states have exception info
            if state.result(raise_on_failure=False):
                # Try to get the actual exception
                exc = state.result(raise_on_failure=False)
                if isinstance(exc, Exception):
                    error = exc
                else:
                    error = Exception(str(exc))
            else:
                error = Exception("Unknown error")
            
            # Build context info
            context = {
                "run_id": str(flow_run.id) if flow_run else "unknown",
                "parameters": flow_run.parameters if flow_run else {},
            }
            
            send_flow_failure_notification(self.flow_name, error, context)
        except Exception as e:
            # Don't let the handler failure break the flow
            print(f"Error in Slack failure hook: {e}")


def create_failure_handler(flow_name: str) -> SlackFailureHook:
    """
    Create a Slack failure hook for a Prefect flow.
    
    Usage:
        @flow(on_failure=[create_failure_handler("my_flow")])
        def my_flow():
            ...
    
    Args:
        flow_name: Name of the flow (for the Slack message)
    
    Returns:
        A SlackFailureHook instance
    """
    return SlackFailureHook(flow_name)
