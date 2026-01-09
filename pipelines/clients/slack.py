from slack_sdk import WebClient
import os
from dotenv import load_dotenv

load_dotenv()


def get_slack_client():
    slack_bot_token = os.getenv("SLACK_BOT_TOKEN")

    if not slack_bot_token:
        raise RuntimeError(
            f"""
            Environment variables not set:
                SLACK_BOT_TOKEN: {slack_bot_token}
            """
        )

    return WebClient(token=slack_bot_token)
