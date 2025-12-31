from clickhouse_connect import get_client
import os
from dotenv import load_dotenv

load_dotenv()


def get_clickhouse_client():
    host = os.getenv("HOST")
    port = os.getenv("PORT")
    username = os.getenv("CLICKHOUSE_USER")
    password = os.getenv("CLICKHOUSE_PASSWORD")
    secure = os.getenv("SECURE")

    if not (host and port and username and password and secure):
        raise RuntimeError(
            f"""
            Environment variables not set:
                HOST: {host}
                PORT: {port}
                CLICKHOUSE_USER: {username}
                CLICKHOUSE_PASSWORD: {password}
                SECURE: {secure}
            """
        )

    return get_client(
        host=host, port=port, username=username, password=password, secure=secure
    )
