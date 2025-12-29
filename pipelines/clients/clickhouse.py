from clickhouse_connect import get_client
import os
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("HOST")
port = os.getenv("PORT")
username = os.getenv("CLICKHOUSE_USER")
password = os.getenv("CLICKHOUSE_PASSWORD")

if not (host and port and username and password):
    raise RuntimeError(
        f"""
        Environment variables not set:
            HOST: {host}
            PORT: {port}
            CLICKHOUSE_USER: {username}
            CLICKHOUSE_PASSWORD: {password}
        """
    )

clickhouse_client = get_client(
    host=host,
    port=port,
    username=username,
    password=password,
    secure=True
)