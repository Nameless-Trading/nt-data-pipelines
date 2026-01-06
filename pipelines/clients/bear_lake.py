import bear_lake as bl
import os
from dotenv import load_dotenv

load_dotenv()

access_key_id = os.getenv("ACCESS_KEY_ID")
secret_access_key = os.getenv("SECRET_ACCESS_KEY")
region = os.getenv("REGION")
endpoint = os.getenv("ENDPOINT")
bucket = os.getenv("BUCKET")


storage_options = {
    "aws_access_key_id": access_key_id,
    "aws_secret_access_key": secret_access_key,
    "region": region,
    "endpoint_url": endpoint,
}

url = f"s3://{bucket}"


def get_bear_lake_client() -> bl.Database:
    return bl.connect_s3(path=url, storage_options=storage_options)
