import os
from dotenv import load_dotenv

# Load .env and set custom headers BEFORE importing Prefect
load_dotenv()

cf_access_client_id = os.getenv("CF_ACCESS_CLIENT_ID")
cf_access_client_secret = os.getenv("CF_ACCESS_CLIENT_SECRET")

if cf_access_client_id and cf_access_client_secret:
    os.environ["PREFECT_CLIENT_CUSTOM_HEADERS"] = (
        f'{{"CF-Access-Client-Id": "{cf_access_client_id}", "CF-Access-Client-Secret": "{cf_access_client_secret}"}}'
    )

import prefect as prefect_client