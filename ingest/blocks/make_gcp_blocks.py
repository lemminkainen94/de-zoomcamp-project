import os

from dotenv import load_dotenv
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# alternative to creating GCP blocks in the UI
# copy your own service_account_info dictionary from the json file you downloaded from google
# IMPORTANT - do not store credentials in a publicly available repository!

load_dotenv()
BUCKET = os.getenv("BUCKET")

credentials_block = GcpCredentials(
    service_account_file="/home/wojtek/.gc/wojtek-de.json"
)
credentials_block.save("zoom-gcp-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("zoom-gcp-creds"),
    bucket=BUCKET,  # insert your  GCS bucket name
)

bucket_block.save("zoom-gcs", overwrite=True)
