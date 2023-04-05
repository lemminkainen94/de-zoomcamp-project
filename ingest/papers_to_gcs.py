from pathlib import Path
import json
import os
import pandas as pd
import requests
from dotenv import load_dotenv
from prefect import flow, task
from prefect.deployments import Deployment
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.server.schemas.schedules import CronSchedule


load_dotenv()

BASE_URL = "https://paperswithcode.com/api/v1"
BUCKET = os.getenv("BUCKET")
PAPERS_DIR = os.getenv("PAPERS_DIR")


@task(retries=3)
def get_resource(res, per_page=1000):
    results = []
    base_page_url = f"{BASE_URL}/{res}/?items_per_page={per_page}"
    page_url = f"{base_page_url}&page=1"
    
    while page_url:
        page_json = get_resource_page_json(page_url)
        results.append(page_json["results"])
        page_url = page_json["next"]
    
    return results


def get_resource_page_json(page_url):
    r = requests.get(page_url)
    print(r.url, r.status_code)
    return r.json()


@task(retries=3)
def results_to_gcs(results, gcs_path):
    df = pd.concat([pd.DataFrame(d) for d in results])
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_dataframe(df, gcs_path, "parquet")


@flow()
def papers_to_gcs():
    """Main flow to load pasperswithcode data into GCS"""
    for resource in ["tasks", "areas", "repositiories", "papers", "methods"]:
        results = get_resource(resource)
        results_to_gcs(results, f"/{PAPERS_DIR}/{resource}.parquet")


deployment = Deployment.build_from_flow(
    flow=papers_to_gcs,
    name="papers_to_gcs",
    work_queue_name="default",
    schedule=CronSchedule(cron="0 6 * * 1")
)

deployment.apply()
