import json
import os
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from time import sleep

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery, bigquery_datatransfer_v1, dataproc_v1, storage
from google.cloud.exceptions import NotFound
from google.protobuf.timestamp_pb2 import Timestamp
from paper_repo_deps import append_paper_repo_deps
from prefect import flow, task
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_shell import shell_run_command

PAPER_REPO_ACTIVITY_TABLE_ID = (
    "velvety-setup-377621.papers_and_code.paper_repo_activity"
)
PAPER_REPO_DEPS_TABLE_ID = "velvety-setup-377621.papers_and_code.paper_repo_deps"
REPO_ACTIVITY_TABLE_ID = "velvety-setup-377621.gha.repo_activity"
EU_REPO_ACTIVITY_TABLE_ID = "velvety-setup-377621.papers_and_code.repo_activity"
REPO_ACTIVITY_SCHEMA = [
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
]

yesterday = date.today() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y%m%d")
year = yesterday.year

"""
There are two modes of getting the gh archive data:
from the start of the year to yesterday
and just yesterday for daily updates
"""
GHA_SQL_POPULATE = f"""
    SELECT repo.name, created_at
    FROM `githubarchive.month.{year}*` ga
    WHERE 
        type = 'PushEvent'
"""

GHA_SQL_UPDATE = f"""
    SELECT repo.name, created_at
    FROM `githubarchive.day.{yesterday_str}` ga
    WHERE 
        type = 'PushEvent'
"""

ACTIVITY_SQL = f"""
    SELECT * FROM `velvety-setup-377621.papers_and_code.repo_activity` ra
    JOIN (
    select FORMAT('%s/%s', split(split(repo_url, 'https://github.com/')[1], '/')[0], split(split(repo_url, 'https://github.com/')[1], '/')[1]) as name
    from `papers_and_code.paper_repo` pr join `papers_and_code.paper` p on p.paper_url = pr.paper_url 
    where p.date >= '2023-01-01'
    and repo_url like 'https://github.com/%'
    ) pr
    using (name)
"""


@task
def submit_dataproc_job(region, cluster_name, spark_path, project_id):
    # Create the job client.
    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the job config.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": spark_path,
            "jar_file_uris": ["gs://spark-lib/bigquery/spark-3.3-bigquery-0.31.1.jar"],
        },
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output is saved to the Cloud Storage bucket
    # allocated to the job. Use regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_string()
    )

    print(f"Job finished successfully: {output}\r\n")


def query_results_to_bq(client: bigquery.Client, table_id: str, schema: str, sql: str):
    """
    first checks if the table exists, if not then it creates the table with the given id and schema
    then it writes the query results to the table
    """
    # if the table exists just update it with yesterdays data
    try:
        client.get_table(table_id)
        print("Table {} already exists.".format(table_id))
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)

    job_config = bigquery.QueryJobConfig(destination=table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    query_job = client.query(sql, job_config=job_config)
    query_job.result()


@task
def deps_to_bq(client: bigquery.Client):
    """
    checks if paper_repo_deps exists
    if no then it creates and populates it first, with the data from the start of the year until now
    otherwise just updates with the latest data
    """
    try:
        client.get_table(PAPER_REPO_DEPS_TABLE_ID)
        print("Table {} already exists.".format(PAPER_REPO_DEPS_TABLE_ID))
        append_paper_repo_deps(str(yesterday), str(yesterday))
    except NotFound:
        append_paper_repo_deps("2023-01-01", str(yesterday))


@task
def gh_to_bq(client: bigquery.Client):
    """
    load github archive data to the US dataset
    if the target table exists, then just update with yesterday's data
    otherwise populate from the start of the year
    """
    # if the table exists just update it with yesterdays data
    sql = GHA_SQL_UPDATE
    try:
        client.get_table(PAPER_REPO_ACTIVITY_TABLE_ID)
        print("Table {} already exists.".format(PAPER_REPO_ACTIVITY_TABLE_ID))
    except NotFound:
        # if the table needs to be recreated, populate it with this year's data
        table = bigquery.Table(
            PAPER_REPO_ACTIVITY_TABLE_ID, schema=REPO_ACTIVITY_SCHEMA
        )
        table = client.create_table(table)
        sql = GHA_SQL_POPULATE

    query_results_to_bq(client, REPO_ACTIVITY_TABLE_ID, REPO_ACTIVITY_SCHEMA, sql)


@task
def activity_to_bq(client: bigquery.Client):
    """
    write paper repo activity to the target table, storing push events from paper repos
    """
    query_results_to_bq(
        client, PAPER_REPO_ACTIVITY_TABLE_ID, REPO_ACTIVITY_SCHEMA, ACTIVITY_SQL
    )


@task
def bq_region_transfer(project: str):
    """
    trigger gh archive data transfer from US to EU
    """
    bqt_client = bigquery_datatransfer_v1.DataTransferServiceClient()
    parent = "projects/643927585543/locations/europe-west6/transferConfigs/64a023fb-0000-25d2-b3ab-30fd3811ac9c"

    start_time = Timestamp(seconds=int(time.time()))

    request = bigquery_datatransfer_v1.types.StartManualTransferRunsRequest(
        {"parent": parent, "requested_run_time": start_time}
    )

    response = bqt_client.start_manual_transfer_runs(request, timeout=360)
    # wait for the data transfer to complete
    sleep(180)


@task
def remove_gha_tables(client: bigquery.Client):
    client.delete_table(REPO_ACTIVITY_TABLE_ID)
    client.delete_table(EU_REPO_ACTIVITY_TABLE_ID)


load_dotenv()

BUCKET = os.getenv("BUCKET")
SPARK_PATH = os.getenv("SPARK_PATH")
PROJECT = os.getenv("PROJECT_ID")
REGION = "europe-west6"
CLUSTER = os.getenv("CLUSTER")
PAPERS_DIR = os.getenv("PAPERS_DIR")
LINKS_FILE = "links-between-papers-and-code.json.gz"
PAPERS_FILE = "papers-with-abstracts.json.gz"
URL = "https://production-media.paperswithcode.com/about"


@flow()
def papers_to_bq(
    url, dest_dir, region, cluster_name, gcs_bucket, spark_path, project_id
):
    """
    Data ingestion dag to be deployed in Prefect orion
    Ingests the data from paperswithcode API, github API and Github Archive BQ project
    Uses spark jobs submitted to dataproc to perform ETL, resulting in a simple DWH in BQ,
    which later is used with dbt to create the resulting reporting dataset
    """
    client = bigquery.Client()

    for filename in [PAPERS_FILE, LINKS_FILE]:
        shell_run_command(
            command=f"curl {URL}/{filename} | gsutil cp - gs://{gcs_bucket}/{dest_dir}/{filename}"
        )
    gh_to_bq(client)
    submit_dataproc_job(region, cluster_name, spark_path, project_id)
    deps_to_bq(client)
    bq_region_transfer(project_id)
    activity_to_bq(client)
    remove_gha_tables(client)


if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=papers_to_bq,
        name="papers_to_bq",
        work_queue_name="default",
        parameters={
            "url": URL,
            "dest_dir": PAPERS_DIR,
            "region": REGION,
            "cluster_name": CLUSTER,
            "gcs_bucket": BUCKET,
            "spark_path": SPARK_PATH,
            "project_id": PROJECT,
        },
        schedule=CronSchedule(cron="0 6 * * *"),
    )

    deployment.apply()
