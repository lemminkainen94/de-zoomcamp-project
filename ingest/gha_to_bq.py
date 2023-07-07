import sys
from datetime import date, timedelta

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def query_results_to_bq(table_id: str, schema: str, sql: str):
    try:
        client.get_table(table_id)
        print("Table {} already exists.".format(table_id))
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)

    job_config = bigquery.QueryJobConfig(destination=table_id)

    query_job = client.query(sql, job_config=job_config)
    query_job.result()
