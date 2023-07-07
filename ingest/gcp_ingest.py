import pandas as pd
import pyspark
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sc = SparkContext()

spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()


BUCKET = "wojtek_data_lake_velvety-setup-377621"
PAPERS_DIR = "papers_and_code"

client = bigquery.Client()

PAPER_REPO_TABLE_ID = "velvety-setup-377621.papers_and_code.paper_repo"
PAPER_TABLE_ID = "velvety-setup-377621.papers_and_code.paper"

PAPER_REPO_FILE = "links-between-papers-and-code.json.gz"
PAPER_FILE = "papers-with-abstracts.json.gz"

PAPER_REPO_SCHEMA = [
    bigquery.SchemaField("paper_url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("repo_url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("mentioned_in_paper", "BOOL"),
    bigquery.SchemaField("mentioned_in_github", "BOOL"),
]

PAPER_SCHEMA = [
    bigquery.SchemaField("paper_url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("arxiv_id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("abstract", "STRING"),
    bigquery.SchemaField("url_abs", "STRING"),
    bigquery.SchemaField("url_pdf", "STRING"),
    bigquery.SchemaField("proceeding", "STRING"),
    bigquery.SchemaField("date", "DATE"),
]

PAPER_REPO_SQL = """
    select FORMAT('%s/%s', split(split(repo_url, 'https://github.com/')[1], '/')[0], split(split(repo_url, 'https://github.com/')[1], '/')[1]) as name
    from `velvety-setup-377621.papers_and_code.paper_repo` pr join `velvety-setup-377621.papers_and_code.paper` p
    on p.paper_url = pr.paper_url 
    where p.date >= '2023-01-01'
    and repo_url like 'https://github.com/%'
"""


def load_table_to_bq(table_id: str, schema: list, src_json_gz_file: str):
    """
    Load the source data from src_json_gz_file, downloaded from paperswithcode API
    Using spark and write it to a BQ table. Make sure the table exists first
    """
    try:
        client.get_table(table_id)
        print("Table {} already exists.".format(table_id))
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)

    df = spark.read.option("multiline", "true").json(
        f"gs://{BUCKET}/{PAPERS_DIR}/{src_json_gz_file}"
    )
    df.printSchema()

    cols = [sf.name for sf in schema]
    if "date" in cols:
        df = df.withColumn("date", (col("date").cast("date")))

    df.select(cols).write.format("bigquery").option("writeMethod", "direct").mode(
        "overwrite"
    ).save(table_id)


load_table_to_bq(PAPER_TABLE_ID, PAPER_SCHEMA, PAPER_FILE)
load_table_to_bq(PAPER_REPO_TABLE_ID, PAPER_REPO_SCHEMA, PAPER_REPO_FILE)
