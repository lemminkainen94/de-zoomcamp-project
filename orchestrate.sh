prefect orion start &
prefect agent start --work-queue "default" &
cd ingest/
poetry run python papers_to_bq.py