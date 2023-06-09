# de-zoomcamp-project
The final project for the Data Engineering Zoomcamp - Data Pipeline for Research Papers and Repos
![deproject_arch drawio](https://user-images.githubusercontent.com/17692760/228347236-4e20b3c4-e422-449b-a3d3-a4fa4fe01b16.png)


# Papers And Code
The goal of this project was to develop a data pipeline, which ingests paperswithcode date, complements it with some repo parsing using github REST API
and combining it with github archive Big Query project to build a simple data warehouse of papers and their corresponding repos. 

Main steps:
1. Data ingestion using a pyspark job submitted to google dataproc. The job downloads the data to a gcs bucket, performs some simple transformations in pyspark and loads the data to big query.
2. A downstream task uses github REST API to download and parse data about dependencies from research paper github repos written in python.
3. Another job loads the data from github archive big query project to this data warehouse, using gq data transfer service to move the data from US to EU region
4. These steps are orchestrated in Prefect and deployed to run on a daily basis.
5. Then a dbt project picks up the data warehouse tables and builds a reporting dataset with models capturing interesting stats.
6. Data loooker reports are created using the reporting dataset. Here are some visualizations created using the data warehouse:

![Screenshot from 2023-07-07 09-16-40](https://github.com/lemminkainen94/de-zoomcamp-project/assets/17692760/38f60dcd-fd95-445f-afdc-4d87b9f9a7c2)
![Screenshot from 2023-07-07 09-16-02](https://github.com/lemminkainen94/de-zoomcamp-project/assets/17692760/d8c950b6-7639-4d07-884b-1a03ec1fc350)
![Screenshot from 2023-07-07 09-15-39](https://github.com/lemminkainen94/de-zoomcamp-project/assets/17692760/fbfb951d-740d-4edd-8151-92c5ba23afe3)


# Install and Run
The project uses poetry to manage dependencies and isort/black for style checks  
To start, run: poetry init (if you don't have it, pip install poetry first)  

You should also have your google cloud created and a cli access from your machine, e.g. via a service account.  
I use a credentials json located in ~/.gh.  
Also make sure you have terraform, docker and docker-compose installed, as well as a github token in ~/.gh  

## terraform

Go to terraform folder and follow the Readme file there.
I've created a bucket for data ingestion from paperswithcode as well as two datasets: one for the target data wareousse, located in europe-west6, one in US, for github archive data.
## Ingest with Prefect

Run: bash orchestrate.sh
This should spin up a prefect ui server, start a default prefect agent queue and deploy ingest/papers_to_bq.py
I've set it up to run the ingestion dag daily.

## dbt
Go to the dbt folder and follow the instructions to setup and build the dbt project with docker compose.
