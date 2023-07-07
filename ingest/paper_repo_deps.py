"""
github_read_file function taken from https://stackoverflow.com/a/70136393 and adjusted to the project's needs
"""

import base64
import datetime
import json
import os
import re
from time import sleep

import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PAPER_REPO_DEPS_TABLE_ID = "velvety-setup-377621.papers_and_code.paper_repo_deps"
PAPER_REPO_DEPS_SCHEMA = [
    bigquery.SchemaField("paper_url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("repo_url", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("repo_dep", "STRING", mode="REQUIRED"),
]


def read_github_token():
    with open("/home/wojtek/.gh/gh_api_token") as f:
        return f.read().strip()


def github_read_file(
    username: str, repository_name: str, file_path: str, github_token=None
) -> str:
    """
    use github REST API to retrieve the file contents given the repo and the file path
    """
    headers = {}
    if github_token:
        headers["Authorization"] = f"token {github_token}"

    url = f"https://api.github.com/repos/{username}/{repository_name}/contents/{file_path}"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()
    file_content = data["content"]
    file_content_encoding = data.get("encoding")
    if file_content_encoding == "base64":
        file_content = base64.b64decode(file_content).decode()

    return file_content


def get_deps_from_requirements_file(file_content: str) -> list:
    """
    get a list of dependencies from requirements file
    """
    deps = []
    for line in file_content.split("\n"):
        dep = line.split("#")[0].split("<")[0].split("=")[0].split(">")[0]
        if len(dep) > 0:
            deps.append(dep)
    return deps


def get_dependencies_from_pyproject_file(file_content: str) -> list:
    """
    get a list of dependencies from pyproject file, with dependencies in either pep or poetry style
    """
    deps = []

    # poetry style
    dep_section = file_content.split("[tool.poetry.dependencies]")
    if len(dep_section) > 0:
        dep_section = dep_section[1].split("[")[0]

    # pep style
    dep_section = file_content.split("dependencies = [")
    if len(dep_section) > 0:
        dep_section = dep_section[1].split("]")[0]

    for line in file_content.split("\n"):
        dep = line.strip().split("#")[0].split("<")[0].split("=")[0].split(">")[0]
        if len(dep) > 0:
            deps.append(dep)
    return deps


def append_paper_repo_deps(date_from: str, date_to: str) -> None:
    """
    get all dependencies from paper repos for the given time range
    append them to the specified bq table
    focus on github repos since that's > 99% of all cases
    """
    QUERY = f"""
        SELECT pr.paper_url, pr.repo_url 
        FROM `papers_and_code.paper_repo` pr 
        JOIN `papers_and_code.paper` p
        ON p.paper_url = pr.paper_url
        LEFT JOIN `papers_and_code.paper_repo_deps` prd
        ON pr.repo_url = prd.repo_url
        WHERE p.date BETWEEN '{date_from}' AND '{date_to}'
        AND pr.repo_url LIKE 'https://github.com/%'
        AND prd.repo_url IS NULL
    """
    dep_rows = []
    client = bigquery.Client()
    token = read_github_token()

    try:
        table = client.get_table(PAPER_REPO_DEPS_TABLE_ID)
        print("Table {} already exists.".format(PAPER_REPO_DEPS_TABLE_ID))
    except NotFound:
        table = bigquery.Table(PAPER_REPO_DEPS_TABLE_ID, schema=PAPER_REPO_DEPS_SCHEMA)
        table = client.create_table(table)

    query_job = client.query(QUERY)
    rows = query_job.result()

    counter = 0
    row_count = rows.total_rows
    for row in rows:
        counter += 1
        if counter % 100 == 0:
            print(f"insert {len(dep_rows)} rows")
            if len(dep_rows) > 0:
                client.insert_rows(table, dep_rows)
            dep_rows = []
        print(f"row {counter} of {row_count}")
        sleep(0.5)

        try:
            user_repo = row[1].split("https://github.com/")[1]
            user_repo = user_repo.split("/")
            user = user_repo[0]
            repo = user_repo[1]
        except:
            print(f"incorrect github url: {row[1]}. Skipping")
            continue

        try:
            file_content = github_read_file(user, repo, "requirements.txt", token)
            deps = get_deps_from_requirements_file(file_content)
        except:
            try:
                file_content = github_read_file(user, repo, "pyproject.toml", token)
                deps = get_dependencies_from_pyproject_file(file_content)
            except:
                print(
                    f"The repo {repo} has neither requirements.txt nor pyproject.toml file. Skipping"
                )
                continue
        print(f"appending deps for repo: {repo}, {row[1]}")
        for dep in deps:
            print(dep)
            dep_rows.append((row[0], row[1], dep))

    print(f"insert {len(dep_rows)} rows")
    if len(dep_rows) > 0:
        client.insert_rows(table, dep_rows)


def main():
    date_from = "2023-01-01"
    date_to = str(datetime.date.today())
    append_paper_repo_deps(date_from, date_to)


if __name__ == "__main__":
    main()
