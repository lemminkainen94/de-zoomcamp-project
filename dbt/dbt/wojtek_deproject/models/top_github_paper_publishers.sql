{{ config(materialized='table') }}


select split(repo_url, "/")[3] as github_user, count(distinct paper_url) as paper_count 
from {{ source('papers_and_code', 'paper_repo') }}
group by 1 order by 2 desc
