{{ config(materialized='table') }}


select repo_dep, count(*) as dep_count 
from {{ source('papers_and_code', 'paper_repo_deps') }}
group by 1 order by 2 desc
