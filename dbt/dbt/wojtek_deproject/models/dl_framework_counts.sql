{{ config(materialized='table') }}


select 
    repo_dep as dl_framework,
    count(*) as repo_count
from {{ source('papers_and_code', 'paper_repo_deps') }}
where repo_dep in ('tensorflow', 'torch', 'theano', 'keras')
group by 1
