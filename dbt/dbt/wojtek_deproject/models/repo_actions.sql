{{ config(materialized='table') }}


select date_trunc(created_at, month) as push_month, name as repo, count(*) as push_count
from {{source('papers_and_code', 'paper_repo_activity')}}
group by 1,2
order by 3 desc
