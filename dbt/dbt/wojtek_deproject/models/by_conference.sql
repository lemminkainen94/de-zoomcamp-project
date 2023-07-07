{{ config(materialized='table') }}


select proceeding, count(*) as paper_count 
from {{ source('papers_and_code', 'paper') }}
group by 1 order by 2 desc
