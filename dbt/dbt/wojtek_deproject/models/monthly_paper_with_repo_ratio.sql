{{ config(materialized='table') }}


select 
    date_trunc(date, MONTH) as month,
    count(distinct p.paper_url) as paper_count,
    count(distinct pr.paper_url) as paper_with_repo_count,
    count(distinct pr.paper_url) / count(distinct p.paper_url) as paper_with_repo_ratio
from {{ source('papers_and_code', 'paper') }} p
left join {{ source('papers_and_code', 'paper_repo') }} pr
using (paper_url)
group by 1
order by 1
