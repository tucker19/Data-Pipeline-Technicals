select vehicle_id, count(*) as issue_count
from issues
where vehicle_id is not NULL
group by vehicle_id 
order by issue_count desc
limit 10;