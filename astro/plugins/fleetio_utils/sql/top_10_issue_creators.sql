select created_by_id, count(*) as issue_count
from issues
where created_by_id is not null
group by created_by_id;

select created_by_id, count(*) as issue_count
from issues
where created_by_id is not null
group by created_by_id
order by issue_count desc
limit 10;