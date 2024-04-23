select date_trunc('day', created_at), count(*)
from issues
group by date_trunc('day', created_at)
order by date_trunc('day', created_at);