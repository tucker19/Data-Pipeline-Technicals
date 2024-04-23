select date_trunc('day', updated_at) as updated_at_day,
  sum(case when state = 'open' then 1 else 0 end) as opened,
  sum(case when state = 'closed' then 1 else 0 end) as closed,
  sum(case when state = 'resolved' then 1 else 0 end) as resolved
from issues
group by updated_at_day
order by updated_at_day;