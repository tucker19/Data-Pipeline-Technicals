select date_trunc('day', updated_at) as updated_at_day, state, count(*) as updated
from issues
group by updated_at_day, state
order by updated_at_day;