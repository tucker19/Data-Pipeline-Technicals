WITH overs_list AS (
  select filename, inning, array_agg(distinct(over)) as overs, max(over)+1 as max_over
  from innings
  group by filename, inning
),

array_size as (
  select filename, inning, overs, array_length(overs, 1) as size, max_over
  from overs_list
)

select filename, inning, overs, size, max_over
from array_size
where size != max_over
order by filename, inning;