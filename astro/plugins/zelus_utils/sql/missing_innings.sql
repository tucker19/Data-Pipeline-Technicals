WITH innings_list AS (
  select filename, array_agg(distinct(inning)) as innings_array, max(inning) as max_inning
  from innings
  group by filename
),

array_size as (
  select filename, innings_array, array_length(innings_array, 1) as size, max_inning
  from innings_list
)

select filename, innings_array, size, max_inning
from array_size
where size != max_inning
order by filename;