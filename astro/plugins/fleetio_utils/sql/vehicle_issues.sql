select vehicle_id, count(*)
from issues
group by vehicle_id 
order by vehicle_id;