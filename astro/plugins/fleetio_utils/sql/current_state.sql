select state, count(*)
from issues
group by state;