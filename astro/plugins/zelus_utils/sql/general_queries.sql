SELECT *
FROM match_info
where filename = '1160280';

SELECT count(*) as row_count
FROM match_info;

select distinct

select *
from match_registry;

select *
from innings
where filename = '1160280';

truncate match_info, match_registry, innings;

ALTER TABLE innings
ADD forfeited text default NULL;

ALTER TABLE innings
ADD super_over text default NULL;

select *
from match_info
where match_number > 0;

select *
from match_info as mi
join match_registry as mr using (filename)
join innings as i using (filename)
where filename in ('1188427')
order by match_number, inning, over;

select *
from match_info
where gender is not null;