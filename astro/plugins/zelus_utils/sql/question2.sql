-- Win records by season and gender
with team_wins as(
  select season, gender, winner as team, count(*) as team_win_count
  from match_info as mi
  where winner is not null
  group by season, gender, winner
),

team_matches as (
  select team, count(*) as match_count
  from (
    select filename, team
    from match_registry
    where team is not null
    group by filename, team)
  group by team
)

select season, gender, team, match_count, team_win_count as wins, match_count - team_win_count as loss, round(100*team_win_count/match_count, 2) as winning_percentage
from team_wins as tw
join team_matches as tm using (team)
order by season, gender, winning_percentage desc;

-- Highest win % for 2019
with team_wins as(
  select season, gender, winner as team, count(*) as team_win_count
  from match_info as mi
  where winner is not null
  group by season, gender, winner
),

team_matches as (
  select team, count(*) as match_count
  from (
    select filename, team
    from match_registry
    where team is not null
    group by filename, team)
  group by team
),

agg as (
  select season, gender, team, match_count, team_win_count as wins, match_count - team_win_count as loss, round(100*team_win_count/match_count, 2) as winning_percentage
  from team_wins as tw
  join team_matches as tm using (team)
  where season = '2019'
  order by season, gender, winning_percentage desc
)

select season, gender, team, match_count, wins, loss, winning_percentage
from (
  select *, ROW_NUMBER() OVER (PARTITION BY gender ORDER BY winning_percentage DESC) AS rn
  from agg
)
where rn = 1;

-- Highest strike rate
-- Honestly could not figure this out