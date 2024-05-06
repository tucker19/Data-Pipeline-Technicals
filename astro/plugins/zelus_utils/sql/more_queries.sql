select filename, team, inning, over,
  array_agg(batter) filter (WHERE batter IS NOT NULL) as batters,
  array_agg(bowler) filter (WHERE bowler IS NOT NULL) as bowlers,
  array_agg(non_striker) filter (WHERE non_striker IS NOT NULL) as non_strikers,
  sum(COALESCE(runs_batter,0)) as total_runs_batter,
  sum(coalesce(runs_extras,0)) as total_runs_extras,
  sum(coalesce(run_total,0)) as overall_run_total,
  array_agg(kind) filter (WHERE kind IS NOT NULL) as kinds,
  array_agg(player_out) filter (WHERE player_out IS NOT NULL) as players_out,
  array_agg(fielders) filter (WHERE fielders IS NOT NULL) as fielders_array,
  array_agg(forfeited) filter (WHERE forfeited IS NOT NULL) as forfeited,
  array_agg(super_over) filter (WHERE super_over IS NOT NULL) as super_over
from innings
group by filename, team, inning, over
order by filename, inning, over, team;

select filename,
  count(distinct(inning)) as total_num_of_innings,
  count(distinct(over)) as total_num_of_overs,
  count(distinct(batter)) as total_baters,
  count(distinct(bowler)) as total_bowlers,
  count(distinct(non_striker)) as total_non_strikers,
  sum(COALESCE(runs_batter,0)) as total_runs_batter,
    sum(coalesce(runs_extras,0)) as total_runs_extras,
    sum(coalesce(run_total,0)) as overall_run_total,
  array_agg(kind) filter (WHERE kind IS NOT NULL) as kinds,
  count(fielders) as total_fielders,
  sum(case when forfeited is not null then 1 else 0 end) as total_forfeits,
  sum(case when super_over is not null then 1 else 0 end) as total_super_overs
from innings
group by filename
order by filename;

select team, count(distinct(name)) as total_distinct_players
from match_registry
group by team
order by team;

select winner as team, count(*) as team_win_count
from match_info as mi
where winner is not null
group by winner
order by team_win_count desc;

select team, count(*) as match_count
from (
  select filename, team
  from match_registry
  where team is not null
  group by filename, team)
group by team
order by match_count desc;

with team_wins as(
  select winner as team, count(*) as team_win_count
  from match_info as mi
  where winner is not null
  group by winner
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

select team, match_count, team_win_count as wins, match_count - team_win_count as loss, round(100*team_win_count/match_count, 2) as winning_percentage
from team_wins as tw
join team_matches as tm using (team)
order by winning_percentage desc
having ;