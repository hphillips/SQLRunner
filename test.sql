create local temporary table rankings as 
select
	*,
	row_number() over(order by wins/(wins+losses) desc, map_wins-map_losses desc) as rank
from (
	select
		team1 as team,
		count(case when points1>points2 then 1 end) as wins,
		count(case when points1<points2 then 1 end) as losses,
		sum(maps1) as map_wins,
		sum(maps2) as map_losses
	from overwatch.matches
	where is_regular_season
		and stage='Stage $stage$'
	group by 1
) as a
order by rank
;


select
	team1 as team,
	team2,
	maps1||'-'||maps2 as score
from overwatch.matches as a
inner join rankings as b on (a.team1=b.team)
inner join rankings as c on (a.team2=c.team)
where stage='Stage 2'
	and state='CONCLUDED' and is_regular_season
order by b.rank, c.rank
;

<pivot>
	select
		team1 as team,
		team2,
		maps1||'-'||maps2 as score,
		rank as rank
	from overwatch.matches as a
	inner join rankings as b on (a.team1=b.team)
	where stage='Stage 2'
		and state='CONCLUDED' and is_regular_season
</pivot>

<pivot>
	select
		team1 as team,
		team2,
		maps1||'-'||maps2 as score,
		rank as rank
	from overwatch.matches as a
	inner join rankings as b on (a.team1=b.team)
	where stage='Stage 2'
		and state='CONCLUDED' and is_regular_season
<by>
	select
		team
	from rankings
</pivot>



