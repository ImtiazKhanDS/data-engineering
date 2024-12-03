-- Fact Data modelling

select * from game_details gd ;


-- Checking if there are any duplicates at the level of (game_id, team_id, player_id)

select game_id, team_id, player_id , count(1)
from game_details group by 1, 2, 3
having count(1)>1


-- We want to create a filter to get rid of duplicates
-- This data is denormalized , there is no when column as well.
-- When you can join something cheaply , we dont want to keep that in the fact data

with deduped as (
	select  g.game_date_est,
			g.season,
			g.home_team_id,
			gd.*, 
			row_number() over (partition by gd.game_id, team_id, player_id order by g.game_date_est) as row_num 
		from game_details gd join games g on gd.game_id = g.game_id
)

select game_date_est as dim_game_date,
	   season as dim_season,
	   team_id as dim_team_id,
	   player_id as dim_player_id,
	   player_name as dim_player_name,
	   start_position as dim_start_position,
	   team_id = home_team_id as dim_is_playing_at_home,
	   coalesce(position('DNP' in comment),0)> 0 as dim_did_not_play, 
	   coalesce(position('DND' in comment),0)> 0 as dim_did_not_dress, 
	   coalesce(position('NWT' in comment),0)> 0 as dim_not_with_team, 
	   cast(split_part(min, ':', 1) as real ) + cast(split_part(min, ':', 2) as real)/60 as m_minutes,
	   fgm as m_fgm,
	   fga as m_fga,
	   fg3m as m_fg3m,
	   fg3a as m_fg3a,
	   ftm as m_ftm,
	   fta as m_fta,
	   oreb as m_oreb,
	   dreb as m_dreb,
	   reb as m_reb,
	   ast as m_ast,
	   stl as m_stl,
	   blk as m_blk,
	   "TO" as m_turnovers,
	   pf as m_pf,
	   pts as m_pts,
	   plus_minus as m_plus_minus
from deduped where row_num = 1


-- Create fact table now
-- naming convention when you start a column with dim_ , these are the values which you should filter on
-- when you start a column with m_ , these are the columns you should do math on.
create table fct_game_details(

	dim_game_date DATE,
	dim_season integer,
	dim_team_id integer,
	dim_player_id integer,
	dim_player_name text,
	dim_start_position text,
	dim_did_not_play boolean,
	dim_did_not_dress boolean,
	dim_not_with_team boolean,
	dim_is_playing_at_home boolean,
	m_minutes real,
	m_fgm integer,
	m_fga integer,
	m_fg3m integer,
	m_fg3a integer,
	m_ftm integer,
	m_fta integer,
	m_oreb integer,
	m_dreb integer,
	m_reb integer,
	m_ast integer,
	m_stl integer,
	m_blk integer,
	m_turnovers integer,
	m_pf integer,
	m_pts integer,
	m_plus_minus integer,
	primary key(dim_game_date, dim_team_id, dim_player_id)
)
	

insert into fct_game_details
with deduped as (
	select  g.game_date_est,
			g.season,
			g.home_team_id,
			gd.*, 
			row_number() over (partition by gd.game_id, team_id, player_id order by g.game_date_est) as row_num 
		from game_details gd join games g on gd.game_id = g.game_id
)

select game_date_est as dim_game_date,
	   season as dim_season,
	   team_id as dim_team_id,
	   player_id as dim_player_id,
	   player_name as dim_player_name,
	   start_position as dim_start_position,
	   team_id = home_team_id as dim_is_playing_at_home,
	   coalesce(position('DNP' in comment),0)> 0 as dim_did_not_play, 
	   coalesce(position('DND' in comment),0)> 0 as dim_did_not_dress, 
	   coalesce(position('NWT' in comment),0)> 0 as dim_not_with_team, 
	   cast(split_part(min, ':', 1) as real ) + cast(split_part(min, ':', 2) as real)/60 as m_minutes,
	   fgm as m_fgm,
	   fga as m_fga,
	   fg3m as m_fg3m,
	   fg3a as m_fg3a,
	   ftm as m_ftm,
	   fta as m_fta,
	   oreb as m_oreb,
	   dreb as m_dreb,
	   reb as m_reb,
	   ast as m_ast,
	   stl as m_stl,
	   blk as m_blk,
	   "TO" as m_turnovers,
	   pf as m_pf,
	   pts as m_pts,
	   plus_minus as m_plus_minus
from deduped where row_num = 1


select * from fct_game_details

-- we can just join to get more more team information

select t.*, gd.* from fct_game_details gd join teams t on t.team_id = gd.dim_team_id;

-- Find out the player who bailed out many times

select 
	dim_player_name, 
	count(1) as num_games, 
	count(case when dim_not_with_team then 1 end) ,
	cast(count(case when dim_not_with_team then 1 end) as real)*100/count(1) as bail_percentage
from fct_game_details
group by 1 order by 4 desc;

-- Fact data modelling bottomline : Can you build datasets to answer these type of questions really quickly ?





