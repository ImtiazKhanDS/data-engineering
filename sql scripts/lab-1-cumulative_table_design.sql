select * from player_seasons ps ;

-- In this table we have players and for each player we have a temporal component i.e seasons here , 
-- One problem about this table would be if have to use this in downstream tasks then we would lose compression
-- due to shuffling of values. So our goal here is to create one row per player so that all the season information
-- is stored in an array and if someone uses this table in down stream task compression is not lost.
-- To start with look at what things change in the player perspective and what remains constant
-- Things that remain same for example are player_name, height, country , draft_year, draft_round, draft_number etc.
-- Things that change are gp, pts, reb, ast, season etc.


-- We need to create table a array of structs of the temporal component and all fixed player variables in a table.

create type season_stats as (
		season INTEGER,
		gp INTEGER,
		pts real,
		reb real,
		ast real
)


create table players(
player_name text,
height text,
college text,
country text,
draft_year text,
draft_round text,
draft_number text,
season_stats season_stats[],
current_season INTEGER , -- This is the season value , we need this value to cumulatively build this table.
primary key(player_name, current_season)
)

-- Now lets implement the full outer join logic

-- Lets find out the min season from all the seasons

select min(ps.season) from player_seasons ps; -- which is 1996

-- With the help of cte we show now how today and yesterday query works

with yesterday as (
	select * from players
	where current_season = 1995
	),
	today as (select * from player_seasons
			  where season = 1996
	)
	
select * from today t full outer join yesterday y on t.player_name = y.player_name;

-- If you run the above query, what you will observe is that all the yesterday data is null
-- For the variables that dont change we will use coalesce to get the first not null value
-- Coalesce returns the first non-null value in a list. If all the values in the list are NULL, 
-- then the function returns null.

with yesterday as (
	select * from players
	where current_season = 1995
	),
	today as (select * from player_seasons
			  where season = 1996
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number
from today t full outer join yesterday y 
on t.player_name = y.player_name;

-- "select * from players where current_season = 1995" This query is called the seed query.
-- Now lets add the season stats within the same query

with yesterday as (
	select * from players
	where current_season = 1995
	),
	today as (select * from player_seasons
			  where season = 1996
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;

-- Now we can create a pipeline where we can push the data to the players table

insert into players 

with yesterday as (
	select * from players
	where current_season = 1995
	),
	today as (select * from player_seasons
			  where season = 1996
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;


select player_name ,season_stats, current_season from players;

-- What we do now we just fiddle with yesterday and today season values for 1996 and 1997


insert into players 

with yesterday as (
	select * from players
	where current_season = 1996
	),
	today as (select * from player_seasons
			  where season = 1997
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;


select player_name ,season_stats, current_season from players where current_season = 1997;

-- In here you will see players under the season stats , who
-- 1. have played both seasons, 2. just joined in this season 3.retired in 1996 as well.
-- Now moving on lets run this pipeline for more data 1997 to 2001

insert into players 

with yesterday as (
	select * from players
	where current_season = 1997
	),
	today as (select * from player_seasons
			  where season = 1998
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;

select player_name ,season_stats, current_season from players where current_season = 1998;



insert into players 

with yesterday as (
	select * from players
	where current_season = 1998
	),
	today as (select * from player_seasons
			  where season = 1999
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;

select player_name ,season_stats, current_season from players where current_season = 1999;


insert into players 

with yesterday as (
	select * from players
	where current_season = 1999
	),
	today as (select * from player_seasons
			  where season = 2000
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;

select player_name ,season_stats, current_season from players where current_season = 2000;


insert into players 

with yesterday as (
	select * from players
	where current_season = 2000
	),
	today as (select * from player_seasons
			  where season = 2001
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;

select player_name ,season_stats, current_season from players where current_season = 2001;

-- Michael Jordan all time greats, lets query about him and get insights

select * from players where current_season = 2001 and player_name = 'Michael Jordan'


-- Michael Jordan had a gap, he played 1996, 1997 and then came back and played in 2001

-- With the below query , we can go back to the actual data and its very powerful
with unnested as (
	select 
	player_name ,
	unnest(season_stats)::season_stats as season_stats
	from players where current_season = 2001 and player_name = 'Michael Jordan'
	)
	
select player_name, (season_stats::season_stats).* from unnested;

-- We get to know the versatility of all the data and also get the facts of a particular player
-- We also can do this for all data , but the main advantage is the data within a player is sorted. 
-- This is the way you ensure compression of the data because it keeps the temporal component together.

with unnested as (
	select 
	player_name ,
	unnest(season_stats)::season_stats as season_stats
	from players where current_season = 2001
	)
select player_name, (season_stats::season_stats).* from unnested;



select * from players where current_season = 2001

-- Now we want to create a scoring_class and years since last season to be added.

create type scoring_class as enum('star', 'good', 'average', 'bad');

drop table players;

create table players(
player_name text,
height text,
college text,
country text,
draft_year text,
draft_round text,
draft_number text,
season_stats season_stats[],
scoring_class scoring_class,
years_since_last_season INTEGER,
current_season INTEGER , -- This is the season value , we need this value to cumulatively build this table.
primary key(player_name, current_season)
);


insert into players 

with yesterday as (
	select * from players
	where current_season = 1995
	),
	today as (select * from player_seasons
			  where season = 1996
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	
	case when t.season is not null then
		case when t.pts>20 then 'star'
			 when t.pts>15 then 'good'
			 when t.pts>10 then 'average'
			 else 'bad'
		end::scoring_class
		
		else y.scoring_class
	end as scoring_class,
	
	case 
		when t.season is not null then 0  -- if the current season is not null then years since last season is zero
		else y.years_since_last_season+1  -- if null then you add 1 year since that player is not participating in the current season
	end as years_since_last_season,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;


select player_name ,season_stats, current_season from players;


 -- 1996 to 1997
insert into players
with yesterday as (
	select * from players
	where current_season = 1996
	),
	today as (select * from player_seasons
			  where season = 1997
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	
	case when t.season is not null then
		case when t.pts>20 then 'star'
			 when t.pts>15 then 'good'
			 when t.pts>10 then 'average'
			 else 'bad'
		end::scoring_class
		
		else y.scoring_class
	end as scoring_class,
	
	case 
		when t.season is not null then 0  -- if the current season is not null then years since last season is zero
		else y.years_since_last_season+1  -- if null then you add 1 year since that player is not participating in the current season
	end as years_since_last_season,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;


select player_name ,season_stats, current_season from players;



-- 1997 to 1998
insert into players
with yesterday as (
	select * from players
	where current_season = 1997
	),
	today as (select * from player_seasons
			  where season = 1998
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	
	case when t.season is not null then
		case when t.pts>20 then 'star'
			 when t.pts>15 then 'good'
			 when t.pts>10 then 'average'
			 else 'bad'
		end::scoring_class
		
		else y.scoring_class
	end as scoring_class,
	
	case 
		when t.season is not null then 0  -- if the current season is not null then years since last season is zero
		else y.years_since_last_season+1  -- if null then you add 1 year since that player is not participating in the current season
	end as years_since_last_season,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;


select player_name ,season_stats, current_season from players;



-- 1998 to 1999
insert into players
with yesterday as (
	select * from players
	where current_season = 1998
	),
	today as (select * from player_seasons
			  where season = 1999
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	
	case when t.season is not null then
		case when t.pts>20 then 'star'
			 when t.pts>15 then 'good'
			 when t.pts>10 then 'average'
			 else 'bad'
		end::scoring_class
		
		else y.scoring_class
	end as scoring_class,
	
	case 
		when t.season is not null then 0  -- if the current season is not null then years since last season is zero
		else y.years_since_last_season+1  -- if null then you add 1 year since that player is not participating in the current season
	end as years_since_last_season,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;


select player_name ,season_stats, current_season from players;




-- 1999 to 2000
insert into players

with yesterday as (
	select * from players
	where current_season = 1999
	),
	today as (select * from player_seasons
			  where season = 2000
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	
	case when t.season is not null then
		case when t.pts>20 then 'star'
			 when t.pts>15 then 'good'
			 when t.pts>10 then 'average'
			 else 'bad'
		end::scoring_class
		
		else y.scoring_class
	end as scoring_class,
	
	case 
		when t.season is not null then 0  -- if the current season is not null then years since last season is zero
		else y.years_since_last_season+1  -- if null then you add 1 year since that player is not participating in the current season
	end as years_since_last_season,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;


select player_name ,season_stats, current_season from players;



-- 2000 to 2001
insert into players
with yesterday as (
	select * from players
	where current_season = 2000
	),
	today as (select * from player_seasons
			  where season = 2001
	)
	
select 
	coalesce(t.player_name, y.player_name) as player_name,
	coalesce(t.height, y.height) as height,
	coalesce(t.college, y.college) as college,
	coalesce(t.country, y.country) as country,
	coalesce(t.draft_year, y.draft_year) as draft_year,
	coalesce(t.draft_round, y.draft_round) as draft_round,
	coalesce(t.draft_number, y.draft_number) as draft_number,
	case when y.season_stats is null
		 then array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 when t.season is not null then y.season_stats || array[row(
		 				t.season,
		 				t.gp,
		 				t.pts,
		 				t.reb,
		 				t.ast)::season_stats
		 			]
		 -- This is the case when a player is retired and we dont want the current season nulls to the data
		 else y.season_stats
	end as season_stats,
	
	case when t.season is not null then
		case when t.pts>20 then 'star'
			 when t.pts>15 then 'good'
			 when t.pts>10 then 'average'
			 else 'bad'
		end::scoring_class
		
		else y.scoring_class
	end as scoring_class,
	
	case 
		when t.season is not null then 0  -- if the current season is not null then years since last season is zero
		else y.years_since_last_season+1  -- if null then you add 1 year since that player is not participating in the current season
	end as years_since_last_season,
	coalesce(t.season, y.current_season+1) as current_season

from today t full outer join yesterday y 
on t.player_name = y.player_name;


select player_name ,season_stats, current_season from players

select * from players where current_season = 2001

-- Now if you query for michael jordan for year 2001 , years since last season will  be 0 , but when
-- we change that to 2000 , the years since will be 3 years since there is gap.

select * from players where current_season = 2000 and player_name = 'Michael Jordan'
select * from players where current_season = 2001 and player_name = 'Michael Jordan'



-- Analytics
-- To see which player had the biggest improvement from their first season to their most recent season.

select player_name, 
		(season_stats[cardinality(season_stats)]::season_stats).pts/ case when (season_stats[1]::season_stats).pts = 0 then 1 else (season_stats[1]::season_stats).pts end as improvement
from players
where current_season = 2001
order by 2 desc;

-- This cumulative pattern is very powerful, it gives access to historical analysis and is very fast
-- There is no groupby , this can be easily parellizable and its crazy fast.






 