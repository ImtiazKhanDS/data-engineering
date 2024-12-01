-- SCD - Slowly changing dimension type 2 - idempotent.
-- UNION and UNION ALL in SQL are used to retrieve data from two or more tables. UNION returns distinct records from both tables, while UNION ALL returns all the records from both tables.
-- The ARRAY_REMOVE() function in PostgreSQL is used to eliminate all elements equal to a specified value 
-- from a one-dimensional array. This function is particularly useful for cleaning up arrays by
-- removing unwanted or duplicate elements.
-- SELECT array_remove(ARRAY[1,2,3,2], 2); output :  {1,3}
/*
 
 arr_agg example 
 
 
 CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50)
);

INSERT INTO employees (name, department) VALUES
('Alice', 'HR'),
('Bob', 'IT'),
('Charlie', 'HR'),
('David', 'Finance'),
('Eve', 'IT');

SELECT department, ARRAY_AGG(name) AS employees
FROM employees
GROUP BY department;


department	 	employees

  HR			  {Alice, Charlie}
  IT			  {Bob, Eve}
  Finance		  {David}
  
 */


/*
 
 A window function in SQL is a type of function that allows us to perform calculations across a specific set of rows related to the current row. 
 These calculations happen within a defined window of data, and they are particularly useful for aggregates, rankings, and cumulative totals without altering the dataset.

The OVER clause is key to defining this window. 
It partitions the data into different sets 
(using the PARTITION BY clause) and orders them (using the ORDER BY clause). 
These windows enable functions like 
SUM(), AVG(), ROW_NUMBER(), RANK(), and DENSE_RANK() to be applied 
in a sophisticated manner. 


SELECT column_name1, 
 window_function(column_name2)
 OVER([PARTITION BY column_name3] [ORDER BY column_name4]) AS new_column
FROM table_name;

Key Terms

window_function= any aggregate or ranking function
column_name1= column to be selected
column_name2= column on which window function is to be applied
column_name3= column on whose basis partition of rows is to be done
new_column= Name of new column
table_name= Name of table


Name	Age	Department	Salary
Ramesh	20	Finance		50,000
Deep	25	Sales		30,000
Suresh	22	Finance		50000
Ram		28	Finance		20,000
Pradeep	22	Sales		20,000


SELECT Name, Age, Department, Salary, 
 AVG(Salary) OVER( PARTITION BY Department) AS Avg_Salary
 FROM employee
 
 
 Name	Age	Department	Salary	Avg_Salary
Ramesh	20	Finance		50,000	40,000
Suresh	22	Finance		50,000	40,000
Ram		28	Finance		20,000	40,000
Deep	25	Sales		30,000	25,000
Pradeep	22	Sales		20,000	25,000
  */

drop table players;

CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     season_stats season_stats[],
     scoring_class scoring_class,
     years_since_last_active INTEGER,
     current_season INTEGER,
	 is_active BOOLEAN,
     PRIMARY KEY (player_name, current_season)
 );

-- select * from players 
insert into players 
with years as (
	select *
	from generate_series(1996, 2022) as season
),
p as (
	select player_name , MIN(season) as first_season 
	from player_seasons 
	group by player_name 
),
players_and_seasons as (
	select * 
	from p
	join years y 
	on p.first_season <= y.season
),
windowed as (
	select 
	ps.player_name, ps.season,
	array_remove(
	array_agg(case 
		when p1.season is not null then 
		cast(row(p1.season, p1.gp, p1.pts, p1.reb, p1.ast) as season_stats)
		end
		)
	over (partition by ps.player_name order by coalesce(p1.season, ps.season)) 
	,null
) 
as seasons
	from players_and_seasons ps
	left join player_seasons p1
	on ps.player_name = p1.player_name and ps.season = p1.season
	order by ps.player_name, ps.season
)
,static as ( 
	select player_name,
	max(height) as height,
	max(college) as college,
	max(country) as country,
	max(draft_year) as draft_year,
	max(draft_round) as draft_round,
	max(draft_number) as draft_number
	from player_seasons ps 
	group by player_name
	)
	
select 
	w.player_name, 
	s.height,
	s.college,
	s.country,
	s.draft_year,
	s.draft_number,
	s.draft_round,
	seasons as season_stats
--	,( seasons[cardinality(seasons)]).pts
	,case 
	when (seasons[cardinality(seasons)]).pts > 20 then 'star'
	when (seasons[cardinality(seasons)]).pts > 15 then 'good'
	when (seasons[cardinality(seasons)]).pts > 10 then 'average'
	else 'bad'
	end :: scoring_class as scorring_class
	,w.season - (seasons[cardinality(seasons)]).season as years_since_last_season
	,w.season as current_season
	,(seasons[cardinality(seasons)]).season = w.season as is_active
from windowed w 
join static s
on w.player_name = s.player_name;

select * from players;


create type scd_type as (

scoring_class scoring_class,
is_active boolean,
start_season integer,
end_season integer

)
-- We want model a scd of type 2
create table players_scd (

	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	current_season integer,
	start_season integer,
	end_season integer,
	primary key(player_name, start_season)
)

-- What is the streak of a player

select player_name, scoring_class, is_active from players;

-- we use window function to check the previous

with with_previous as (
select 
	player_name, 
	scoring_class,
	lag(scoring_class, 1) over (partition by player_name order by current_season) as previous_scoring_class,
	lag(is_active, 1) over (partition by player_name order by current_season) as previous_is_active,
	is_active 
from players
)

-- we can create an indicator whether or not it changed.

select *, 
case 
	when scoring_class <> previous_scoring_class then 1 
	else 0 
end as scoring_class_change_indicator ,

case 
	when is_active <> previous_is_active then 1 
	else 0 
end as is_active_change_indicator

from with_previous


-- Now combine these into one single indicator

with with_previous as (
select 
	player_name, 
	scoring_class,
	lag(scoring_class, 1) over (partition by player_name order by current_season) as previous_scoring_class,
	lag(is_active, 1) over (partition by player_name order by current_season) as previous_is_active,
	is_active,
	current_season
from players
),

with_indicators as (
select *, 
case 
	when scoring_class <> previous_scoring_class then 1 
	when is_active <> previous_is_active then 1 
	else 0 
end as change_indicator

from with_previous

),

with_streaks as (

select *, 
sum(change_indicator) 
over (partition by player_name order by current_season) 
as streak_identifier 
from with_indicators

)

select player_name, 
		streak_identifier,
		is_active,
		scoring_class,
		min(current_season) as start_season,
		max(current_season) as end_season,
		2021 as current_season
		
		from with_streaks
		
		group by player_name, streak_identifier, is_active, scoring_class
		order by player_name, streak_identifier;



-- We want to essentiially sum this up so that we can see the streak.

insert into players_scd 

with with_previous as (
select 
	player_name,
	current_season,
	scoring_class,
	is_active,
	lag(scoring_class, 1) over (partition by player_name order by current_season) as previous_scoring_class,
	lag(is_active, 1) over (partition by player_name order by current_season) as previous_is_active
	
from players
where current_season <= 2021
),

with_indicators as (
select *, 
case 
	when scoring_class <> previous_scoring_class then 1 
	when is_active <> previous_is_active then 1 
	else 0 
end as change_indicator

from with_previous

),

with_streaks as (

select *, 
sum(change_indicator) 
over (partition by player_name order by current_season) 
as streak_identifier 
from with_indicators

)

select player_name, 
		scoring_class,
		is_active,
		2021 as current_season,
		min(current_season) as start_season,
		max(current_season) as end_season
		from with_streaks
		group by player_name, streak_identifier, is_active, scoring_class
		order by player_name, streak_identifier;

select * from players_scd ps ;

-- The big expensive parts of this query are just window functions and in the very end big groupby.

with last_season_scd as (
	select * from players_scd
	where current_season = 2021
	and end_season = 2021
),

historical_scd as (
	select * from players_scd 
	where current_season = 2021
	and end_season<2021
),

this_season_data as (
	select * from players 
	where current_season = 2022
)

select ts.player_name,
	   ts.scoring_class,
	   ts.is_active,
	   ls.scoring_class,
	   ls.is_active
from this_season_data ts
left join last_season_scd ls
on ls.player_name = ts.player_name


--So in here will have changed records and unchanged records


with last_season_scd as (
	select * from players_scd
	where current_season = 2021
	and end_season = 2021
),

historical_scd as (
	select 
		player_name,
		scoring_class,
		is_active,
		start_season,
		end_season
	from players_scd 
	where current_season = 2021
	and end_season<2021
),

this_season_data as (
	select * from players 
	where current_season = 2022
),

unchanged_records as (

select ts.player_name,
	   ts.scoring_class,
	   ts.is_active,
	   ls.start_season,
	   ts.current_season as end_season
from this_season_data ts
join last_season_scd ls
on ls.player_name = ts.player_name
where ls.scoring_class = ts.scoring_class and ls.is_active = ts.is_active
),

changed_records as (

select ts.player_name,
	   unnest(ARRAY[
	   	row	(
	   			ls.scoring_class,
	   			ls.is_active,
	   			ls.start_season,
	   			ls.end_season
	   
	   		)::scd_type,
	   	
	   	row	(
	   			ts.scoring_class,
	   			ts.is_active,
	   			ts.current_season,
	   			ts.current_season
	   
	   		)::scd_type
	   	
	 ]) as records
	   
from this_season_data ts
left join last_season_scd ls
on ls.player_name = ts.player_name
where (ls.scoring_class <> ts.scoring_class 
		or ls.is_active <> ts.is_active)
	  
),

unnested_changed_records as (

	select 
		player_name ,
		(records::scd_type).scoring_class,
		(records::scd_type).is_active,
		(records::scd_type).start_season,
		(records::scd_type).end_season
	from changed_records
),


new_records as (

	select
		
		ts.player_name,
		ts.scoring_class,
		ts.is_active,
		ts.current_season as start_season,
		ts.current_season as end_season
	
		from this_season_data ts 
		left join last_season_scd ls
		on ts.player_name = ls.player_name
		where ls.player_name is null
)

select * from  historical_scd

union all

select * from unchanged_records

union all 

select * from unnested_changed_records

union all

select * from new_records







