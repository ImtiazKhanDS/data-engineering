-- Graph Network of NBA players to know which players play with each other
-- in games and which players play against each other
-- Which NBA players are part of which team at which times ?


-- The ARRAY_AGG function in PostgreSQL is an aggregate function that 
-- combines values from multiple rows into an array.
-- ARRAY_AGG(expression [ORDER BY sorting_expression])

/*
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



department	employees
HR			{Alice, Charlie}
IT			{Bob, Eve}
Finance		{David}
    */


-- vertices and edges
create type vertex_type as enum('player', 'team', 'game');


create table vertices (
	identifier text,
	type vertex_type,
	properties JSON,
	primary key(identifier, type)
)



create type edge_type as enum('plays_against',
								'shares_team',
								'plays_in',
								 'plays_on')
								 
create table edges (
	subject_identifier text,
	subject_type vertex_type,
	object_identifier text,
	object_type vertex_type,
	edge_type edge_type,
	properties json,
	
	primary key(subject_identifier, 
				subject_type,
				object_identifier,
				object_type,
				edge_type)
)

-- game types
insert into vertices
select game_id as identifier,
	'game'::vertex_type as type,
	json_build_object(
			'pts_home', pts_home,
			'pts_away', pts_away,
			'winning_team' , case when home_team_wins = 1 then home_team_id else visitor_team_id end
	) as properties

from games

select * from vertices;


-- player types
insert into vertices
with players_agg as (

select
	player_id as identifier,
	max(player_name) as player_name ,
	count(1) as number_of_games,
	sum(pts) as total_points,
	ARRAY_AGG(distinct team_id) as teams
from game_details
group by player_id
)

select 
	identifier, 'player'::vertex_type,
	json_build_object(
		'player_name', player_name,
		'number_of_games', number_of_games,
		'total_points', total_points,
		'teams', teams
	) as properties
	from players_agg




-- teams type
insert into vertices

with teams_deduped as (

select *, row_number() over (partition by team_id) as row_num
	from teams
)

select 
	team_id as identifier,
	'team'::vertex_type as type,
	jsonb_build_object(
		'abbreviation', abbreviation,
		'nickname', nickname,
		'city', city,
		'arena', arena,
		'year_founded', yearfounded
	) as properties
	
from teams_deduped where row_num = 1



-- Querying


select type , count(1) from vertices group by 1;


-- edge plays_in
insert into edges

with deduped as (

select * , row_number() over (partition by player_id, game_id) as row_num

	from game_details

)

select 
	player_id as subject_identifier,
	'player'::vertex_type as subject_type,
	game_id as object_identifier,
	'game'::vertex_type as object_type,
	'plays_in'::edge_type as edge_type,
	json_build_object(
	
		'start_position', start_position,
		'pts', pts,
		'team_id', team_id,
		'team_abbreviation', team_abbreviation
	) as properties
	

from deduped where row_num = 1



select * from edges;


-- example vertex and edge query , analytical

select v.properties->>'player_name',max(v.type),
		max(e.edge_type), max(e.properties->>'team_abbreviation'),
		max(e.properties->>'pts')
	from vertices v
	join edges e
	on e.subject_identifier = v.identifier 
	and e.subject_type = v.type
group by 1
order by 5 desc


insert into edges
with deduped as (

select * , row_number() over (partition by player_id, game_id) as row_num

	from game_details

),

	filtered as (
	
		select * from deduped where row_num = 1
	),

aggregated as (

	select  
		f1.player_id as subject_player_id,
		f2.player_id as object_player_id, 
		case when f1.team_abbreviation = f2.team_abbreviation
			 then 'shares_team'::edge_type
			 else 'plays_against'::edge_type
		end as edge_type,
		max(f1.player_name) as subject_player_name, 
		max(f2.player_name) as object_player_name, 
		COUNT(1) as num_games,
		sum(f1.pts) as subject_points,
		sum(f2.pts) as object_points

from filtered f1 join filtered f2
	on f1.game_id = f2.game_id
	and f1.player_name <> f2.player_name
	where f1.player_id > f2.player_id -- so that we dont have double edges here.

group by f1.player_id,
		 f2.player_id, 
		case when f1.team_abbreviation = f2.team_abbreviation
			 then 'shares_team'::edge_type
			 else 'plays_against'::edge_type
		end
)	

select 
	subject_player_id as subject_identifier,
	'player'::vertex_type as subject_type,
	object_player_id as object_identifier,
	'player'::vertex_type as object_type,
	edge_type as edge_type,
	json_build_object(
		'num_games', num_games,
		'subject_points', subject_points,
		'object_points', object_points
	)
from aggregated;




-- Analytical query, what is the total points average of a player and 
-- what is the points average of a player when he is 
-- playing against a specific player ( is it greater than career average or lower)
select 

	v.properties->>'player_name' as player,
	e.object_identifier as player_against,
	cast(v.properties->>'total_points' as real)/cast(v.properties->>'number_of_games' as real) as career_avg,
	--cast(v.properties->>'number_of_games' as real)/ case when cast(v.properties->>'total_points' as real) = 0 then 1 else cast(v.properties->>'total_points' as real) end as career_avg,
	cast(e.properties->>'subject_points'as real)/cast(e.properties->>'num_games' as real) as against_player_avg
	--e.properties->>'subject_points',
	--e.properties->>'num_games'

from vertices v join edges e
on v.identifier = e.subject_identifier 
and v.type = e.subject_type where e.object_type = 'player'::vertex_type



