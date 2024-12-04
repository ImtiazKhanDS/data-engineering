-- Building a datelist data type

select * from events;


select max(event_time), min(event_time) from events;


with yesterday as (

	select * from users_cumulated
		where date = Date('2022-12-31')
),

today as (
  select 
  	user_id, 
  from events 
  where DATE(cast(event_time as Timestamp)) = DATE('2023-01-01')
)


drop table users_cumulated ;
create table users_cumulated (
	user_id TEXT,
	-- This list of dates in the past where the user was active
	dates_active DATE[],
	-- The  current date for the user
	date Date,
	primary key(user_id, date)
	
)



insert into users_cumulated 
with yesterday as (

	select * from users_cumulated
		where date = Date('2022-12-31')
),

today as (
  select 
  	cast(user_id as text) as user_id, 
  	DATE(cast(event_time as Timestamp)) as date_active
  from events 
  where DATE(cast(event_time as Timestamp)) = DATE('2023-01-01') and user_id is not null
  group by user_id , DATE(cast(event_time as timestamp))
)

select 
	coalesce(t.user_id, y.user_id) as user_id,
	case
		when y.dates_active is null then array[t.date_active] 
		when t.date_active is null then y.dates_active
		else array[t.date_active]  || y.dates_active
	end as dates_active,
	coalesce(t.date_active, y.date + interval '1 day') as date
	
from today t 
full outer join yesterday y
	on t.user_id = y.user_id

	
select * from users_cumulated;



-- do it for other years for all the days till 2023-01-31

insert into users_cumulated 
with yesterday as (

	select * from users_cumulated
		where date = Date('2023-01-30')
),

today as (
  select 
  	cast(user_id as text) as user_id, 
  	DATE(cast(event_time as Timestamp)) as date_active
  from events 
  where DATE(cast(event_time as Timestamp)) = DATE('2023-01-31') and user_id is not null
  group by user_id , DATE(cast(event_time as timestamp))
)

select 
	coalesce(t.user_id, y.user_id) as user_id,
	case
		when y.dates_active is null then array[t.date_active] 
		when t.date_active is null then y.dates_active
		else array[t.date_active]  || y.dates_active
	end as dates_active,
	coalesce(t.date_active, y.date + interval '1 day') as date
	
from today t 
full outer join yesterday y
	on t.user_id = y.user_id


	
-- create a datelist now
	
select * from generate_series(DATE('2023-01-01'), DATE('2023-01-31'), interval '1 day');

with users as (

	 select * from users_cumulated
	 	where date = DATE('2023-01-31')
),
	series as (
		select * 
		from generate_series(DATE('2023-01-01'), DATE('2023-01-31'), interval '1 day') as series_date
	),
	
	place_holder_ints as (
		select *,
		 		case 
			 		when dates_active @> ARRAY[DATE(series_date)] then cast(POW(2, 32-(date - DATE(series_date))) as bigint)
			 		else 0
		 		 end as placeholder_int_value
		 
		from users cross join series
	)

select user_id, 
		cast(cast(sum(placeholder_int_value) as bigint) as bit(32)),
		bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_monthly_active,
		bit_count(cast('11111110000000000000000000000000' as bit(32)) & cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_weekly_active,
		bit_count(cast('10000000000000000000000000000000' as bit(32)) & cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_daily_active
from place_holder_ints
group by user_id order by dim_monthly_active desc;