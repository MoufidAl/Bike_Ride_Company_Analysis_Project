-- The steps I followed to begin the analysis:

-- 1. Download the individual CSV documents (I had 12 files for each month of the year).
-- 2. Import each into separate tables.
-- 3. Combine them all into one table.
-- 4. Inspect the data for any anomolies/ items that need to be cleaned up.
-- 5. Identify and exclude data with anomalies
-- 6. Create some queries for data visualisation.

-- combining data into one table

drop table Trips_Data;

create table Trips_Data as
(select *
from trips_Jan

union

select *
from trips_Feb
union

select *
from trips_Mar
union

select *
from trips_Apr
union

select *
from trips_May
union

select *
from trips_Jun
union

select *
from trips_Jul
union

select *
from trips_Aug
union

select *
from trips_Sep
union

select *
from trips_Oct
union

select *
from trips_Nov
union

select *
from trips_Dec
);

-------
-- I wanted to add a column to ease the calculation for the duration of the trips in the database. 

select count(1)
from Trips_Data;

-- calculating the length of the trips

select ended_at, started_at, (ended_at - started_at) * 1440, round((ended_at - started_at) * 1440) 
from Trips_Data;

update Trips_Data
set trip_length_mins = round((ended_at - started_at) * 1440);

-- Now that the data is in a format that is consitent and under one table. we can begin to look for any anomolies that will negatively impact the results of the analysis.

select distinct member_casual
from Trips_Data;

select min (end_lng), max(end_lng), min (end_lat), max(end_lat), min (start_lng), max(start_lng), min (start_lat), max(start_lat)
from Trips_Data;

-- At this point I am trying to make sure that any value that is NULL is excluded from the analysis (Null does not equal 0 or empty).

select end_station_id, end_station_name, count(1)
from Trips_Data
group by end_station_id, end_station_name;

select rideable_type, count(1)
from Trips_Data
group by rideable_type;

select ride_id, count(1)
from Trips_Data
group by ride_id
having count(1) > 1;

select *
from Trips_Data
where started_at is null and ended_at is null;

select *
from Trips_Data
where started_at is null or ended_at is null;

select *
from Trips_Data
where exclude is null
and (start_lat is null or end_lat is null);

select *
from Trips_Data
where exclude is null
and (start_lng is null or end_lng is null);

select *
from Trips_Data
where exclude is null
and member_casual is null;

select *
from Trips_Data
where exclude is null
and rideable_type is null;


-- I also realized that some start times were higher than end times so I needed to exclude cases where start time is greater than end time.

select count(1)
from Trips_Data
where started_at >= ended_at;

update Trips_Data
set exclude = 'Y'
where started_at >= ended_at;


--exclude cases where trip length less than or equal to 0
select *
from Trips_Data
order by trip_length_mins asc;

update Trips_Data
set exclude = 'Y'
where trip_length_mins <= 0;

select *
from Trips_Data
where exclude is null
order by trip_length_mins asc;

select trip_length_mins, count(1)
from Trips_Data
where exclude is null
group by trip_length_mins;

-- There we were also cases where the Trip lenght was equal to 0 or less than 0 which does not make any sense and would causes errors in the analysis.

-- we had to remove these cases before proceeding in doing any analysis.

select *
from Trips_Data
order by trip_length_mins asc;

update Trips_Data
set exclude = 'Y'
where trip_length_mins <= 0;

select *
from Trips_Data
where exclude is null
order by trip_length_mins asc;

select trip_length_mins, count(1)
from Trips_Data
where exclude is null
group by trip_length_mins;

-- at this point I wanted to do some final checkings before starting to visualize my data. 

select start_station_name, count(1)
from Trips_Data
where exclude is null
group by start_station_name;

select *
from Trips_Data
where exclude is null
and (upper(start_station_name) like '%BASE%WAREHOUSE%' or upper(end_station_name) like '%BASE%WAREHOUSE%');

update Trips_Data
set exclude = 'Y'
where exclude is null
and (upper(start_station_name) like '%BASE%WAREHOUSE%' or upper(end_station_name) like '%BASE%WAREHOUSE%');


-- lastly, it is time to create queuries so we can visualize the data more easily. 
-- the below queries : will cut, unionize, and group data in simpler formats so that visualization tools such as Tableau or even Excel can easily used. 

elect count(1)
from Trips_Data
where exclude is null;

select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
from Trips_Data
where exclude is null;


with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from Trips_Data
            where exclude is null)
            
    select member_casual, count(id)
    from dt
    group by member_casual;
    
    
with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from Trips_Data
            where exclude is null) 
            
    select member_casual, to_char(started_at, 'mon-yyyy'), count(id)
    from dt
    group by member_casual, to_char(started_at, 'mon-yyyy');       
    
    
with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from Trips_Data
            where exclude is null) 
            
    select member_casual, to_char(started_at, 'day'), count(id)
    from dt
    group by member_casual, to_char(started_at, 'day');     
    
    
with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from Trips_Data
            where exclude is null) 
            
    select member_casual, count(id)
    from dt
    where trip_length_mins >= 60 and trip_length_mins <= 119
    group by member_casual;
    



with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from Trips_Data
            where exclude is null) 
            
    select *
    from 
    (
        select 'Origin' origin_destination, start_station_name station_name, id path_id, start_lat lat, start_lng lng, member_casual
        from dt
        
        union
        
        select 'Destination' origin_destination, end_station_name station_name, id path_id, end_lat lat, end_lng lng, member_casual
        from dt
    )
    order by path_id;
    
    
with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from Trips_Data
            where exclude is null) 
            
    select rownum, i.*
    from (
           select member_casual, start_station_name, end_station_name, num, rank() over (partition by member_casual order by num desc) num_rank
           from (
            select member_casual, start_station_name, end_station_name, count(1) num
            from dt
            group by member_casual,  start_station_name, end_station_name
            ) 
    ) i
    where num_rank <= 30;
-- this orders it by num_rank
  
--  fetching the first 100 rows only
  
set define off;

with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from Trips_Data
            where exclude is null) 
            
    select distinct round(start_lat, 2), round(start_lng, 2)
    from dt
    where start_station_name = 'Streeter Dr & Grand Ave';
    
with dt as (select rownum id, rideable_type, started_at, ended_at, start_station_name, end_station_name, start_lat, start_lng, end_lat, end_lng, member_casual, trip_length_mins
            from Trips_Data
            where exclude is null) 
            
            select member_casual, start_station_name, end_station_name, count(1) num
            from dt
            group by member_casual,  start_station_name, end_station_name    ;
