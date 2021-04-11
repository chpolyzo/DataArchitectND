--create temperature stage
CREATE STAGE "YELP"."PUBLIC".temp 
URL = 's3://chrysanthibucket/las_vegas_temp.csv' 
CREDENTIALS = (AWS_KEY_ID = 'AKIAIWY64PEUDGUIMOBA' AWS_SECRET_KEY = '****************************************');

-- check LAS_VEGAS_TEMP stage, staged in an amazon s3 bucket
list @las_vegas_temp;

-- create an empty table to host date from the staged file
create or replace table temp  
(date timestamp,
  max_temp integer,
  min_temp integer,
  norm_max integer,
  norm_min integer);

-- transfer data from staged file to the table we have created
copy into temp from @las_vegas_temp
ON_ERROR = CONTINUE
file_format= (type = CSV);

-- check data validity using a selection query
select * from temp limit 20;


--create precipitation stage
CREATE STAGE "YELP"."PUBLIC".prec
URL = 's3://chrysanthibucket/las_vegas_pec.csv' 
CREDENTIALS = (AWS_KEY_ID = 'AKIAIWY64PEUDGUIMOBA' AWS_SECRET_KEY = '****************************************');

-- check LAS_VEGAS_PREC stage, staged in an amazon s3 bucket and connected with snowflake
list @las_vegas_prec;

-- create an empty table to host date from the staged file
create or replace table prec 
(date timestamp,
  precipitation integer,
  precipitation_normal integer)

-- transfer data from staged file to the table we have created
copy into prec from @las_vegas_prec
ON_ERROR = CONTINUE
file_format= (type = CSV);


-- check data validity using a selection query
select * from prec limit 20;

-- create one table for temperature and precipitation
select temp.date as dt, norm_max as temperature_max,
       precipitation_normal as precipitation
from yelp.public.temp 
join prec
    on prec.date = temp.date
where prec.precipitation OR temp.norm_max OR temp.norm_min is not null;

----------------------------------
----------------------------------
----- Working with Json Data -----
----------------------------------
-- stage six tables from amazon s3

--create review stage
CREATE STAGE "YELP"."PUBLIC".review 
URL = 's3://chrysanthibucket/yelp_academic_dataset_review.json' 
CREDENTIALS = (AWS_KEY_ID = 'AKIAIWY64PEUDGUIMOBA' AWS_SECRET_KEY = '****************************************');
--create business stage
CREATE STAGE "YELP"."PUBLIC".business 
URL = 's3://chrysanthibucket/yelp_academic_dataset_business.json' 
CREDENTIALS = (AWS_KEY_ID = 'AKIAIWY64PEUDGUIMOBA' AWS_SECRET_KEY = '****************************************');
--create checkin stage
CREATE STAGE "YELP"."PUBLIC".checkin
URL = 's3://chrysanthibucket/yelp_academic_dataset_checkin.json' 
CREDENTIALS = (AWS_KEY_ID = 'AKIAIWY64PEUDGUIMOBA' AWS_SECRET_KEY = '****************************************');
--create covid stage
CREATE STAGE "YELP"."PUBLIC".covid
URL = 's3://chrysanthibucket/yelp_academic_dataset_covid_features.json' 
CREDENTIALS = (AWS_KEY_ID = 'AKIAIWY64PEUDGUIMOBA' AWS_SECRET_KEY = '****************************************');
--create user stage
CREATE STAGE "YELP"."PUBLIC".user
URL = 's3://chrysanthibucket/yelp_academic_dataset_user.json' 
CREDENTIALS = (AWS_KEY_ID = 'AKIAIWY64PEUDGUIMOBA' AWS_SECRET_KEY = '****************************************');
--create photos stage
CREATE STAGE "YELP"."PUBLIC".pics
URL = 's3://chrysanthibucket/yelp_photos.tar' 
CREDENTIALS = (AWS_KEY_ID = 'AKIAIWY64PEUDGUIMOBA' AWS_SECRET_KEY = '****************************************');

-- check if files have been successfully staged

list @business;
list @checkin;
list @review;
list @covid;
list @user;
list @pics;

--------------------------------------------
-- create empty table for BUSINESS json data
create or replace table business (v variant);

-- fill business table with json staged data 
copy into business 
from @business 
file_format = (type=json);

-- check data validity using a selection query
select * from business limit 10;
-----------------------------------------------

-- create empty table for CHECKIN json data
create or replace table checkin (s variant);

-- fill checkin table with json staged data 
copy into checkin 
from @checkin 
file_format = (type=json);

-- check data validity using a selection query
select * from checkin limit 100;
----------------------------------------------

-- create empty table for REVIEW json data
create or replace table review (t variant);

-- copy staged data into review table
copy into review 
from @review 
file_format = (type=json);

-- select first 20 rows
select * from review limit 20;
--------------------------------------------


-- create empty table for COVID json data
create or replace table covid (v variant);

-- copy staged data into covid table
copy into covid
from @covid 
file_format = (type=json);


-- select first 20 rows
select * from covid limit 20;

-- create empty table for USERS json data
create or replace table users (v variant);

-- copy staged data into covid table
copy into users
from @user 
file_format = (type=json);

-- select first 20 rows
select * from users limit 20;

-- create empty table for USERS json data
create or replace table pics (v variant);

-- copy staged data into covid table
copy into users from @pics
ON_ERROR = CONTINUE
file_format = (type=json);

-- select first 20 rows
select * from pics limit 20;

-----------------------------------------
-- check our tables
-- BUSINESS
select
  v:adderess::string as address,
  v:business_id::string as business_id,
  v:categories::string as categories,
  v:city::string as city,
  v:latitude::float as latitude,
  v:longitude::float as longitude,
  v:name::string as name,
  v:postal_code::string as postal_code,
  v:review_count::integer as review_count,
  v:stars::float as stars,
  v:state::string as state
from business
where city = 'Las Vegas'


select
  v:adderess::string as address,
  v:business_id::string as business_id,
  v:categories::string as categories,
  v:city::string as city,
  v:latitude::float as latitude,
  v:longitude::float as longitude,
  v:name::string as name,
  v:postal_code::string as postal_code,
  v:review_count::integer as review_count,
  v:stars::float as stars,
  v:state::string as state
from business
where city = 'Las Vegas';

-- CHECKIN
select
  s:business_id::string as business_id,
  s:date::string as checkin_date
from checkin;

-- REVIEW
select 
    t:business_id::string as business_id,
    t:cool::integer as cool,
    t:date::timestamp as date,
    t:funny::integer as funny,
    t:review_id::string as review_id,
    t:stars::integer as stars,
    t:text::string as text,
    t:useful::integer as useful,
    t:user_id::string as user_id
from review;
    
-- COVID 
select
    v:"Call To Action enabled"::boolean as call2action,
    v:"Covid Banner"::string as covid_banner,
    v:"Grubhub enabled"::string as grubhub_enabled,
    v:"Request a Quote Enabled"::boolean as request_a_quote_enabled,
    v:"Temporary Closed Until"::string as temporary_closed_until,
    v:"Virtual Services Offered"::string as virtual_services_offered,
    v:"business_id"::string as business_id,
    v:"delivery or takeout"::boolean as delivery_or_takeout,
    v:"highlights"::string as highlights
from covid;

-- USERS
select
  v:average_stars::integer,
  v:compliment_cool::integer,
  v:compliment_cute::integer,
  v:compliment_funny::integer,
  v:compliment_hot::integer,
  v:compliment_list::integer,
  v:compliment_more::integer,
  v:compliment_note::integer,
  v:compliment_photos::integer,
  v:compliment_plain::integer,
  v:compliment_profile::integer,
  v:compliment_writer::integer,
  v:cool::integer,
  v:elite::string,
  v:fans::integer,
  v:friends::string,
  v:funny::integer,
  v:name::string,
  v:review_count::integer,
  v:useful::integer,
  v:user_id::string as user_id,
  v:yelping_since::timestamp
from users;

---------------------------------------------------
---------------------------------------------------
-------------- CREATE FINAL TABLES ----------------
---------------------------------------------------
---------------------------------------------------
-- USERS
create or replace table users as (
  select
  v:average_stars::integer as avg_stars,
  v:compliment_cool::integer as compliment_cool, 
  v:compliment_cute::integer as compliment_cute,
  v:compliment_funny::integer as compliment_funny,
  v:compliment_hot::integer as compliment_hot,
  v:compliment_list::integer as compliment_list,
  v:compliment_more::integer as compliment_more,
  v:compliment_note::integer as compliment_note,
  v:compliment_photos::integer as compliment_photos,
  v:compliment_plain::integer as compliment_plain,
  v:compliment_profile::integer as compliment_profile,
  v:compliment_writer::integer as compliment_writer,
  v:cool::integer as cool,
  v:elite::string as elite,
  v:fans::integer as fans,
  v:friends::string as friends,
  v:funny::integer as funny,
  v:name::string as name,
  v:review_count::integer as review_count,
  v:useful::integer as useful,
  v:user_id::string as user_id,
  v:yelping_since::timestamp as yelping_since
from users);

-- CONNECT USERS
ALTER TABLE Users ADD PRIMARY KEY(user_id);
ADD FOREIGN KEY (PersonID) REFERENCES Persons(PersonID);
-- check the updated table
select * from users limit 10;

-- BUSINESS
create or replace table business as (
  select
    v:adderess::string as address,
    v:business_id::string as business_id,
    v:categories::string as categories,
    v:city::string as city,
    v:latitude::float as latitude,
    v:longitude::float as longitude,
    v:name::string as name,
    v:postal_code::string as postal_code,
    v:review_count::integer as review_count,
    v:stars::float as stars,
    v:state::string as state
    from business
    where city = 'Las Vegas');

-- CONNECT BUSINESS
ALTER TABLE business ADD PRIMARY KEY(business_id);
select * from business limit 10;

-- REVIEW
create or replace table review as (
select 
    t:business_id::string as business_id,
    t:cool::integer as cool,
    t:date::timestamp as date,
    t:funny::integer as funny,
    t:review_id::string as review_id,
    t:stars::integer as stars,
    t:text::string as text,
    t:useful::integer as useful,
    t:user_id::string as user_id
from review);

-- CONNECT REVIEW
ALTER TABLE review 
ADD PRIMARY KEY(date);
ALTER TABLE review 
ADD FOREIGN KEY (business_id) REFERENCES business(business_id);
ALTER TABLE review 
ADD FOREIGN KEY (user_id) REFERENCES Users(user_id);
select * from review limit 10;


-- CHECKIN 
create or replace table checkin as (
select
  s:business_id::string as business_id,
  s:date::string as checkin_date
from checkin);

-- COVID
create or replace table covid  
(id integer primary key autoincrement);

-- create empty table for COVID json data
create or replace table covid (v variant);

create or replace table covid as (
select
    v:"Call To Action enabled"::boolean as call2action,
    v:"Covid Banner"::string as covid_banner,
    v:"Grubhub enabled"::string as grubhub_enabled,
    v:"Request a Quote Enabled"::boolean as request_a_quote_enabled,
    v:"Temporary Closed Until"::string as temporary_closed_until,
    v:"Virtual Services Offered"::string as virtual_services_offered,
    v:"business_id"::string as business_id,
    v:"delivery or takeout"::boolean as delivery_or_takeout,
    v:"highlights"::string as highlights
from covid);

select * from covid limit 5;
-- CONNECT COVID
ALTER TABLE covid 
ADD FOREIGN KEY (business_id) REFERENCES business(business_id);

Alter table covid
add id integer primary key autoincrement;

select * from covid limit 10;

-- CONNECT temp
ALTER TABLE temp 
ADD PRIMARY KEY (date);

-- CONNECT prec
ALTER TABLE prec  
ADD PRIMARY KEY (date);

-- create events table
create or replace table events_tb as(
select distinct u.user_id as users, r.date as review_date, c.checkin_date as checkin_date, u.yelping_since as yelping_since
from review as r
join business as b
on r.business_id = b.business_id
join users as u
on u.user_id = r.user_id
join checkin as c
on c.business_id = b.business_id);

   
select * from checkin limit 10;    
select * from business limit 2; 
select * from users limit 2;   
select * from review limit 2; 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    