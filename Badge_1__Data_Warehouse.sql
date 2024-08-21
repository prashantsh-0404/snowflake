select 'hello' as "Greeting";

show schemas in account;

show schemas;

use role sysadmin;
create or replace table GARDEN_PLANTS.VEGGIES.ROOT_DEPTH (
   ROOT_DEPTH_ID number(1), 
   ROOT_DEPTH_CODE text(1), 
   ROOT_DEPTH_NAME text(7), 
   UNIT_OF_MEASURE text(2),
   RANGE_MIN number(2),
   RANGE_MAX number(2)
   );

   USE WAREHOUSE COMPUTE_WH;

INSERT INTO ROOT_DEPTH (
	ROOT_DEPTH_ID ,
	ROOT_DEPTH_CODE ,
	ROOT_DEPTH_NAME ,
	UNIT_OF_MEASURE ,
	RANGE_MIN ,
	RANGE_MAX 
)

VALUES
(
    3,
    'D',
    'Deep',
    'cm',
    60,
    90
)
;






use role accountadmin;

create or replace api integration dora_api_integration
api_provider = aws_api_gateway
api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole'
enabled = true
api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

create or replace external function util_db.public.grader(
      step varchar
    , passed boolean
    , actual integer
    , expected integer
    , description varchar)
returns variant
api_integration = dora_api_integration 
context_headers = (current_timestamp, current_account, current_statement, current_account_name) 
as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'
; 

use role accountadmin;
use database util_db; 
use schema public; 

select grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from ( 
 SELECT 'DWW05' as step 
,( select row_count 
  from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
  where table_name = 'ROOT_DEPTH') as actual 
, 3 as expected 
,'ROOT_DEPTH row count' as description
);  


--Set your worksheet drop list role to ACCOUNTADMIN
--Set your worksheet drop list database and schema to the location of your GRADER function

-- DO NOT EDIT ANYTHING BELOW THIS LINE. THE CODE MUST BE RUN EXACTLY AS IT IS WRITTEN
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from ( 
 SELECT 'DWW05' as step 
,( select row_count 
  from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
  where table_name = 'ROOT_DEPTH') as actual 
, 3 as expected 
,'ROOT_DEPTH row count' as description
); 

-- DO NOT EDIT ANYTHING BELOW THIS LINE. THE CODE MUST BE RUN EXACTLY AS IT IS WRITTEN
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW06' as step
 ,( select count(*) 
   from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
   where table_name = 'VEGETABLE_DETAILS') as actual
 , 1 as expected
 ,'VEGETABLE_DETAILS Table' as description
); 


delete from vegetable_details
where plant_name ilike 'spinach'
and root_depth_code = 'D';

create or replace TABLE GARDEN_PLANTS.Flowers.flower_DETAILS (
	PLANT_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from ( 
   SELECT 'DWW08' as step 
   ,( select iff(count(*)=0, 0, count(*)/count(*))
      from table(information_schema.query_history())
      where query_text like 'execute notebook%Uncle Yer%') as actual 
   , 1 as expected 
   , 'Notebook success!' as description 
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW09' as step
 ,( select count(*)/count(*) 
    from snowflake.account_usage.query_history
    where query_text like 'execute streamlit "GARDEN_PLANTS"."FRUITS".%'
   ) as actual
 , 1 as expected
 ,'SiS App Works' as description
); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DWW10' as step
  ,( select count(*) 
    from UTIL_DB.INFORMATION_SCHEMA.stages
    where stage_name='MY_INTERNAL_STAGE' 
    and stage_type is null) as actual
  , 1 as expected
  , 'Internal stage created' as description
 );

 create or replace table vegetable_details_soil_type
( plant_name varchar(25)
 ,soil_type number(1,0)
);

create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
    type = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    field_delimiter = '|' --pipes as column separators
    skip_header = 1 --one header row to skip
    ;

    copy into vegetable_details_soil_type
from @util_db.public.my_internal_stage
files = ( 'VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW );

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DWW11' as step
  ,( select row_count 
    from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
    where table_name = 'VEGETABLE_DETAILS_SOIL_TYPE') as actual
  , 42 as expected
  , 'Veg Det Soil Type Count' as description
 ); 

 create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'--csv for comma separated files
    FIELD_DELIMITER = ',' --commas as column separators
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;


--The data in the file, with no FILE FORMAT specified
select $1
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv;

--Same file but with one of the file formats we created earlier
copy into garden_plants.veggies.lu_soil_type
from @util_db.public.my_internal_stage
files = ( 'LU_SOIL_TYPE.tsv')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.L9_CHALLENGE_FF );

--Same file but with the other file format we created earlier
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW );

 create or replace file format garden_plants.veggies.L9_CHALLENGE_FF 
    TYPE = 'CSV'--csv for comma separated files
    FIELD_DELIMITER = '\t' --commas as column separators
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;

    create or replace TABLE GARDEN_PLANTS.VEGGIES.LU_SOIL_TYPE (
	SOIL_TYPE_ID NUMBER(1,0),
	SOIL_TYPE VARCHAR(25),
    SOIL_DESCRIPTION VARCHAR(100)
);



    create or replace TABLE GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS_PLANT_HEIGHT (
	plant_name VARCHAR(25),
    UOM VARCHAR(10),
    Low_End_of_Range NUMBER(2,0),
    High_End_of_Range NUMBER(2,0)
);

copy into garden_plants.veggies.VEGETABLE_DETAILS_PLANT_HEIGHT
from @util_db.public.my_internal_stage
files = ( 'veg_plant_height.csv')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW );

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (  
      SELECT 'DWW12' as step 
      ,( select row_count 
        from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
        where table_name = 'VEGETABLE_DETAILS_PLANT_HEIGHT') as actual 
      , 41 as expected 
      , 'Veg Detail Plant Height Count' as description   
); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (  
     SELECT 'DWW13' as step 
     ,( select row_count 
       from GARDEN_PLANTS.INFORMATION_SCHEMA.TABLES 
       where table_name = 'LU_SOIL_TYPE') as actual 
     , 8 as expected 
     ,'Soil Type Look Up Table' as description   
); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from ( 
     SELECT 'DWW14' as step 
     ,( select count(*) 
       from GARDEN_PLANTS.INFORMATION_SCHEMA.FILE_FORMATS 
       where FILE_FORMAT_NAME='L9_CHALLENGE_FF' 
       and FIELD_DELIMITER = '\t') as actual 
     , 1 as expected 
     ,'Challenge File Format Created' as description  
); 


use role sysadmin;

// Create a new database and set the context to use the new database
create database library_card_catalog comment = 'DWW Lesson 10 ';

//Set the worksheet context to use the new database
use database library_card_catalog;

// Create Author table
create or replace table author (
   author_uid number 
  ,first_name varchar(50)
  ,middle_name varchar(50)
  ,last_name varchar(50)
);

// Insert the first two authors into the Author table
insert into author(author_uid, first_name, middle_name, last_name)  
values
(1, 'Fiona', '','Macdonald')
,(2, 'Gian','Paulo','Faleschini');

// Look at your table with it's new rows
select * 
from author;

use role sysadmin;

//See how the nextval function works
select SEQ_AUTHOR_UID.nextval


use role sysadmin;

//Drop and recreate the counter (sequence) so that it starts at 3 
// then we'll add the other author records to our author table
create or replace sequence library_card_catalog.public.seq_author_uid
start = 3 
increment = 1 
ORDER
comment = 'Use this to fill in the AUTHOR_UID every time you add a row';

//Add the remaining author records and use the nextval function instead 
//of putting in the numbers
insert into author(author_uid,first_name, middle_name, last_name) 
values
(seq_author_uid.nextval, 'Laura', 'K','Egendorf')
,(seq_author_uid.nextval, 'Jan', '','Grover')
,(seq_author_uid.nextval, 'Jennifer', '','Clapp')
,(seq_author_uid.nextval, 'Kathleen', '','Petelinsek');


use database library_card_catalog;
use role sysadmin;

// Create a new sequence, this one will be a counter for the book table
create or replace sequence library_card_catalog.public.seq_book_uid
  start = 1
  increment = 1
  ORDER
  comment = 'Use this to fill in the BOOK_UID every time you add a new row';


// Create the book table and use the NEXTVAL as the 
// default value each time a row is added to the table

create or replace table book
( book_uid number default seq_book_uid.nextval
 , title varchar(50)
 , year_published number(4,0)
);

// Insert records into the book table
// You don't have to list anything for the
// BOOK_UID field because the default setting
// will take care of it for you

insert into book(title, year_published)
values
 ('Food',2001)
,('Food',2006)
,('Food',2008)
,('Food',2016)
,('Food',2015);

// Create the relationships table
// this is sometimes called a "Many-to-Many table"
create table book_to_author
( book_uid number
  ,author_uid number
);

//Insert rows of the known relationships
insert into book_to_author(book_uid, author_uid)
values
 (1,1)  // This row links the 2001 book to Fiona Macdonald
,(1,2)  // This row links the 2001 book to Gian Paulo Faleschini
,(2,3)  // Links 2006 book to Laura K Egendorf
,(3,4)  // Links 2008 book to Jan Grover
,(4,5)  // Links 2016 book to Jennifer Clapp
,(5,6); // Links 2015 book to Kathleen Petelinsek


//Check your work by joining the 3 tables together
//You should get 1 row for every author
select * 
from book_to_author ba 
join author a 
on ba.author_uid = a.author_uid 
join book b 
on b.book_uid=ba.book_uid; 


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (  
     SELECT 'DWW15' as step 
     ,( select count(*) 
      from LIBRARY_CARD_CATALOG.PUBLIC.Book_to_Author ba 
      join LIBRARY_CARD_CATALOG.PUBLIC.author a 
      on ba.author_uid = a.author_uid 
      join LIBRARY_CARD_CATALOG.PUBLIC.book b 
      on b.book_uid=ba.book_uid) as actual 
     , 6 as expected 
     , '3NF DB was Created.' as description  
); 

// JSON DDL Scripts
use database library_card_catalog;
use role sysadmin;

// Create an Ingestion Table for JSON Data
create table library_card_catalog.public.author_ingest_json
(
  raw_author variant
);

//Create File Format for JSON Data 
create or replace file format library_card_catalog.public.json_file_format
type = 'JSON' 
compression = 'AUTO' 
enable_octal = TRUE
allow_duplicate = FALSE 
strip_outer_array = TRUE
strip_null_values = TRUE
ignore_utf8_errors = TRUE; 


-- truncate table library_card_catalog.public.author_ingest_json;
copy into library_card_catalog.public.author_ingest_json
from @util_db.public.my_internal_stage
files = ('author_with_header.json')
file_format = (format_name = library_card_catalog.public.json_file_format)
;
//returns AUTHOR_UID value from top-level object's attribute
select raw_author:AUTHOR_UID
from author_ingest_json;

select * from author_ingest_json;

//returns the data in a way that makes it look like a normalized table
SELECT 
 raw_author:AUTHOR_UID
,raw_author:FIRST_NAME::STRING as FIRST_NAME
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
FROM AUTHOR_INGEST_JSON;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT 'DWW16' as step
  ,( select row_count 
    from LIBRARY_CARD_CATALOG.INFORMATION_SCHEMA.TABLES 
    where table_name = 'AUTHOR_INGEST_JSON') as actual
  ,6 as expected
  ,'Check number of rows' as description
 ); 


 // Create an Ingestion Table for the NESTED JSON Data
CREATE OR REPLACE TABLE LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON 
(
  "RAW_NESTED_BOOK" VARIANT
);

copy into library_card_catalog.public.NESTED_INGEST_JSON
from @util_db.public.my_internal_stage
files = ('json_book_author_nested.txt')
file_format = (format_name = library_card_catalog.public.json_file_format);


//a few simple queries
SELECT RAW_NESTED_BOOK
FROM NESTED_INGEST_JSON;

SELECT RAW_NESTED_BOOK:year_published
FROM NESTED_INGEST_JSON;

SELECT RAW_NESTED_BOOK:authors
FROM NESTED_INGEST_JSON;

//Use these example flatten commands to explore flattening the nested book and author data
SELECT value:first_name
FROM NESTED_INGEST_JSON
,LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);

SELECT value:first_name
FROM NESTED_INGEST_JSON
,table(flatten(RAW_NESTED_BOOK:authors));

//Add a CAST command to the fields returned
SELECT value:first_name::VARCHAR, value:last_name::VARCHAR
FROM NESTED_INGEST_JSON
,LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);

//Assign new column  names to the columns using "AS"
SELECT value:first_name::VARCHAR AS FIRST_NM
, value:last_name::VARCHAR AS LAST_NM
FROM NESTED_INGEST_JSON
,LATERAL FLATTEN(input => RAW_NESTED_BOOK:authors);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (   
     SELECT 'DWW17' as step 
      ,( select row_count 
        from LIBRARY_CARD_CATALOG.INFORMATION_SCHEMA.TABLES 
        where table_name = 'NESTED_INGEST_JSON') as actual 
      , 5 as expected 
      ,'Check number of rows' as description  
); 


//Create a new database to hold the Twitter file
CREATE DATABASE SOCIAL_MEDIA_FLOODGATES 
COMMENT = 'There\'s so much data from social media - flood warning';

USE DATABASE SOCIAL_MEDIA_FLOODGATES;

//Create a table in the new database
CREATE TABLE SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST 
("RAW_STATUS" VARIANT) 
COMMENT = 'Bring in tweets, one row per tweet or status entity';

//Create a JSON file format in the new database
CREATE FILE FORMAT SOCIAL_MEDIA_FLOODGATES.PUBLIC.JSON_FILE_FORMAT 
TYPE = 'JSON' 
COMPRESSION = 'AUTO' 
ENABLE_OCTAL = FALSE 
ALLOW_DUPLICATE = FALSE 
STRIP_OUTER_ARRAY = TRUE 
STRIP_NULL_VALUES = FALSE 
IGNORE_UTF8_ERRORS = FALSE;



copy into SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
from @util_db.public.my_internal_stage
files = ('nutrition_tweets.json')
file_format = (format_name = SOCIAL_MEDIA_FLOODGATES.PUBLIC.JSON_FILE_FORMAT);


//select statements as seen in the video
SELECT RAW_STATUS
FROM TWEET_INGEST;

SELECT RAW_STATUS:entities
FROM TWEET_INGEST;

SELECT RAW_STATUS:entities:hashtags
FROM TWEET_INGEST;

//Explore looking at specific hashtags by adding bracketed numbers
//This query returns just the first hashtag in each tweet
SELECT RAW_STATUS:entities:hashtags[0].text
FROM TWEET_INGEST;

//This version adds a WHERE clause to get rid of any tweet that 
//doesn't include any hashtags
SELECT RAW_STATUS:entities:hashtags[0].text
FROM TWEET_INGEST
WHERE RAW_STATUS:entities:hashtags[0].text is not null;

//Perform a simple CAST on the created_at key
//Add an ORDER BY clause to sort by the tweet's creation date
SELECT RAW_STATUS:created_at::DATE
FROM TWEET_INGEST
ORDER BY RAW_STATUS:created_at::DATE;

//Flatten statements that return the whole hashtag entity
SELECT value
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

SELECT value
FROM TWEET_INGEST
,TABLE(FLATTEN(RAW_STATUS:entities:hashtags));

//Flatten statement that restricts the value to just the TEXT of the hashtag
SELECT value:text
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);


//Flatten and return just the hashtag text, CAST the text as VARCHAR
SELECT value:text::VARCHAR
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

//Flatten and return just the hashtag text, CAST the text as VARCHAR
// Use the AS command to name the column
SELECT value:text::VARCHAR AS THE_HASHTAG
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

//Add the Tweet ID and User ID to the returned table
SELECT RAW_STATUS:user:id AS USER_ID
,RAW_STATUS:id AS TWEET_ID
,value:text::VARCHAR AS HASHTAG_TEXT
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags);

-- Set your worksheet drop lists. DO NOT EDIT THE DORA CODE.
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
   SELECT 'DWW18' as step
  ,( select row_count 
    from SOCIAL_MEDIA_FLOODGATES.INFORMATION_SCHEMA.TABLES 
    where table_name = 'TWEET_INGEST') as actual
  , 9 as expected
  ,'Check number of rows' as description  
 ); 

 create or replace view SOCIAL_MEDIA_FLOODGATES.PUBLIC.HASHTAGS_NORMALIZED as
(SELECT RAW_STATUS:user:id AS USER_ID
,RAW_STATUS:id AS TWEET_ID
,value:text::VARCHAR AS HASHTAG_TEXT
FROM TWEET_INGEST
,LATERAL FLATTEN
(input => RAW_STATUS:entities:hashtags)
);

-- Set your worksheet drop lists. DO NOT EDIT THE DORA CODE.

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT 'DWW19' as step
  ,( select count(*) 
    from SOCIAL_MEDIA_FLOODGATES.INFORMATION_SCHEMA.VIEWS 
    where table_name = 'HASHTAGS_NORMALIZED') as actual
  , 1 as expected
  ,'Check number of rows' as description
 );

 --Set your worksheet drop list role to ACCOUNTADMIN
--Set your worksheet drop list database and schema to the location of your GRADER function

-- DO NOT EDIT ANYTHING BELOW THIS LINE. THE CODE MUST BE RUN EXACTLY AS IT IS WRITTEN
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from ( 
   SELECT 'DWW08' as step 
   ,( select iff(count(*)=0, 0, count(*)/count(*))
      from table(information_schema.query_history())
      where query_text like 'execute notebook%Uncle Yer%') as actual 
   , 1 as expected 
   , 'Notebook success!' as description 
); 