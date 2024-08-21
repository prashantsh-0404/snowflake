create or replace table util_db.public.my_data_types
(
  my_number number
, my_text varchar(10)
, my_bool boolean
, my_float float
, my_date date
, my_timestamp timestamp_tz
, my_variant variant
, my_array array
, my_object object
, my_geography geography
, my_geometry geometry
, my_vector vector(int,16)
);

use role sysadmin;
create database ZENAS_ATHLEISURE_DB;
drop schema ZENAS_ATHLEISURE_DB.public;
create schema ZENAS_ATHLEISURE_DB.PRODUCTS;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DLKW01' as step
  ,( select count(*)  
      from ZENAS_ATHLEISURE_DB.INFORMATION_SCHEMA.STAGES 
      where stage_schema = 'PRODUCTS'
      and 
      (stage_type = 'Internal Named' 
      and stage_name = ('PRODUCT_METADATA'))
      or stage_name = ('SWEATSUITS')
   ) as actual
, 2 as expected
, 'Zena stages look good' as description
); 

list @product_metadata;

select $1
from @product_metadata/product_coordination_suggestions.txt;


create file format zmd_file_format_1
RECORD_DELIMITER = '^';

select $1,$10
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

create file format zmd_file_format_2
FIELD_DELIMITER = '^';  

select $1
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);


create or replace file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'
TRIM_SPACE = TRUE; 

select $1, $2
from @product_metadata/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);


create or replace file format zmd_file_format_1
RECORD_DELIMITER = ';'
TRIM_SPACE = TRUE;

select $1 as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 );

create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE = TRUE;

select $1, $2, $3
from @product_metadata/swt_product_line.txt
(file_format => zmd_file_format_2 );

select replace($1, concat(chr(13), chr(10))) as sizes_available
from @product_metadata/sweatsuit_sizes.txt
(file_format => zmd_file_format_1 )
where sizes_available <> '';
