---------------------------------------------------------------------------------
-- create database
create or replace database ingest_data;

create or replace table customer(
    customer_id string, 
    customer_name string,
    customer_email string,
    customer_city string,
    customer_state string,
    customer_dob string
);

select *
from customer;
---------------------------------------------------------------------------------
-- we made s3 public
create or replace stage bulk_copy_example_stage url='s3://snowflake-essentials/ingesting_data/new_customer';

list @bulk_copy_example_stage;

use database ingest_data;

-- load csv 
copy into customer
from @bulk_copy_example_stage
pattern = '.*.csv'
file_format = (type=csv field_delimiter='|' skip_header=1);

select *
from customer;

select count(1)
from customer;
---------------------------------------------------------------------------------
-- load txt
list @bulk_copy_example_stage;

copy into customer
from @bulk_copy_example_stage/2019-09-24/additional_data.txt
file_format = (type=csv field_delimiter='|' skip_header=1);

select *
from customer;

select count(1)
from customer;
---------------------------------------------------------------------------------
-- load json
use database ingest_data;

create or replace table organisations_json_raw(
    json_data_raw variant -- any type of data
);

create or replace stage json_exmaple_stage url = 's3://snowflake-essentials/json_data';

list @json_exmaple_stage;

copy into organisations_json_raw
from @json_exmaple_stage/example_json_file.json
file_format = (type=json);

select *
from organisations_json_raw;

select 
json_data_raw:data_set,
json_data_raw:extract_date
from organisations_json_raw;

-- use flatten table function to conver the JSON data into column
SELECT
    value:name::String,
    value:state::String,
    value:org_code::String,
	json_data_raw:extract_date
FROM
    organisations_json_raw
    , lateral flatten( input => json_data_raw:organisations );
    
-- "create table as" to load the columnar data extracted from JSON
CREATE OR REPLACE TABLE organisations_ctas AS
SELECT
    VALUE:name::String AS org_name,
    VALUE:state::String AS state,
    VALUE:org_code::String AS org_code,
	json_data_raw:extract_date AS extract_date
FROM
    organisations_json_raw
    , lateral flatten( input => json_data_raw:organisations );

select *
from organisations_ctas;


-- If you don't want to do a "create table as" you can pre-create a table
CREATE or replace TABLE organisations (
    org_name STRING,
    state   STRING,
    org_code STRING,
	extract_date DATE
); 

-- and insert the JSON data into the table
INSERT INTO organisations 
SELECT
    VALUE:name::String AS org_name,
    VALUE:state::String AS state,
    VALUE:org_code::String AS org_code,
	json_data_raw:extract_date AS extract_date
FROM
    organisations_json_raw
    , lateral flatten( input => json_data_raw:organisations );
    

select *
from organisations;
---------------------------------------------------------------------------------
-- assignment
create or replace table assignment_json_raw(
  json_data_raw VARIANT
);

create or replace stage assignment_stage url = 's3://snowflake-essentials-json-lab';

list @assignment_stage;

copy into assignment_json_raw
from @assignment_stage
file_format = (type = json);

select *
from assignment_json_raw;

select 
json_data_raw:cdc_date, 
json_data_raw:customers
from assignment_json_raw;


select 
json_data_raw:cdc_date,
value:Customer_City::string,
value:Customer_ID::string,
value:Customer_Name::string,
value:Customer_Phone::string
from assignment_json_raw,
lateral flatten(input => json_data_raw:customers);

create or replace table assignment as 
select
value:Customer_ID::string as customer_id,
value:Customer_City::string as customer_city,
value:Customer_Name::string as customer_name,
value:Customer_Phone::string as customer_phone,
json_data_raw:cdc_date as exact_date
from assignment_json_raw,
lateral flatten(input => json_data_raw:customers);

select *
from assignment;

create or replace table assignment(
customer_id string,
customer_city string,
customer_name string,
customer_phone string,
exact_date date
);

insert into assignment
select
value:Customer_ID::string as customer_id,
value:Customer_City::string as customer_city,
value:Customer_Name::string as customer_name,
value:Customer_Phone::string as customer_phone,
json_data_raw:cdc_date as exact_date
from assignment_json_raw,
lateral flatten(input => json_data_raw:customers);

select count(1) as cnt
from assignment
where customer_city like '%Cornwall%';

---------------------------------------------------------------------------------
-- snowpipe
use database ingest_data;

create or replace stage snowpipe_copy_example_stage url='s3://snowpipe-streaming/transactions';

list @snowpipe_copy_example_stage;

create or replace table transactions(
Transaction_Date DATE,
Customer_ID NUMBER,
Transaction_ID NUMBER,
Amount NUMBER);

copy into transactions
from @snowpipe_copy_example_stage
file_format = (type = csv field_delimiter='|' skip_header=1);

select count(1) as cnt
from transactions;

create or replace pipe transaction_pipe
auto_ingest = True
as copy into transactions 
from @snowpipe_copy_example_stage
file_format = (type = csv field_delimiter='|' skip_header=1);

show pipes;


