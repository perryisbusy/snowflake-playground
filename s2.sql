---------------------------------------------------------------------------------
CREATE or replace DATABASE OUR_FIRST_DATABASE;

use OUR_FIRST_DATABASE;

CREATE or replace TABLE OUR_FIRST_TABLE (
  first_name STRING ,
  last_name STRING ,
  address string ,
  city string ,
  state string
);


create or replace stage my_s3_stage url='s3://snowflake-essentials/';

list @my_s3_stage;

copy into OUR_FIRST_TABLE
from s3://snowflake-essentials/our_first_table_data.csv
pattern = '.*.csv'
file_format = (type = csv field_delimiter = '|' skip_header = 1);

SELECT * FROM OUR_FIRST_TABLE;

SELECT COUNT(*) FROM OUR_FIRST_TABLE;

---------------------------------------------------------------------------------
create or replace table customer(
    id string,
    name string,
    address string,
    city string,
    postcode string,
    state string,
    company string,
    contact string
);
  
create or replace stage customer_s3_stage url='s3://snowflake-essentials/';

list @customer_s3_stage;

copy into customer
from s3://snowflake-essentials/customer.csv
file_format = (type = csv field_delimiter = '|' skip_header = 1);

select *
from customer;

select count(1) as "Total"
from customer;

drop stage customer_s3_stage;
---------------------------------------------------------------------------------

  
