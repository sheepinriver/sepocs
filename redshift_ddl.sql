create external schema spectrum_tpcds_csv
from data catalog
database 'tpcds_csv'
iam_role '6666666'
create external database if not exists;



CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.customer_address
( 
  ca_address_sk int4 ,
  ca_address_id char(16) ,
  ca_street_number char(10) ,      
  ca_street_name varchar(60) ,   
  ca_street_type char(15) ,     
  ca_suite_number char(10) ,    
  ca_city varchar(60) ,         
  ca_county varchar(30) ,       
  ca_state char(2) ,            
  ca_zip char(10) ,             
  ca_country varchar(20) ,      
  ca_gmt_offset numeric(5,2) ,  
  ca_location_type char(20)     
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/customer_address/'



CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.customer_demographics
(
  cd_demo_sk int4,   
  cd_gender char(1) ,          
  cd_marital_status char(1) ,   
  cd_education_status char(20) , 
  cd_purchase_estimate int4 ,   
  cd_credit_rating char(10) ,   
  cd_dep_count int4 ,             
  cd_dep_employed_count int4 ,    
  cd_dep_college_count int4
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/customer_demographics/'



CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.date_dim
(
    d_date_sk                 integer                       ,
    d_date_id                 char(16)                      ,
    d_date                    date,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/date_dim/'





CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.warehouse
(
    w_warehouse_sk            integer                       ,
    w_warehouse_id            char(16)                      ,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)                  
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/warehouse/'




CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.ship_mode
(
    sm_ship_mode_sk           integer                       ,
    sm_ship_mode_id           char(16)                      ,
    sm_type                   char(30)                      ,
    sm_code                   char(10)                      ,
    sm_carrier                char(20)                      ,
    sm_contract               char(20)                      
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/ship_mode/'


CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.time_dim
(
    t_time_sk                 integer                       ,
    t_time_id                 char(16)                      ,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/time_dim/'



CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.reason
(
    r_reason_sk               integer                       ,
    r_reason_id               char(16)                      ,
    r_reason_desc             char(100)                     
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/reason/'


CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.income_band
(
    ib_income_band_sk         integer                       ,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       
) 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/income_band/'



CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.item
(
i_item_sk int4,                     
  i_item_id char(16),      
  i_rec_start_date date,             
  i_rec_end_date date,               
  i_item_desc varchar(200) ,         
  i_current_price numeric(7,2),      
  i_wholesale_cost numeric(7,2),     
  i_brand_id int4,                   
  i_brand char(50) ,                 
  i_class_id int4,                   
  i_class char(50) ,                 
  i_category_id int4,                
  i_category char(50) ,              
  i_manufact_id int4,                
  i_manufact char(50) ,              
  i_size char(20) ,                  
  i_formulation char(20) ,           
  i_color char(20) ,            
  i_units char(10),             
  i_container char(10),         
  i_manager_id int4,            
  i_product_name char(50)       
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/item/'


CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.store
(
    s_store_sk                integer                       ,
    s_store_id                char(16)                      ,
    s_rec_start_date          date,
    s_rec_end_date            date,
    s_closed_date_sk          integer                       ,
    s_store_name              varchar(50)                   ,
    s_number_employees        integer                       ,
    s_floor_space             integer                       ,
    s_hours                   char(20)                      ,
    s_manager                 varchar(40)                   ,
    s_market_id               integer                       ,
    s_geography_class         varchar(100)                  ,
    s_market_desc             varchar(100)                  ,
    s_market_manager          varchar(40)                   ,
    s_division_id             integer                       ,
    s_division_name           varchar(50)                   ,
    s_company_id              integer                       ,
    s_company_name            varchar(50)                   ,
    s_street_number           varchar(10)                   ,
    s_street_name             varchar(60)                   ,
    s_street_type             char(15)                      ,
    s_suite_number            char(10)                      ,
    s_city                    varchar(60)                   ,
    s_county                  varchar(30)                   ,
    s_state                   char(2)                       ,
    s_zip                     char(10)                      ,
    s_country                 varchar(20)                   ,
    s_gmt_offset              decimal(5,2)                  ,
    s_tax_precentage          decimal(5,2)                  
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/store/'





CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.call_center
(
    cc_call_center_sk         integer                       ,
    cc_call_center_id         char(16)                      ,
    cc_rec_start_date         date,
    cc_rec_end_date           date,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   varchar(50)                   ,
    cc_class                  varchar(50)                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  char(20)                      ,
    cc_manager                varchar(40)                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              char(50)                      ,
    cc_mkt_desc               varchar(100)                  ,
    cc_market_manager         varchar(40)                   ,
    cc_division               integer                       ,
    cc_division_name          varchar(50)                   ,
    cc_company                integer                       ,
    cc_company_name           char(50)                      ,
    cc_street_number          char(10)                      ,
    cc_street_name            varchar(60)                   ,
    cc_street_type            char(15)                      ,
    cc_suite_number           char(10)                      ,
    cc_city                   varchar(60)                   ,
    cc_county                 varchar(30)                   ,
    cc_state                  char(2)                       ,
    cc_zip                    char(10)                      ,
    cc_country                varchar(20)                   ,
    cc_gmt_offset             decimal(5,2)                  ,
    cc_tax_percentage         decimal(5,2)                  
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/call_center/'






CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.customer
(
  c_customer_sk int4 ,                 
  c_customer_id char(16) ,             
  c_current_cdemo_sk int4 ,   
  c_current_hdemo_sk int4 ,   
  c_current_addr_sk int4 ,    
  c_first_shipto_date_sk int4 ,                 
  c_first_sales_date_sk int4 ,
  c_salutation char(10) ,     
  c_first_name char(20) ,     
  c_last_name char(30) ,      
  c_preferred_cust_flag char(1) ,               
  c_birth_day int4 ,          
  c_birth_month int4 ,        
  c_birth_year int4 ,         
  c_birth_country varchar(20) ,                 
  c_login char(13) ,          
  c_email_address char(50) ,  
  c_last_review_date_sk int4
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/customer/'





CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.web_site
(
    web_site_sk               integer                       ,
    web_site_id               char(16)                      ,
    web_rec_start_date        date,
    web_rec_end_date          date,
    web_name                  varchar(50)                   ,
    web_open_date_sk          integer                       ,
    web_close_date_sk         integer                       ,
    web_class                 varchar(50)                   ,
    web_manager               varchar(40)                   ,
    web_mkt_id                integer                       ,
    web_mkt_class             varchar(50)                   ,
    web_mkt_desc              varchar(100)                  ,
    web_market_manager        varchar(40)                   ,
    web_company_id            integer                       ,
    web_company_name          char(50)                      ,
    web_street_number         char(10)                      ,
    web_street_name           varchar(60)                   ,
    web_street_type           char(15)                      ,
    web_suite_number          char(10)                      ,
    web_city                  varchar(60)                   ,
    web_county                varchar(30)                   ,
    web_state                 char(2)                       ,
    web_zip                   char(10)                      ,
    web_country               varchar(20)                   ,
    web_gmt_offset            decimal(5,2)                  ,
    web_tax_percentage        decimal(5,2)                  
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/web_site/'




CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.store_returns
(
sr_returned_date_sk int4 ,    
  sr_return_time_sk int4 ,    
  sr_item_sk int4 ,  
  sr_customer_sk int4 ,       
  sr_cdemo_sk int4 ,          
  sr_hdemo_sk int4 ,          
  sr_addr_sk int4 ,           
  sr_store_sk int4 ,          
  sr_reason_sk int4 ,         
  sr_ticket_number int8 ,               
  sr_return_quantity int4 ,   
  sr_return_amt numeric(7,2) ,
  sr_return_tax numeric(7,2) ,
  sr_return_amt_inc_tax numeric(7,2) ,          
  sr_fee numeric(7,2) ,       
  sr_return_ship_cost numeric(7,2) ,            
  sr_refunded_cash numeric(7,2) ,               
  sr_reversed_charge numeric(7,2) ,             
  sr_store_credit numeric(7,2) ,                
  sr_net_loss numeric(7,2)                      
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/store_returns/'




CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.household_demographics
(
    hd_demo_sk                integer                       ,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/household_demographics/'




CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.web_page
(
    wp_web_page_sk            integer                       ,
    wp_web_page_id            char(16)                      ,
    wp_rec_start_date         date,
    wp_rec_end_date           date,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           char(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   char(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/web_page/'




CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.promotion
(
    p_promo_sk                integer                       ,
    p_promo_id                char(16)                      ,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    decimal(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              char(50)                      ,
    p_channel_dmail           char(1)                       ,
    p_channel_email           char(1)                       ,
    p_channel_catalog         char(1)                       ,
    p_channel_tv              char(1)                       ,
    p_channel_radio           char(1)                       ,
    p_channel_press           char(1)                       ,
    p_channel_event           char(1)                       ,
    p_channel_demo            char(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 char(15)                      ,
    p_discount_active         char(1)                       
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/promotion/'






CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.catalog_page
(
    cp_catalog_page_sk        integer                       ,
    cp_catalog_page_id        char(16)                      ,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/catalog_page/'





CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.inventory
(
 inv_date_sk int4 , 
  inv_item_sk int4 ,
  inv_warehouse_sk int4 ,
  inv_quantity_on_hand int4
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/inventory/'






CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.catalog_returns
(
 cr_returned_date_sk int4 ,  
  cr_returned_time_sk int4 , 
  cr_item_sk int4 , 
  cr_refunded_customer_sk int4 ,
  cr_refunded_cdemo_sk int4 ,   
  cr_refunded_hdemo_sk int4 ,   
  cr_refunded_addr_sk int4 ,    
  cr_returning_customer_sk int4 ,
  cr_returning_cdemo_sk int4 ,   
  cr_returning_hdemo_sk int4 ,  
  cr_returning_addr_sk int4 ,   
  cr_call_center_sk int4 ,      
  cr_catalog_page_sk int4 ,     
  cr_ship_mode_sk int4 ,        
  cr_warehouse_sk int4 ,        
  cr_reason_sk int4 ,           
  cr_order_number int8,
  cr_return_quantity int4 ,     
  cr_return_amount numeric(7,2) ,
  cr_return_tax numeric(7,2) ,   
  cr_return_amt_inc_tax numeric(7,2) ,
  cr_fee numeric(7,2) ,         
  cr_return_ship_cost numeric(7,2) , 
  cr_refunded_cash numeric(7,2) ,    
  cr_reversed_charge numeric(7,2) ,  
  cr_store_credit numeric(7,2) ,
  cr_net_loss numeric(7,2)
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/catalog_returns/'






CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.web_returns
(
wr_returned_date_sk int4 ,   
  wr_returned_time_sk int4 , 
  wr_item_sk int4 , 
  wr_refunded_customer_sk int4 ,
  wr_refunded_cdemo_sk int4 ,   
  wr_refunded_hdemo_sk int4 ,   
  wr_refunded_addr_sk int4 ,    
  wr_returning_customer_sk int4 ,
  wr_returning_cdemo_sk int4 ,   
  wr_returning_hdemo_sk int4 ,  
  wr_returning_addr_sk int4 ,   
  wr_web_page_sk int4 ,         
  wr_reason_sk int4 ,           
  wr_order_number int8 ,
  wr_return_quantity int4 ,     
  wr_return_amt numeric(7,2) ,  
  wr_return_tax numeric(7,2) ,  
  wr_return_amt_inc_tax numeric(7,2) ,
  wr_fee numeric(7,2) ,         
  wr_return_ship_cost numeric(7,2) ,
  wr_refunded_cash numeric(7,2) ,   
  wr_reversed_charge numeric(7,2) ,  
  wr_account_credit numeric(7,2) ,   
  wr_net_loss numeric(7,2)           
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/web_returns/'




CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.web_sales
(
 ws_sold_date_sk int4 ,             
  ws_sold_time_sk int4 ,        
  ws_ship_date_sk int4 ,        
  ws_item_sk int4 ,    
  ws_bill_customer_sk int4 ,    
  ws_bill_cdemo_sk int4 ,       
  ws_bill_hdemo_sk int4 ,       
  ws_bill_addr_sk int4 ,        
  ws_ship_customer_sk int4 ,    
  ws_ship_cdemo_sk int4 ,       
  ws_ship_hdemo_sk int4 ,       
  ws_ship_addr_sk int4 ,        
  ws_web_page_sk int4 ,         
  ws_web_site_sk int4 ,         
  ws_ship_mode_sk int4 ,        
  ws_warehouse_sk int4 ,        
  ws_promo_sk int4 ,            
  ws_order_number int8 ,
  ws_quantity int4 ,            
  ws_wholesale_cost numeric(7,2) ,                
  ws_list_price numeric(7,2) ,  
  ws_sales_price numeric(7,2) , 
  ws_ext_discount_amt numeric(7,2) ,              
  ws_ext_sales_price numeric(7,2) ,
  ws_ext_wholesale_cost numeric(7,2) ,               
  ws_ext_list_price numeric(7,2) , 
  ws_ext_tax numeric(7,2) ,     
  ws_coupon_amt numeric(7,2) ,  
  ws_ext_ship_cost numeric(7,2) ,                 
  ws_net_paid numeric(7,2) ,    
  ws_net_paid_inc_tax numeric(7,2) ,              
  ws_net_paid_inc_ship numeric(7,2) ,             
  ws_net_paid_inc_ship_tax numeric(7,2) ,         
  ws_net_profit numeric(7,2)                      
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/web_sales/'


CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.catalog_sales
(
 cs_sold_date_sk int4 ,          
  cs_sold_time_sk int4 ,        
  cs_ship_date_sk int4 ,        
  cs_bill_customer_sk int4 ,    
  cs_bill_cdemo_sk int4 ,       
  cs_bill_hdemo_sk int4 ,       
  cs_bill_addr_sk int4 ,        
  cs_ship_customer_sk int4 ,    
  cs_ship_cdemo_sk int4 ,       
  cs_ship_hdemo_sk int4 ,       
  cs_ship_addr_sk int4 ,        
  cs_call_center_sk int4 ,      
  cs_catalog_page_sk int4 ,     
  cs_ship_mode_sk int4 ,        
  cs_warehouse_sk int4 ,        
  cs_item_sk int4 ,    
  cs_promo_sk int4 ,            
  cs_order_number int8 ,                 
  cs_quantity int4 ,            
  cs_wholesale_cost numeric(7,2) ,                
  cs_list_price numeric(7,2) ,  
  cs_sales_price numeric(7,2) , 
  cs_ext_discount_amt numeric(7,2) ,              
  cs_ext_sales_price numeric(7,2) ,               
  cs_ext_wholesale_cost numeric(7,2) ,            
  cs_ext_list_price numeric(7,2) ,
  cs_ext_tax numeric(7,2) ,     
  cs_coupon_amt numeric(7,2) , 
  cs_ext_ship_cost numeric(7,2) ,                
  cs_net_paid numeric(7,2) ,   
  cs_net_paid_inc_tax numeric(7,2) ,             
  cs_net_paid_inc_ship numeric(7,2) ,            
  cs_net_paid_inc_ship_tax numeric(7,2) ,        
  cs_net_profit numeric(7,2)                     
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/catalog_sales/'


CREATE EXTERNAL TABLE spectrum_tpcds100g_csv.store_sales
(
ss_sold_date_sk int4 ,            
  ss_sold_time_sk int4 ,     
  ss_item_sk int4 ,      
  ss_customer_sk int4 ,           
  ss_cdemo_sk int4 ,              
  ss_hdemo_sk int4 ,         
  ss_addr_sk int4 ,               
  ss_store_sk int4 ,           
  ss_promo_sk int4 ,           
  ss_ticket_number int8 ,        
  ss_quantity int4 ,           
  ss_wholesale_cost numeric(7,2) ,          
  ss_list_price numeric(7,2) ,              
  ss_sales_price numeric(7,2) ,
  ss_ext_discount_amt numeric(7,2) ,             
  ss_ext_sales_price numeric(7,2) ,              
  ss_ext_wholesale_cost numeric(7,2) ,           
  ss_ext_list_price numeric(7,2) ,               
  ss_ext_tax numeric(7,2) ,                 
  ss_coupon_amt numeric(7,2) , 
  ss_net_paid numeric(7,2) ,   
  ss_net_paid_inc_tax numeric(7,2) ,             
  ss_net_profit numeric(7,2)                     
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'separatorChar'='|') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://hly-s3-lake/TPC-DS/2.13/100GB/store_sales/'
;


select count(*) from spectrum_tpcds100g_csv.call_center;  -- 48
select count(*) from spectrum_tpcds100g_csv.catalog_page;  -- 36000
select count(*) from spectrum_tpcds100g_csv.catalog_returns;  -- 432018033
select count(*) from spectrum_tpcds100g_csv.catalog_sales;  -- 4320078880
select count(*) from spectrum_tpcds100g_csv.customer;  -- 30000000
select count(*) from spectrum_tpcds100g_csv.customer_address;  -- 15000000
select count(*) from spectrum_tpcds100g_csv.customer_demographics;  -- 1920800
select count(*) from spectrum_tpcds100g_csv.date_dim;  -- 73049
select count(*) from spectrum_tpcds100g_csv.household_demographics;  -- 7200
select count(*) from spectrum_tpcds100g_csv.income_band;  -- 20
select count(*) from spectrum_tpcds100g_csv.inventory;  -- 1033560000
select count(*) from spectrum_tpcds100g_csv.item;  -- 360000
select count(*) from spectrum_tpcds100g_csv.promotion;  -- 1800
select count(*) from spectrum_tpcds100g_csv.reason;  -- 67
select count(*) from spectrum_tpcds100g_csv.ship_mode;  -- 20
select count(*) from spectrum_tpcds100g_csv.store;  -- 1350
select count(*) from spectrum_tpcds100g_csv.store_returns;  -- 863989652
select count(*) from spectrum_tpcds100g_csv.store_sales;  -- 8639936081
select count(*) from spectrum_tpcds100g_csv.time_dim;  -- 86400
select count(*) from spectrum_tpcds100g_csv.warehouse;  -- 22
select count(*) from spectrum_tpcds100g_csv.web_page;  -- 3600
select count(*) from spectrum_tpcds100g_csv.web_returns;  -- 216003761
select count(*) from spectrum_tpcds100g_csv.web_sales;  -- 2159968881
select count(*) from spectrum_tpcds100g_csv.web_site;  -- 66



select /* TPC-DS query96.tpl 0.1 */  count(*) 
from store_sales
    ,household_demographics 
    ,time_dim, 
    store
where ss_sold_time_sk = time_dim.t_time_sk   
    and ss_hdemo_sk = household_demographics.hd_demo_sk 
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 5
    and store.s_store_name = 'ese'
order by count(*)
limit 100;




CREATE MATERIALIZED VIEW tpcds100g_mv.call_center 
diststyle all
AS
select * from spectrum_tpcds100g_csv.call_center
;

CREATE MATERIALIZED VIEW tpcds100g_mv.catalog_page 
diststyle all
AS
select * from spectrum_tpcds100g_csv.catalog_page
;

CREATE MATERIALIZED VIEW tpcds100g_mv.catalog_returns 
distkey(cr_item_sk) sortkey(cr_returned_date_sk)
AS
select * from spectrum_tpcds100g_csv.catalog_returns
;

CREATE MATERIALIZED VIEW tpcds100g_mv.catalog_sales 
distkey(cs_item_sk) sortkey(cs_sold_date_sk)
AS
select * from spectrum_tpcds100g_csv.catalog_sales
;

CREATE MATERIALIZED VIEW tpcds100g_mv.customer 
distkey(c_customer_sk)
AS
select * from spectrum_tpcds100g_csv.customer
;

CREATE MATERIALIZED VIEW tpcds100g_mv.customer_address 
distkey(ca_address_sk)
AS
select * from spectrum_tpcds100g_csv.customer_address
;

CREATE MATERIALIZED VIEW tpcds100g_mv.customer_demographics 
distkey (cd_demo_sk)
AS
select * from spectrum_tpcds100g_csv.customer_demographics
;

CREATE MATERIALIZED VIEW tpcds100g_mv.date_dim 
diststyle all
AS
select * from spectrum_tpcds100g_csv.date_dim
;

CREATE MATERIALIZED VIEW tpcds100g_mv.household_demographics 
diststyle all
AS
select * from spectrum_tpcds100g_csv.household_demographics
;

CREATE MATERIALIZED VIEW tpcds100g_mv.income_band 
diststyle all
AS
select * from spectrum_tpcds100g_csv.income_band
;

CREATE MATERIALIZED VIEW tpcds100g_mv.inventory 
distkey(inv_item_sk) sortkey(inv_date_sk)
AS
select * from spectrum_tpcds100g_csv.inventory
;

CREATE MATERIALIZED VIEW tpcds100g_mv.item 
distkey(i_item_sk) sortkey(i_category)
AS
select * from spectrum_tpcds100g_csv.item
;

CREATE MATERIALIZED VIEW tpcds100g_mv.promotion 
diststyle all
AS
select * from spectrum_tpcds100g_csv.promotion
;

CREATE MATERIALIZED VIEW tpcds100g_mv.reason diststyle all
AS
select * from spectrum_tpcds100g_csv.reason
;

CREATE MATERIALIZED VIEW tpcds100g_mv.ship_mode diststyle all
AS
select * from spectrum_tpcds100g_csv.ship_mode
;

CREATE MATERIALIZED VIEW tpcds100g_mv.store 
diststyle all
AS
select * from spectrum_tpcds100g_csv.store
;

CREATE MATERIALIZED VIEW tpcds100g_mv.store_returns 
distkey(sr_item_sk) sortkey(sr_returned_date_sk)
AS
select * from spectrum_tpcds100g_csv.store_returns
;

CREATE MATERIALIZED VIEW tpcds100g_mv.store_sales 
distkey(ss_item_sk) sortkey(ss_sold_date_sk)
AS
select * from spectrum_tpcds100g_csv.store_sales
;

CREATE MATERIALIZED VIEW tpcds100g_mv.time_dim 
diststyle all
AS
select * from spectrum_tpcds100g_csv.time_dim
;

CREATE MATERIALIZED VIEW tpcds100g_mv.warehouse 
diststyle all
AS
select * from spectrum_tpcds100g_csv.warehouse
;

CREATE MATERIALIZED VIEW tpcds100g_mv.web_page 
diststyle all
AS
select * from spectrum_tpcds100g_csv.web_page
;

CREATE MATERIALIZED VIEW tpcds100g_mv.web_returns 
distkey(wr_order_number) sortkey(wr_returned_date_sk)
AS
select * from spectrum_tpcds100g_csv.web_returns
;

CREATE MATERIALIZED VIEW tpcds100g_mv.web_sales 
distkey(ws_order_number) sortkey(ws_sold_date_sk)
AS
select * from spectrum_tpcds100g_csv.web_sales
;

CREATE MATERIALIZED VIEW tpcds100g_mv.web_site 
diststyle all
AS
select * from spectrum_tpcds100g_csv.web_site
;
