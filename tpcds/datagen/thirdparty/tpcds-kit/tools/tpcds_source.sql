-- 
-- Legal Notice 
-- 
-- This document and associated source code (the "Work") is a part of a 
-- benchmark specification maintained by the TPC. 
-- 
-- The TPC reserves all right, title, and interest to the Work as provided 
-- under U.S. and international laws, including without limitation all patent 
-- and trademark rights therein. 
-- 
-- No Warranty 
-- 
-- 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
--     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
--     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
--     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
--     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
--     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
--     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
--     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
--     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
--     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
--     WITH REGARD TO THE WORK. 
-- 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
--     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
--     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
--     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
--     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
--     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
--     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
--     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
-- 
-- Contributors:
-- Gradient Systems
--

-- ============================================================
--   Database name:  tpcds_source                              
--   DBMS name:      ANSI Level 2                              
--   Created on:     2/8/2007  9:38 AM                         
-- ============================================================

-- ============================================================
--   Table: s_catalog_page                                     
-- ============================================================
create table s_catalog_page
(
    cpag_catalog_number         integer               not null,
    cpag_catalog_page_number    integer               not null,
    cpag_department             char(20)                      ,
    cpag_id                     char(16)                      ,
    cpag_start_date             char(10)                      ,
    cpag_end_date               char(10)                      ,
    cpag_description            varchar(100)                  ,
    cpag_type                   varchar(100)                  
);

-- ============================================================
--   Table: s_zip_to_gmt                                       
-- ============================================================
create table s_zip_to_gmt
(
    zipg_zip                    char(5)               not null,
    zipg_gmt_offset             integer               not null
);

-- ============================================================
--   Table: s_purchase_lineitem                                
-- ============================================================
create table s_purchase_lineitem
(
    plin_purchase_id            integer               not null,
    plin_line_number            integer               not null,
    plin_item_id                char(16)                      ,
    plin_promotion_id           char(16)                      ,
    plin_quantity               integer                       ,
    plin_sale_price             numeric(7,2)                  ,
    plin_coupon_amt             numeric(7,2)                  ,
    plin_comment                varchar(100)                  
);

-- ============================================================
--   Table: s_customer                                         
-- ============================================================
create table s_customer
(
    cust_customer_id            char(16)              not null,
    cust_salutation             char(10)                      ,
    cust_last_name              char(20)                      ,
    cust_first_name             char(20)                      ,
    cust_preffered_flag         char(1)                       ,
    cust_birth_date             char(10)                      ,
    cust_birth_country          char(20)                      ,
    cust_login_id               char(13)                      ,
    cust_email_address          char(50)                      ,
    cust_last_login_chg_date    char(10)                      ,
    cust_first_shipto_date      char(10)                      ,
    cust_first_purchase_date    char(10)                      ,
    cust_last_review_date       char(10)                      ,
    cust_primary_machine_id     char(15)                      ,
    cust_secondary_machine_id   char(15)                      ,
    cust_street_number          smallint                      ,
    cust_suite_number           char(10)                      ,
    cust_street_name1           char(30)                      ,
    cust_street_name2           char(30)                      ,
    cust_street_type            char(15)                      ,
    cust_city                   char(60)                      ,
    cust_zip                    char(10)                      ,
    cust_county                 char(30)                      ,
    cust_state                  char(2)                       ,
    cust_country                char(20)                      ,
    cust_loc_type               char(20)                      ,
    cust_gender                 char(1)                       ,
    cust_marital_status         char(1)                       ,
    cust_educ_status            char(20)                      ,
    cust_credit_rating          char(10)                      ,
    cust_purch_est              numeric(7,2)                  ,
    cust_buy_potential          char(15)                      ,
    cust_depend_cnt             smallint                      ,
    cust_depend_emp_cnt         smallint                      ,
    cust_depend_college_cnt     smallint                      ,
    cust_vehicle_cnt            smallint                      ,
    cust_annual_income          numeric(9,2)                  
);

-- ============================================================
--   Table: s_customer_address                                 
-- ============================================================
create table s_customer_address
(
    cadr_address_id             char(16)              not null,
    cadr_street_number          integer                       ,
    cadr_street_name1           char(25)                      ,
    cadr_street_name2           char(25)                      ,
    cadr_street_type            char(15)                      ,
    cadr_suitnumber             char(10)                      ,
    cadr_city                   char(60)                      ,
    cadr_county                 char(30)                      ,
    cadr_state                  char(2)                       ,
    cadr_zip                    char(10)                      ,
    cadr_country                char(20)                      
);

-- ============================================================
--   Table: s_purchase                                         
-- ============================================================
create table s_purchase
(
    purc_purchase_id            integer               not null,
    purc_store_id               char(16)                      ,
    purc_customer_id            char(16)                      ,
    purc_purchase_date          char(10)                      ,
    purc_purchase_time          integer                       ,
    purc_register_id            integer                       ,
    purc_clerk_id               integer                       ,
    purc_comment                char(100)                     
);

-- ============================================================
--   Table: s_catalog_order                                    
-- ============================================================
create table s_catalog_order
(
    cord_order_id               integer               not null,
    cord_bill_customer_id       char(16)                      ,
    cord_ship_customer_id       char(16)                      ,
    cord_order_date             char(10)                      ,
    cord_order_time             integer                       ,
    cord_ship_mode_id           char(16)                      ,
    cord_call_center_id         char(16)                      ,
    cord_order_comments         varchar(100)                  
);

-- ============================================================
--   Table: s_web_order                                        
-- ============================================================
create table s_web_order
(
    word_order_id               integer               not null,
    word_bill_customer_id       char(16)                      ,
    word_ship_customer_id       char(16)                      ,
    word_order_date             char(10)                      ,
    word_order_time             integer                       ,
    word_ship_mode_id           char(16)                      ,
    word_web_site_id            char(16)                      ,
    word_order_comments         char(100)                     
);

-- ============================================================
--   Table: s_item                                             
-- ============================================================
create table s_item
(
    item_item_id                char(16)              not null,
    item_item_description       char(200)                     ,
    item_list_price             numeric(7,2)                  ,
    item_wholesale_cost         numeric(7,2)                  ,
    item_size                   char(20)                      ,
    item_formulation            char(20)                      ,
    item_color                  char(20)                      ,
    item_units                  char(10)                      ,
    item_container              char(10)                      ,
    item_manager_id             integer                       
);

-- ============================================================
--   Table: s_catalog_order_lineitem                           
-- ============================================================
create table s_catalog_order_lineitem
(
    clin_order_id               integer               not null,
    clin_line_number            integer               not null,
    clin_item_id                char(16)                      ,
    clin_promotion_id           char(16)                      ,
    clin_quantity               integer                       ,
    clin_sales_price            numeric(7,2)                  ,
    clin_coupon_amt             numeric(7,2)                  ,
    clin_warehouse_id           char(16)                      ,
    clin_ship_date              char(10)                      ,
    clin_catalog_number         integer                       ,
    clin_catalog_page_number    integer                       ,
    clin_ship_cost              numeric(7,2)                  
);

-- ============================================================
--   Table: s_web_order_lineitem                               
-- ============================================================
create table s_web_order_lineitem
(
    wlin_order_id               integer               not null,
    wlin_line_number            integer               not null,
    wlin_item_id                char(16)                      ,
    wlin_promotion_id           char(16)                      ,
    wlin_quantity               integer                       ,
    wlin_sales_price            numeric(7,2)                  ,
    wlin_coupon_amt             numeric(7,2)                  ,
    wlin_warehouse_id           char(16)                      ,
    wlin_ship_date              char(10)                      ,
    wlin_ship_cost              numeric(7,2)                  ,
    wlin_web_page_id            char(16)                      
);

-- ============================================================
--   Table: s_store                                            
-- ============================================================
create table s_store
(
    stor_store_id               char(16)              not null,
    stor_closed_date            char(10)                      ,
    stor_name                   char(50)                      ,
    stor_employees              integer                       ,
    stor_floor_space            integer                       ,
    stor_hours                  char(20)                      ,
    stor_store_manager          char(40)                      ,
    stor_market_id              integer                       ,
    stor_geography_class        char(100)                     ,
    stor_market_manager         char(40)                      ,
    stor_tax_percentage         numeric(5,2)                  
);

-- ============================================================
--   Table: s_call_center                                      
-- ============================================================
create table s_call_center
(
    call_center_id              char(16)              not null,
    call_open_date              char(10)                      ,
    call_closed_date            char(10)                      ,
    call_center_name            char(50)                      ,
    call_center_class           char(50)                      ,
    call_center_employees       integer                       ,
    call_center_sq_ft           integer                       ,
    call_center_hours           char(20)                      ,
    call_center_manager         char(40)                      ,
    call_center_tax_percentage  numeric(7,2)                  
);

-- ============================================================
--   Table: s_web_site                                         
-- ============================================================
create table s_web_site
(
    wsit_web_site_id            char(16)              not null,
    wsit_open_date              char(10)                      ,
    wsit_closed_date            char(10)                      ,
    wsit_site_name              char(50)                      ,
    wsit_site_class             char(50)                      ,
    wsit_site_manager           char(40)                      ,
    wsit_tax_percentage         decimal(5,2)                  
);

-- ============================================================
--   Table: s_warehouse                                        
-- ============================================================
create table s_warehouse
(
    wrhs_warehouse_id           char(16)              not null,
    wrhs_warehouse_desc         char(200)                     ,
    wrhs_warehouse_sq_ft        integer                       
);

-- ============================================================
--   Table: s_web_page                                         
-- ============================================================
create table s_web_page
(
    wpag_web_page_id            char(16)              not null,
    wpag_create_date            char(10)                      ,
    wpag_access_date            char(10)                      ,
    wpag_autogen_flag           char(1)                       ,
    wpag_url                    char(100)                     ,
    wpag_type                   char(50)                      ,
    wpag_char_cnt               integer                       ,
    wpag_link_cnt               integer                       ,
    wpag_image_cnt              integer                       ,
    wpag_max_ad_cnt             integer                       
);

-- ============================================================
--   Table: s_promotion                                        
-- ============================================================
create table s_promotion
(
    prom_promotion_id           char(16)              not null,
    prom_promotion_name         char(30)                      ,
    prom_start_date             char(10)                      ,
    prom_end_date               char(10)                      ,
    prom_cost                   numeric(7,2)                  ,
    prom_response_target        char(1)                       ,
    prom_channel_dmail          char(1)                       ,
    prom_channel_email          char(1)                       ,
    prom_channel_catalog        char(1)                       ,
    prom_channel_tv             char(1)                       ,
    prom_channel_radio          char(1)                       ,
    prom_channel_press          char(1)                       ,
    prom_channel_event          char(1)                       ,
    prom_channel_demo           char(1)                       ,
    prom_channel_details        char(100)                     ,
    prom_purpose                char(15)                      ,
    prom_discount_active        char(1)                       ,
    prom_discount_pct           numeric(5,2)                  
);

-- ============================================================
--   Table: s_store_returns                                    
-- ============================================================
create table s_store_returns
(
    sret_store_id               char(16)                      ,
    sret_purchase_id            char(16)              not null,
    sret_line_number            integer               not null,
    sret_item_id                char(16)              not null,
    sret_customer_id            char(16)                      ,
    sret_return_date            char(10)                      ,
    sret_return_time            char(10)                      ,
    sret_ticket_number          char(20)                      ,
    sret_return_qty             integer                       ,
    sret_return_amt             numeric(7,2)                  ,
    sret_return_tax             numeric(7,2)                  ,
    sret_return_fee             numeric(7,2)                  ,
    sret_return_ship_cost       numeric(7,2)                  ,
    sret_refunded_cash          numeric(7,2)                  ,
    sret_reversed_charge        numeric(7,2)                  ,
    sret_store_credit           numeric(7,2)                  ,
    sret_reason_id              char(16)                      
);

-- ============================================================
--   Table: s_catalog_returns                                  
-- ============================================================
create table s_catalog_returns
(
    cret_call_center_id         char(16)                      ,
    cret_order_id               integer               not null,
    cret_line_number            integer               not null,
    cret_item_id                char(16)              not null,
    cret_return_customer_id     char(16)                      ,
    cret_refund_customer_id     char(16)                      ,
    cret_return_date            char(10)                      ,
    cret_return_time            char(10)                      ,
    cret_return_qty             integer                       ,
    cret_return_amt             numeric(7,2)                  ,
    cret_return_tax             numeric(7,2)                  ,
    cret_return_fee             numeric(7,2)                  ,
    cret_return_ship_cost       numeric(7,2)                  ,
    cret_refunded_cash          numeric(7,2)                  ,
    cret_reversed_charge        numeric(7,2)                  ,
    cret_merchant_credit        numeric(7,2)                  ,
    cret_reason_id              char(16)                      ,
    cret_shipmode_id            char(16)                      ,
    cret_catalog_page_id        char(16)                      ,
    cret_warehouse_id           char(16)                      
);

-- ============================================================
--   Table: s_web_returns                                      
-- ============================================================
create table s_web_returns
(
    wret_web_site_id            char(16)                      ,
    wret_order_id               integer               not null,
    wret_line_number            integer               not null,
    wret_item_id                char(16)              not null,
    wret_return_customer_id     char(16)                      ,
    wret_refund_customer_id     char(16)                      ,
    wret_return_date            char(10)                      ,
    wret_return_time            char(10)                      ,
    wret_return_qty             integer                       ,
    wret_return_amt             numeric(7,2)                  ,
    wret_return_tax             numeric(7,2)                  ,
    wret_return_fee             numeric(7,2)                  ,
    wret_return_ship_cost       numeric(7,2)                  ,
    wret_refunded_cash          numeric(7,2)                  ,
    wret_reversed_charge        numeric(7,2)                  ,
    wret_account_credit         numeric(7,2)                  ,
    wret_reason_id              char(16)                      
);

-- ============================================================
--   Table: s_inventory                                        
-- ============================================================
create table s_inventory
(
    invn_warehouse_id           char(16)              not null,
    invn_item_id                char(16)              not null,
    invn_date                   char(10)              not null,
    invn_qty_on_hand            integer                       
);

