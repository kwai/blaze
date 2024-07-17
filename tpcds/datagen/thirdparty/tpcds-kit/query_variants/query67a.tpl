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
-- 
 define YEAR = random(1998, 2002, uniform);
define _LIMIT=100;

with results as
(     select i_category ,i_class ,i_brand ,i_product_name ,d_year ,d_qoy ,d_moy ,s_store_id
                  ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
            from store_sales ,date_dim ,store ,item
       where  ss_sold_date_sk=d_date_sk
          and ss_item_sk=i_item_sk
          and ss_store_sk = s_store_sk
          and d_year=[YEAR]
       group by i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id)
 ,
 results_rollup as
 (select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, sumsales
  from results
  union all
  select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, null s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy
  union all
  select i_category, i_class, i_brand, i_product_name, d_year, d_qoy, null d_moy, null s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class, i_brand, i_product_name, d_year, d_qoy
  union all
  select i_category, i_class, i_brand, i_product_name, d_year, null d_qoy, null d_moy, null s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class, i_brand, i_product_name, d_year
  union all
  select i_category, i_class, i_brand, i_product_name, null d_year, null d_qoy, null d_moy, null s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class, i_brand, i_product_name
  union all
  select i_category, i_class, i_brand, null i_product_name, null d_year, null d_qoy, null d_moy, null s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class, i_brand
  union all
  select i_category, i_class, null i_brand, null i_product_name, null d_year, null d_qoy, null d_moy, null s_store_id, sum(sumsales) sumsales
  from results
  group by i_category, i_class
  union all
  select i_category, null i_class, null i_brand, null i_product_name, null d_year, null d_qoy, null d_moy, null s_store_id, sum(sumsales) sumsales
  from results
  group by i_category
  union all
  select null i_category, null i_class, null i_brand, null i_product_name, null d_year, null d_qoy, null d_moy, null s_store_id, sum(sumsales) sumsales
  from results)

[_LIMITA] select [_LIMITB] *
from (select i_category
            ,i_class
            ,i_brand
            ,i_product_name
            ,d_year
            ,d_qoy
            ,d_moy
            ,s_store_id
            ,sumsales
            ,rank() over (partition by i_category order by sumsales desc) rk
      from results_rollup) dw2
where rk <= 100
order by i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rk
[_LIMITC];
 
 

