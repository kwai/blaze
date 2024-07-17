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

WITH web_tv as (
select
  ws_item_sk item_sk, d_date, sum(ws_sales_price) sumws,
  row_number()
      over (partition by ws_item_sk order by d_date) rk
from web_sales
    ,date_dim
where ws_sold_date_sk=d_date_sk
  and d_year= [YEAR]
  and ws_item_sk is not NULL
group by ws_item_sk, d_date
),
web_v1 as (
select v1.item_sk, v1.d_date, v1.sumws, sum(v2.sumws) cume_sales
from web_tv v1, web_tv v2
where v1.item_sk = v2.item_sk and v1.rk >= v2.rk
group by v1.item_sk, v1.d_date, v1.sumws
),
store_tv as (
select
  ss_item_sk item_sk, d_date, sum(ss_sales_price) sumss,
  row_number()
      over (partition by ss_item_sk order by d_date) rk
from store_sales
    ,date_dim
where ss_sold_date_sk=d_date_sk
  and d_year= [YEAR]
  and ss_item_sk is not NULL
group by ss_item_sk, d_date
),
store_v1 as (
select v1.item_sk, v1.d_date, v1.sumss, sum(v2.sumss) cume_sales
from store_tv v1, store_tv v2
where v1.item_sk = v2.item_sk and v1.rk >= v2.rk
group by v1.item_sk, v1.d_date, v1.sumss
),
v as (
select item_sk
     ,d_date
     ,web_sales
     ,store_sales
     ,row_number() over (partition by item_sk order by d_date) rk
     from (select case when web.item_sk is not null then web.item_sk else store.item_sk end item_sk
                 ,case when web.d_date is not null then web.d_date else store.d_date end d_date
                 ,web.cume_sales web_sales
                 ,store.cume_sales store_sales
           from web_v1 web full outer join store_v1 store on (web.item_sk = store.item_sk
                                                          and web.d_date = store.d_date)
          ) sq
)
[_LIMITA] select [_LIMITB] *
from(
        select v1.item_sk, v1.d_date, v1.web_sales, v1.store_sales, max(v2.web_sales) web_cumulative, max(v2.store_sales) store_cumulative
        from v v1, v v2
        where v1.item_sk = v2.item_sk and v1.rk >= v2.rk
        group by v1.item_sk, v1.d_date, v1.web_sales, v1.store_sales
)x
where web_cumulative > store_cumulative
order by item_sk, d_date
[_LIMITC];
