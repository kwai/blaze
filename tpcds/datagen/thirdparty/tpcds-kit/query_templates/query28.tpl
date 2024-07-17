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

define LISTPRICE=ulist(random(0, 190, uniform),6);
define COUPONAMT=ulist(random(0, 18000, uniform),6);
define WHOLESALECOST=ulist(random(0, 80, uniform),6);
define _LIMIT=100;

[_LIMITA] select [_LIMITB] *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between [LISTPRICE.1] and [LISTPRICE.1]+10 
             or ss_coupon_amt between [COUPONAMT.1] and [COUPONAMT.1]+1000
             or ss_wholesale_cost between [WHOLESALECOST.1] and [WHOLESALECOST.1]+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between [LISTPRICE.2] and [LISTPRICE.2]+10
          or ss_coupon_amt between [COUPONAMT.2] and [COUPONAMT.2]+1000
          or ss_wholesale_cost between [WHOLESALECOST.2] and [WHOLESALECOST.2]+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between [LISTPRICE.3] and [LISTPRICE.3]+10
          or ss_coupon_amt between [COUPONAMT.3] and [COUPONAMT.3]+1000
          or ss_wholesale_cost between [WHOLESALECOST.3] and [WHOLESALECOST.3]+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between [LISTPRICE.4] and [LISTPRICE.4]+10
          or ss_coupon_amt between [COUPONAMT.4] and [COUPONAMT.4]+1000
          or ss_wholesale_cost between [WHOLESALECOST.4] and [WHOLESALECOST.4]+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between [LISTPRICE.5] and [LISTPRICE.5]+10
          or ss_coupon_amt between [COUPONAMT.5] and [COUPONAMT.5]+1000
          or ss_wholesale_cost between [WHOLESALECOST.5] and [WHOLESALECOST.5]+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between [LISTPRICE.6] and [LISTPRICE.6]+10
          or ss_coupon_amt between [COUPONAMT.6] and [COUPONAMT.6]+1000
          or ss_wholesale_cost between [WHOLESALECOST.6] and [WHOLESALECOST.6]+20)) B6
[_LIMITC];
