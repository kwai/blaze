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
 define COLOR=ulist(dist(colors,1,1),16); 
 define UNIT=ulist(dist(units,1,1),16); 
 define SIZE=ulist(dist(sizes,1,1),6); 
 define MANUFACT= random(667,1000,uniform); -- for qualification 698 
 define _LIMIT=100;
 
 [_LIMITA] select [_LIMITB] distinct(i_product_name)
 from item i1
 where i_manufact_id between [MANUFACT] and [MANUFACT]+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = '[COLOR.1]' or i_color = '[COLOR.2]') and 
        (i_units = '[UNIT.1]' or i_units = '[UNIT.2]') and
        (i_size = '[SIZE.1]' or i_size = '[SIZE.2]')
        ) or
        (i_category = 'Women' and
        (i_color = '[COLOR.3]' or i_color = '[COLOR.4]') and
        (i_units = '[UNIT.3]' or i_units = '[UNIT.4]') and
        (i_size = '[SIZE.3]' or i_size = '[SIZE.4]')
        ) or
        (i_category = 'Men' and
        (i_color = '[COLOR.5]' or i_color = '[COLOR.6]') and
        (i_units = '[UNIT.5]' or i_units = '[UNIT.6]') and
        (i_size = '[SIZE.5]' or i_size = '[SIZE.6]')
        ) or
        (i_category = 'Men' and
        (i_color = '[COLOR.7]' or i_color = '[COLOR.8]') and
        (i_units = '[UNIT.7]' or i_units = '[UNIT.8]') and
        (i_size = '[SIZE.1]' or i_size = '[SIZE.2]')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = '[COLOR.9]' or i_color = '[COLOR.10]') and 
        (i_units = '[UNIT.9]' or i_units = '[UNIT.10]') and
        (i_size = '[SIZE.1]' or i_size = '[SIZE.2]')
        ) or
        (i_category = 'Women' and
        (i_color = '[COLOR.11]' or i_color = '[COLOR.12]') and
        (i_units = '[UNIT.11]' or i_units = '[UNIT.12]') and
        (i_size = '[SIZE.3]' or i_size = '[SIZE.4]')
        ) or
        (i_category = 'Men' and
        (i_color = '[COLOR.13]' or i_color = '[COLOR.14]') and
        (i_units = '[UNIT.13]' or i_units = '[UNIT.14]') and
        (i_size = '[SIZE.5]' or i_size = '[SIZE.6]')
        ) or
        (i_category = 'Men' and
        (i_color = '[COLOR.15]' or i_color = '[COLOR.16]') and
        (i_units = '[UNIT.15]' or i_units = '[UNIT.16]') and
        (i_size = '[SIZE.1]' or i_size = '[SIZE.2]')
        )))) > 0
 order by i_product_name
 [_LIMITC];

