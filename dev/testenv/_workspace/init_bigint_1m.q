create table bigint_1m stored as parquet as
select
    i+1 as n
from
    (select 1) x lateral view posexplode(split(space(1000000),' ')) pe as i, x
;
