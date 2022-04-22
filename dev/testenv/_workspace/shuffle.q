set spark.sql.shuffle.partitions = 2;

select
    n,
    sqrt(n) as sqrtn
from
    bigint_1m
where
    n <= 200000
order by
    n desc
