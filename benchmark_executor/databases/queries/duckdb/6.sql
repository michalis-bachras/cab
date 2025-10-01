select
    sum(l_extendedprice * l_discount) as revenue
from
    :lineitem
where
    l_shipdate >= :1::date
  and l_shipdate < :1::date + INTERVAL 1 YEAR
  and l_discount between (:2::DECIMAL(12,2) / 100) - 0.01 and (:2::DECIMAL(12,2) / 100) + 0.01
  and l_quantity < :3;