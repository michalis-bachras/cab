select
    o_orderpriority,
    count(*) as order_count
from
    :orders
where
    o_orderdate >= :1::date
  and o_orderdate < :1::date + INTERVAL 3 MONTHS
  and exists (
        select
            *
        from
            :lineitem
        where
                l_orderkey = o_orderkey
          and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;