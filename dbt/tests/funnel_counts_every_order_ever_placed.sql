-- Every order ever created was "placed", so the funnel's reached count for 'placed' must
-- equal the number of distinct orders with an insert event in bronze (cancelled and purged
-- orders included). Steps 1 to 4 must not increase.
with created as (
    select count(distinct get_json_object(kafka_key, '$.order_id')) as n
    from {{ source('bronze', 'orders_events') }}
    where op in ('c', 'r')
),
f as (
    select step, orders_reached_step,
           lag(orders_reached_step) over (order by step) as prev
    from {{ ref('order_status_funnel') }}
    where step <= 4
)
select 'placed count differs' as problem, f.orders_reached_step, c.n
from f cross join created c
where f.step = 1 and f.orders_reached_step is distinct from c.n
union all
select 'reached count increases', f.orders_reached_step, f.prev
from f cross join created c
where f.prev is not null and f.orders_reached_step > f.prev
