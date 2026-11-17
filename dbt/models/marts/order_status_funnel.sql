-- orders_now_in_status: live orders in each status now (purged orders are gone).
-- orders_reached_step: orders that ever reached the step, from the order events in bronze,
-- so cancelled and purged orders count for every step they passed. For 'cancelled' it is
-- the number of orders that were ever cancelled.
with now_in as (
    select status, count(*) as orders from {{ ref('stg_orders') }} group by 1
),
steps as (
    select * from values ('placed', 1), ('paid', 2), ('shipped', 3), ('delivered', 4), ('cancelled', 9)
        as t(status, step)
),
reached as (
    select st.step,
           case when st.step < 9 then count_if(o.max_step >= st.step) else count_if(o.was_cancelled) end as orders
    from steps st cross join {{ ref('stg_order_steps') }} o
    group by st.step
)
select st.status, st.step,
       coalesce(n.orders, 0) as orders_now_in_status,
       r.orders as orders_reached_step
from steps st
left join now_in n using (status)
join reached r using (step)
