-- Revenue per order day from order lines; cancelled orders excluded.
select cast(o.ordered_at as date) as order_date,
       count(distinct o.order_id) as orders,
       sum(i.line_amount) as revenue
from {{ ref('stg_orders') }} o
join {{ ref('stg_order_items') }} i using (order_id)
where o.status <> 'cancelled'
group by 1
