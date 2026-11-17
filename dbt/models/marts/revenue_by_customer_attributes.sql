-- Revenue by the customer's country and city AS THEY WERE AT ORDER TIME:
-- the order's insert log position falls inside one history version of its customer.
with o as (
    select o.order_id, o.created_lsn, h.country, h.city
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_customers_history') }} h
      on h.customer_id = o.customer_id
     and o.created_lsn >= h.valid_from_lsn
     and (h.valid_to_lsn is null or o.created_lsn < h.valid_to_lsn)
    where o.status <> 'cancelled'
)
select o.country, o.city, count(distinct o.order_id) as orders, sum(i.line_amount) as revenue
from o join {{ ref('stg_order_items') }} i using (order_id)
group by 1, 2
