-- Every live order must fall inside exactly one history version of its customer.
select o.order_id, count(h.customer_id) as versions
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers_history') }} h
  on h.customer_id = o.customer_id
 and o.created_lsn >= h.valid_from_lsn
 and (h.valid_to_lsn is null or o.created_lsn < h.valid_to_lsn)
group by 1
having count(h.customer_id) <> 1
