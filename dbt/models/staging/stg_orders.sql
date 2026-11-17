-- Live orders (delete marks removed), with the log position of the order's insert,
-- used to find customer attributes as they were at order time.
select o.order_id, o.customer_id, o.status, o.ordered_at, c.created_lsn
from {{ source('silver', 'orders') }} o
join (
    select cast(get_json_object(kafka_key, '$.order_id') as bigint) as order_id, min(source_lsn) as created_lsn
    from {{ source('bronze', 'orders_events') }}
    where op in ('c', 'r')
    group by 1
) c using (order_id)
where not o._deleted
