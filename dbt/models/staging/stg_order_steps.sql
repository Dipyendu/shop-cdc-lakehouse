-- One row per order ever created (live, cancelled or purged), from the order events in
-- bronze: the highest funnel step the order reached and whether it was ever cancelled.
select cast(get_json_object(kafka_key, '$.order_id') as bigint) as order_id,
       max(case get_json_object(payload, '$.after.status')
               when 'placed' then 1 when 'paid' then 2 when 'shipped' then 3 when 'delivered' then 4
           end) as max_step,
       max(get_json_object(payload, '$.after.status') = 'cancelled') as was_cancelled
from {{ source('bronze', 'orders_events') }}
where op in ('c', 'r', 'u')
group by 1
