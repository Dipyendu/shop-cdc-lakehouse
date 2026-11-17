select order_item_id, order_id, product_id, quantity, unit_price, quantity * unit_price as line_amount
from {{ source('silver', 'order_items') }}
where not _deleted
