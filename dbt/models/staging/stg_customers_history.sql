select customer_id, name, email, city, country, valid_from, valid_to, valid_from_lsn, valid_to_lsn, is_current
from {{ source('silver', 'customers_history') }}
