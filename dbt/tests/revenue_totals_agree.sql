-- The two revenue marts must add up to the same total; a NULL total on either side fails.
select a.total as daily_total, b.total as attribute_total
from (select sum(revenue) as total from {{ ref('daily_revenue') }}) a
cross join (select sum(revenue) as total from {{ ref('revenue_by_customer_attributes') }}) b
where a.total is null or b.total is null or a.total <> b.total
