select country, city, count(*) from {{ ref('revenue_by_customer_attributes') }} group by 1, 2 having count(*) > 1
