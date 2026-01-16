-- Fail if payment_type is not one of the expected values
select *
from {{ ref('fact_trips') }}
where cast(payment_type as integer) not in (0, 1, 2, 3, 4, 5, 6)
   or payment_type is null
