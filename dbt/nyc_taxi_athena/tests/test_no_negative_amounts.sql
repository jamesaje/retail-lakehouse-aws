select *
from {{ ref('fact_trips') }}
where total_amount < 0
   or trip_distance < 0
