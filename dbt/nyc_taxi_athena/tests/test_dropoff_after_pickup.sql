select *
from {{ ref('fact_trips') }}
where dropoff_ts < pickup_ts
