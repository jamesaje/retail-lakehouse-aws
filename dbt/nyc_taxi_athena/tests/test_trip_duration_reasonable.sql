select *
from {{ ref('fact_trips') }}
where trip_duration_minutes < 0
   or trip_duration_minutes > 24 * 60
