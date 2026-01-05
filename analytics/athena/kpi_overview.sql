-- Executive Summary KPI Overview for NYC Taxi Trips

SELECT
    COUNT(trip_id) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_revenue_per_trip,
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
    AVG(trip_distance) AS avg_trip_distance
FROM nyc_taxi_lakehouse.fact_trips