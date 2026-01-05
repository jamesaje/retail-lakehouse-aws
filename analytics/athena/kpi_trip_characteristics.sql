-- Trip characteristics KPI Analysis

SELECT
    CASE
        WHEN trip_distance < 1 THEN '<1 mile'
        WHEN trip_distance < 3 THEN '1-3 miles'
        WHEN trip_distance < 5 THEN '3-5 miles'
        ELSE '5+ miles'
    END AS distance_bucket,
    COUNT(*) AS trips,
    AVG(total_amount) AS avg_fare,
    AVG(trip_duration_minutes) AS avg_duration_minutes
FROM nyc_taxi_lakehouse.fact_trips
GROUP BY 1
ORDER BY 1