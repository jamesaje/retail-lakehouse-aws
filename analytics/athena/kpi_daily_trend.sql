-- Daily trips and revenue trend

SELECT
    pickup_date,
    COUNT(trip_id) AS trips,
    SUM(total_amount) AS revenue,
FROM nyc_taxi_lakehouse.fact_trips
GROUP BY pickup_date
ORDER BY pickup_date;