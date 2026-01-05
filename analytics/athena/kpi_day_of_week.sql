-- Trips and revenue by day of the week

SELECT
    d.day_of_week,
    COUNT(f.trip_id) AS trips,
    SUM(f.total_amount) AS revenue,
    AVG(f.total_amount) AS avg_fare,
FROM nyc_taxi_lakehouse.fact_trips f
JOIN nyc_taxi_lakehouse.dim_date d
    ON f.pickup_date = d.date_day
GROUP BY d.day_of_week
ORDER BY d.day_of_week;