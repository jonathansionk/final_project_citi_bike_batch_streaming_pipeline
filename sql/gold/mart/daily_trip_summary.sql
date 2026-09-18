CREATE OR REPLACE VIEW
`jcdeah-009.final_project_jonathan_gold_mart.mart_daily_trip_summary`
AS

WITH daily_summary AS (

    SELECT
        trip_date,

        COUNT(*) AS total_trips,

        ROUND(
            AVG(ride_duration_minutes),
            2
        ) AS avg_ride_duration_minutes,

        COUNT(DISTINCT start_station_id)
            AS active_start_station_count,

        COUNT(DISTINCT end_station_id)
            AS active_end_station_count

    FROM
        `jcdeah-009.final_project_jonathan_silver_transform.fact_citibike_trips_cleaned`

    GROUP BY trip_date
),

hour_count AS (

    SELECT
        trip_date,
        ride_hour,
        COUNT(*) AS total_trips

    FROM
        `jcdeah-009.final_project_jonathan_silver_transform.fact_citibike_trips_cleaned`

    GROUP BY
        trip_date,
        ride_hour
),

peak_hour AS (

    SELECT
        trip_date,
        ride_hour AS peak_hour

    FROM hour_count

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY trip_date
        ORDER BY total_trips DESC
    ) = 1
)

SELECT
    d.trip_date,
    d.total_trips,
    d.avg_ride_duration_minutes,
    d.active_start_station_count,
    d.active_end_station_count,
    p.peak_hour

FROM daily_summary d

LEFT JOIN peak_hour p
    ON d.trip_date = p.trip_date;