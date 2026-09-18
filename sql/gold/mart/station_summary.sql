CREATE OR REPLACE VIEW
`jcdeah-009.final_project_jonathan_gold_mart.mart_station_summary`
AS

WITH station_trips AS (

    SELECT
        start_station_id AS batch_station_id,
        trip_date,
        ride_hour AS activity_hour,
        'START' AS activity_type
    FROM
        `jcdeah-009.final_project_jonathan_silver_transform.fact_citibike_trips_cleaned`

    UNION ALL

    SELECT
        end_station_id AS batch_station_id,
        trip_date,
        EXTRACT(HOUR FROM ended_at) AS activity_hour,
        'END' AS activity_type
    FROM
        `jcdeah-009.final_project_jonathan_silver_transform.fact_citibike_trips_cleaned`
),

station_summary AS (

    SELECT
        batch_station_id,

        COUNTIF(activity_type = 'START') AS start_trip_count,
        COUNTIF(activity_type = 'END') AS end_trip_count,

        COUNT(*) AS total_station_activity,

        ROUND(COUNT(*) / COUNT(DISTINCT trip_date),2)
            AS avg_daily_activity

    FROM station_trips

    WHERE batch_station_id IS NOT NULL

    GROUP BY batch_station_id
),

hour_count AS (

    SELECT
        batch_station_id,
        activity_hour,
        COUNT(*) AS total_activity

    FROM station_trips

    WHERE batch_station_id IS NOT NULL

    GROUP BY
        batch_station_id,
        activity_hour
),

peak_hour AS (

    SELECT
        batch_station_id,
        activity_hour AS peak_hour

    FROM hour_count

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY batch_station_id
        ORDER BY total_activity DESC
    ) = 1
)

SELECT
    s.batch_station_id AS station_id,

    COALESCE(
        r.batch_station_name,
        r.stream_station_name
    ) AS station_name,

    s.start_trip_count,
    s.end_trip_count,
    s.total_station_activity,
    s.avg_daily_activity,

    p.peak_hour

FROM station_summary s

LEFT JOIN
    `jcdeah-009.final_project_jonathan_silver_transform.dim_station_mapping` r
ON s.batch_station_id = r.batch_station_id

LEFT JOIN peak_hour p
ON s.batch_station_id = p.batch_station_id;