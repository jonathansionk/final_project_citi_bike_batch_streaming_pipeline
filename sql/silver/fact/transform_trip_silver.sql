CREATE OR REPLACE TABLE
`jcdeah-009.final_project_jonathan_silver_transform.fact_citibike_trips_cleaned`

PARTITION BY trip_date

CLUSTER BY
    member_casual,
    start_station_id

AS


-- =========================
-- 1. MERUBAH TIPE DATA
-- =========================

WITH typed AS (

    SELECT

        NULLIF(TRIM(ride_id), '') AS ride_id,

        NULLIF(TRIM(rideable_type), '') AS rideable_type,

        SAFE_CAST(started_at AS TIMESTAMP) AS started_at,

        SAFE_CAST(ended_at AS TIMESTAMP) AS ended_at,

        NULLIF(TRIM(start_station_name),'') AS start_station_name,

        NULLIF(TRIM(start_station_id),'') AS start_station_id,

        NULLIF(TRIM(end_station_name),'') AS end_station_name,

        NULLIF(TRIM(end_station_id),'') AS end_station_id,

        SAFE_CAST(start_lat AS FLOAT64) AS start_lat,

        SAFE_CAST(start_lng AS FLOAT64) AS start_lng,

        SAFE_CAST(end_lat AS FLOAT64) AS end_lat,

        SAFE_CAST(end_lng AS FLOAT64) AS end_lng,

        LOWER(
            TRIM(member_casual)
        ) AS member_casual,

        SAFE_CAST(
            trip_date AS DATE
        ) AS trip_date

    FROM
        `jcdeah-009.final_project_jonathan_staging.fact_citibike_trips_stg`
),


-- =========================
-- 2. PENAMBAHAN DATA
-- =========================

enriched AS (

    SELECT

        *,

        ROUND(TIMESTAMP_DIFF(
            ended_at,
            started_at,
            SECOND
        ) / 60.0,2)
            AS ride_duration_minutes,

        EXTRACT(
            HOUR
            FROM started_at
        ) AS ride_hour,

        FORMAT_TIMESTAMP(
            '%A',
            started_at
        ) AS ride_day_of_week,

        EXTRACT(
            MONTH
            FROM started_at
        ) AS ride_month,

        EXTRACT(
            YEAR
            FROM started_at
        ) AS ride_year,

        EXTRACT(
            DAYOFWEEK
            FROM started_at
        ) IN (1, 7) AS is_weekend,

        CASE

            WHEN EXTRACT(
                HOUR FROM started_at
            ) BETWEEN 5 AND 11
                THEN 'Morning'

            WHEN EXTRACT(
                HOUR FROM started_at
            ) BETWEEN 12 AND 16
                THEN 'Afternoon'

            WHEN EXTRACT(
                HOUR FROM started_at
            ) BETWEEN 17 AND 20
                THEN 'Evening'

            ELSE 'Night'

        END AS time_of_day,

        CASE

            WHEN start_lat BETWEEN -90 AND 90
             AND end_lat BETWEEN -90 AND 90
             AND start_lng BETWEEN -180 AND 180
             AND end_lng BETWEEN -180 AND 180

            THEN ROUND(ST_DISTANCE(

                ST_GEOGPOINT(
                    start_lng,
                    start_lat
                ),

                ST_GEOGPOINT(
                    end_lng,
                    end_lat
                )

            ) / 1000,2)

        END AS trip_distance_km

    FROM typed
),


-- =========================
-- 3. DATA QUALITY
-- =========================

cleaned AS (

    SELECT *

    FROM enriched

    WHERE

        ride_id IS NOT NULL

        AND started_at IS NOT NULL

        AND ended_at IS NOT NULL

        AND ended_at > started_at

        AND ride_duration_minutes > 0

        AND ride_duration_minutes <= 1440

        AND start_station_id IS NOT NULL

        AND end_station_id IS NOT NULL

        AND member_casual
            IN ('member', 'casual')
)


-- =========================
-- 4. REMOVE DUPLICATE
-- =========================

SELECT *

FROM cleaned

QUALIFY

    ROW_NUMBER() OVER (
        PARTITION BY ride_id
        ORDER BY started_at
    ) = 1;