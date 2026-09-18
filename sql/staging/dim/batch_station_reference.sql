CREATE OR REPLACE TABLE
`jcdeah-009.final_project_jonathan_staging.dim_batch_station_reference`
AS

WITH stations AS (

    SELECT
        start_station_id AS station_id,
        start_station_name AS station_name,
        SAFE_CAST(start_lat AS FLOAT64) AS lat,
        SAFE_CAST(start_lng AS FLOAT64) AS lon
    FROM
        `jcdeah-009.final_project_jonathan_staging.fact_citibike_trips_stg`

    UNION ALL

    SELECT
        end_station_id AS station_id,
        end_station_name AS station_name,
        SAFE_CAST(end_lat AS FLOAT64) AS lat,
        SAFE_CAST(end_lng AS FLOAT64) AS lon
    FROM
        `jcdeah-009.final_project_jonathan_staging.fact_citibike_trips_stg`
)

SELECT
    station_id AS batch_station_id,
    ANY_VALUE(station_name) AS batch_station_name,
    AVG(lat) AS batch_lat,
    AVG(lon) AS batch_lon

FROM stations

WHERE
    station_id IS NOT NULL
    AND station_name IS NOT NULL

GROUP BY station_id;