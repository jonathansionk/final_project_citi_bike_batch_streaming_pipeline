CREATE OR REPLACE TABLE
`jcdeah-009.final_project_jonathan_staging.dim_stream_station_reference`
AS

SELECT
    station_id AS stream_station_id,
    station_name AS stream_station_name,
    SAFE_CAST(lat AS FLOAT64) AS stream_lat,
    SAFE_CAST(lon AS FLOAT64) AS stream_lon,
    SAFE_CAST(capacity AS INT64) AS capacity

FROM
    `jcdeah-009.final_project_jonathan_staging.fact_valid_stream_station_event`

WHERE
    station_id IS NOT NULL
    AND station_name IS NOT NULL

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY station_id
    ORDER BY event_timestamp DESC
) = 1;