CREATE OR REPLACE TABLE
`jcdeah-009.final_project_jonathan_silver_transform.dim_station_mapping`
AS

SELECT
    COALESCE(b.batch_station_id, 'NO MATCH BATCH') AS batch_station_id,
    COALESCE(b.batch_station_name, 'NO MATCH BATCH') AS batch_station_name,

    COALESCE(s.stream_station_id, 'NO STREAM MATCH') AS stream_station_id,
    COALESCE(s.stream_station_name, 'NO STREAM MATCH') AS stream_station_name,

    b.batch_lat,
    b.batch_lon,

    s.stream_lat,
    s.stream_lon

FROM
    `jcdeah-009.final_project_jonathan_staging.dim_batch_station_reference` b

FULL OUTER JOIN
    `jcdeah-009.final_project_jonathan_staging.dim_stream_station_reference` s

ON LOWER(TRIM(b.batch_station_name))
=
LOWER(TRIM(s.stream_station_name));