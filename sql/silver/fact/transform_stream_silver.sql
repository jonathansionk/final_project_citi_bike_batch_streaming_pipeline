CREATE OR REPLACE TABLE
`jcdeah-009.final_project_jonathan_silver_transform.fact_valid_stream_cleaned`

PARTITION BY DATE(event_timestamp)
CLUSTER BY station_id

AS

SELECT
    TRIM(station_id) AS station_id,
    TRIM(station_name) AS station_name,

    lat,
    lon,
    capacity,

    num_bikes_available,
    num_docks_available,
    num_ebikes_available,

    is_installed,
    is_renting,
    is_returning,

    event_timestamp

FROM
    `jcdeah-009.final_project_jonathan_staging.fact_valid_stream_station_event`

-- =========================
-- DATA QUALITY
-- =========================

WHERE
    station_id IS NOT NULL
    AND TRIM(station_id) != ''

    AND station_name IS NOT NULL
    AND TRIM(station_name) != ''

    AND lat BETWEEN -90 AND 90
    AND lon BETWEEN -180 AND 180

    AND capacity >= 0

    AND num_bikes_available >= 0
    AND num_docks_available >= 0

    AND event_timestamp IS NOT NULL

-- =========================
-- REMOVE DUPLICATE
-- =========================
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY station_id, event_timestamp
        ORDER BY event_timestamp
    ) = 1;