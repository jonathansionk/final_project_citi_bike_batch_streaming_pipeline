CREATE OR REPLACE VIEW
`jcdeah-009.final_project_jonathan_gold_mart.mart_station_anomaly_monitoring`
AS

WITH stream_history AS (

    SELECT
        station_id,
        station_name,

        num_bikes_available,
        num_docks_available,
        event_timestamp,

        LAG(num_bikes_available) OVER (
            PARTITION BY station_id
            ORDER BY event_timestamp
        ) AS previous_bikes_available,

        LAG(num_docks_available) OVER (
            PARTITION BY station_id
            ORDER BY event_timestamp
        ) AS previous_docks_available,

        LAG(event_timestamp) OVER (
            PARTITION BY station_id
            ORDER BY event_timestamp
        ) AS previous_event_timestamp

    FROM
        `jcdeah-009.final_project_jonathan_silver_transform.fact_valid_stream_cleaned`
),

stream_change AS (

    SELECT
        *,

        TIMESTAMP_DIFF(
            event_timestamp,
            previous_event_timestamp,
            SECOND
        ) AS time_diff_seconds,

        previous_bikes_available
            - num_bikes_available AS bikes_difference,

        previous_docks_available
            - num_docks_available AS docks_difference

    FROM stream_history
),

anomaly_detection AS (

    SELECT
        *,

        CASE

            WHEN
                time_diff_seconds <= 60
                AND bikes_difference >= 50
            THEN 'ANOMALY BIKE DROPS'

            ELSE 'NORMAL'

        END AS anomaly_status

    FROM stream_change
)

SELECT
    station_id,
    station_name,

    num_bikes_available,
    previous_bikes_available,
    bikes_difference,

    num_docks_available,
    previous_docks_available,
    docks_difference,

    time_diff_seconds,
    anomaly_status,
    event_timestamp

FROM anomaly_detection;