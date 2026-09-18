CREATE OR REPLACE VIEW
`jcdeah-009.final_project_jonathan_gold_mart.mart_latest_station_status`
AS

SELECT *
FROM `jcdeah-009.final_project_jonathan_silver_transform.fact_valid_stream_cleaned`

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY station_id
    ORDER BY event_timestamp DESC
) = 1;