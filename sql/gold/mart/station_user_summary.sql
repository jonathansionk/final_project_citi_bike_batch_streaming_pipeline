CREATE OR REPLACE VIEW
`jcdeah-009.final_project_jonathan_gold_mart.mart_station_user_summary`
AS

SELECT
    start_station_id AS station_id,
    start_station_name AS station_name,

    COUNTIF(member_casual = 'member') AS member_trips,

    COUNTIF(member_casual = 'casual') AS casual_trips,

    COUNT(*) AS total_trips,

    ROUND(
        COUNTIF(member_casual = 'member') * 100.0
        / COUNT(*),
        2
    ) AS member_percentage,

    ROUND(
        COUNTIF(member_casual = 'casual') * 100.0
        / COUNT(*),
        2
    ) AS casual_percentage

FROM
    `jcdeah-009.final_project_jonathan_silver_transform.fact_citibike_trips_cleaned`

WHERE
    start_station_id IS NOT NULL

GROUP BY
    start_station_id,
    start_station_name;