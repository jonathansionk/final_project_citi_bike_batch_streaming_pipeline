CREATE OR REPLACE VIEW
`jcdeah-009.final_project_jonathan_gold_mart.mart_user_type_summary`
AS

SELECT
    member_casual,

    COUNT(*) AS total_trips,

    ROUND(
        COUNT(*) * 100.0
        / SUM(COUNT(*)) OVER (),
        2
    ) AS trip_percentage,

    ROUND(
        AVG(ride_duration_minutes),
        2
    ) AS avg_ride_duration_minutes

FROM
    `jcdeah-009.final_project_jonathan_silver_transform.fact_citibike_trips_cleaned`

WHERE
    member_casual IS NOT NULL

GROUP BY
    member_casual;