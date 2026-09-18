-- cek jumlah data
SELECT 
    COUNT(*)
FROM `jcdeah-009.final_project_jonathan_silver_transform.fact_citibike_trips_cleaned`;

-- cek duplikat ride id
SELECT 
    ride_id,
    COUNT(*) AS total
FROM
    `jcdeah-009.final_project_jonathan_silver_transform.fact_citibike_trips_cleaned`
GROUP BY ride_id
HAVING COUNT(*) > 1;

-- cek apakah masih ada data NULL di field tertentu
SELECT
    COUNTIF(ride_id IS NULL OR ride_id = '') AS invalid_ride_id,
    COUNTIF(started_at IS NULL) AS invalid_started_at,
    COUNTIF(ended_at IS NULL) AS invalid_ended_at,
    COUNTIF(start_station_id IS NULL OR start_station_id = '') AS invalid_start_station,
    COUNTIF(end_station_id IS NULL OR end_station_id = '') AS invalid_end_station
FROM
    `jcdeah-009.final_project_jonathan_silver_transform.fact_citibike_trips_cleaned`;


SELECT COUNT(*) AS total_rows
FROM `jcdeah-009.final_project_jonathan_silver_transform.fact_valid_stream_cleaned`;

SELECT COUNT(*) AS total_rows
FROM `jcdeah-009.final_project_jonathan_silver_transform.dim_station_mapping`;