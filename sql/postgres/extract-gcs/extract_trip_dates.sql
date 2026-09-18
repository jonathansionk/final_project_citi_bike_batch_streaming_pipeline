SELECT DISTINCT DATE(started_at) AS trip_date
FROM raw_citibike_trips
WHERE started_at >= %s
  AND started_at < %s
ORDER BY trip_date;