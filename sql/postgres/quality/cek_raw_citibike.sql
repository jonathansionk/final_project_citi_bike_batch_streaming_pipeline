SELECT COUNT(*)
FROM raw_citibike_trips
WHERE started_at >= '2026-01-01'
  AND started_at < '2027-01-01';