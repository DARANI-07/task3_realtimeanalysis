SELECT sensor_id, COUNT(*) AS frequency
FROM transformed_data
GROUP BY sensor_id
ORDER BY frequency DESC
LIMIT 10;
