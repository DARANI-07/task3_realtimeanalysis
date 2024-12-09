SELECT 
  from_unixtime(cast(timestamp as BIGINT), '2004-12-11 09:00:00') AS hour,
  COUNT(*) AS record_count
FROM transformed_data
GROUP BY from_unixtime(cast(timestamp as BIGINT), '2004-12-11 09:00:00');
