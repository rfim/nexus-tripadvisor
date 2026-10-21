CREATE TABLE IF NOT EXISTS `nexus-data.gold.location_views_hourly` (
  window_start TIMESTAMP,
  location_id STRING,
  views INT64,
  _loaded_at TIMESTAMP
)
PARTITION BY DATE(window_start)
CLUSTER BY location_id;

MERGE INTO `nexus-data.gold.location_views_hourly` t
USING `nexus-data.stage.location_views_hourly` s
  ON t.window_start = s.window_start AND t.location_id = s.location_id
WHEN MATCHED AND (t.views IS DISTINCT FROM s.views) THEN UPDATE SET views = s.views, _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (window_start, location_id, views, _loaded_at)
  VALUES (s.window_start, s.location_id, s.views, CURRENT_TIMESTAMP());
