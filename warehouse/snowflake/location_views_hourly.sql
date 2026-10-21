CREATE TABLE IF NOT EXISTS NEXUS.GOLD.LOCATION_VIEWS_HOURLY (
  window_start TIMESTAMP_NTZ,
  location_id VARCHAR,
  views NUMBER(38,0),
  _loaded_at TIMESTAMP_NTZ
)
CLUSTER BY (TO_DATE(window_start), location_id);

MERGE INTO NEXUS.GOLD.LOCATION_VIEWS_HOURLY t
USING NEXUS.STAGE.LOCATION_VIEWS_HOURLY s
  ON t.window_start = s.window_start AND t.location_id = s.location_id
WHEN MATCHED AND (t.views IS DISTINCT FROM s.views) THEN UPDATE SET views = s.views, _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (window_start, location_id, views, _loaded_at)
  VALUES (s.window_start, s.location_id, s.views, CURRENT_TIMESTAMP());
