CREATE TABLE IF NOT EXISTS `nexus-data.raw.page_views` (
  event_id STRING,
  event_time TIMESTAMP,
  session_id STRING,
  user_hash STRING OPTIONS(description = 'personal data: policy tag pii.restricted'),
  location_id STRING,
  page_type STRING,
  platform STRING,
  app_version STRING,
  _schema_version INT64,
  _partition INT64,
  _offset INT64,
  _appended_at TIMESTAMP
)
PARTITION BY DATE(_appended_at)
CLUSTER BY _partition;
