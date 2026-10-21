CREATE TABLE IF NOT EXISTS `nexus-data.raw.reviews` (
  event_id STRING,
  event_time TIMESTAMP,
  review_id STRING,
  version INT64,
  op STRING,
  location_id STRING,
  rating INT64,
  status STRING,
  reviewer_hash STRING OPTIONS(description = 'personal data: policy tag pii.restricted'),
  reviewer_email STRING OPTIONS(description = 'personal data: policy tag pii.restricted'),
  text_chars INT64,
  lang STRING,
  _schema_version INT64,
  _partition INT64,
  _offset INT64,
  _appended_at TIMESTAMP
)
PARTITION BY DATE(_appended_at)
CLUSTER BY _partition;
