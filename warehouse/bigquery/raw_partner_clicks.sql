CREATE TABLE IF NOT EXISTS `nexus-data.raw.partner_clicks` (
  click_id STRING,
  event_time TIMESTAMP,
  session_id STRING,
  location_id STRING,
  partner STRING,
  platform STRING,
  cpc_gbp NUMERIC(12,4),
  price_amount NUMERIC(12,4),
  price_currency STRING,
  revenue_gbp NUMERIC(12,4),
  _schema_version INT64,
  _partition INT64,
  _offset INT64,
  _appended_at TIMESTAMP
)
PARTITION BY DATE(_appended_at)
CLUSTER BY _partition;
