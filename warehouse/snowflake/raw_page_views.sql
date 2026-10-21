CREATE TABLE IF NOT EXISTS NEXUS.RAW.PAGE_VIEWS (
  event_id VARCHAR,
  event_time TIMESTAMP_NTZ,
  session_id VARCHAR,
  user_hash VARCHAR,
  location_id VARCHAR,
  page_type VARCHAR,
  platform VARCHAR,
  app_version VARCHAR,
  _schema_version NUMBER(38,0),
  _partition NUMBER(38,0),
  _offset NUMBER(38,0),
  _appended_at TIMESTAMP_NTZ
)
CLUSTER BY (TO_DATE(_appended_at), _partition);
