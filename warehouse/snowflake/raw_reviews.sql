CREATE TABLE IF NOT EXISTS NEXUS.RAW.REVIEWS (
  event_id VARCHAR,
  event_time TIMESTAMP_NTZ,
  review_id VARCHAR,
  version NUMBER(38,0),
  op VARCHAR,
  location_id VARCHAR,
  rating NUMBER(38,0),
  status VARCHAR,
  reviewer_hash VARCHAR,
  reviewer_email VARCHAR,
  text_chars NUMBER(38,0),
  lang VARCHAR,
  _schema_version NUMBER(38,0),
  _partition NUMBER(38,0),
  _offset NUMBER(38,0),
  _appended_at TIMESTAMP_NTZ
)
CLUSTER BY (TO_DATE(_appended_at), _partition);
