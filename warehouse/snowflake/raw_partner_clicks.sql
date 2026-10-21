CREATE TABLE IF NOT EXISTS NEXUS.RAW.PARTNER_CLICKS (
  click_id VARCHAR,
  event_time TIMESTAMP_NTZ,
  session_id VARCHAR,
  location_id VARCHAR,
  partner VARCHAR,
  platform VARCHAR,
  cpc_gbp NUMBER(12,4),
  price_amount NUMBER(12,4),
  price_currency VARCHAR,
  revenue_gbp NUMBER(12,4),
  _schema_version NUMBER(38,0),
  _partition NUMBER(38,0),
  _offset NUMBER(38,0),
  _appended_at TIMESTAMP_NTZ
)
CLUSTER BY (TO_DATE(_appended_at), _partition);
