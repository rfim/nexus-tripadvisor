-- partner_revenue_15m on Flink SQL. Written to be read; src/nexus/stream.py is the tested version.
-- Two lessons from the incident are in the source table: bad records don't stop the job, and both schema
-- versions are read. Flink SQL can only skip a bad record ('json.ignore-parse-errors'); to keep it, as the
-- dead-letter topic does, the source is a DataStream job with a side output, as in the Python and Java versions.

SET 'table.exec.source.idle-timeout' = '5 min';
SET 'table.exec.emit.allow-lateness' = '30 min';
SET 'table.exec.emit.late-fire.enabled' = 'true';

CREATE TABLE partner_clicks (
  click_id      STRING,
  event_time    TIMESTAMP(3),
  session_id    STRING,
  location_id   STRING,
  partner       STRING,
  platform      STRING,
  cpc_gbp       DECIMAL(12, 4),                                -- schema v1
  price         ROW<amount DECIMAL(12, 4), currency STRING>,   -- schema v2, app 7.4
  revenue_gbp AS COALESCE(cpc_gbp, price.amount * CASE price.currency
                  WHEN 'GBP' THEN 1.0 WHEN 'EUR' THEN 0.86 WHEN 'USD' THEN 0.79 END),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'partner_clicks',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'nexus-partner-revenue',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE partner_revenue_15m (
  window_start  TIMESTAMP(3),
  partner       STRING,
  clicks        BIGINT,
  revenue_gbp   DECIMAL(14, 4),
  PRIMARY KEY (window_start, partner) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'gold.partner_revenue_15m',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO partner_revenue_15m
SELECT window_start, partner, COUNT(*) AS clicks, SUM(revenue_gbp) AS revenue_gbp
FROM TABLE(TUMBLE(TABLE partner_clicks, DESCRIPTOR(event_time), INTERVAL '15' MINUTE))
GROUP BY window_start, window_end, partner;
