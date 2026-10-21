-- location_views_hourly on Flink SQL: the same rules as src/nexus/stream.py and java/src/nexus/WindowJob.java.
-- Written to be read: there is no Flink cluster behind a portfolio. The Python and Java versions are the ones tested.

SET 'table.exec.source.idle-timeout' = '5 min';             -- an idle partition stops holding the watermark back
SET 'table.exec.emit.allow-lateness' = '1 h';                -- keep window state an hour past the watermark
SET 'table.exec.emit.late-fire.enabled' = 'true';            -- and re-fire a window when a late record changes it
SET 'table.exec.emit.late-fire.delay' = '0 s';
SET 'table.exec.state.ttl' = '24 h';                         -- de-duplication state for event_id

CREATE TABLE page_views (
  event_id      STRING,
  event_time    TIMESTAMP(3),
  session_id    STRING,
  user_hash     STRING,
  location_id   STRING,
  page_type     STRING,
  platform      STRING,
  app_version   STRING,
  WATERMARK FOR event_time AS event_time - INTERVAL '10' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'page_views',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'properties.group.id' = 'nexus-page-views-hourly',
  'scan.startup.mode' = 'group-offsets',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- producer retries re-send an event_id at a new offset: keep the first arrival
CREATE TEMPORARY VIEW page_views_once AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY PROCTIME()) AS rn FROM page_views
) WHERE rn = 1;

CREATE TABLE location_views_hourly (
  window_start  TIMESTAMP(3),
  location_id   STRING,
  views         BIGINT,
  PRIMARY KEY (window_start, location_id) NOT ENFORCED       -- upsert: a re-fired window replaces itself
) WITH (
  'connector' = 'upsert-kafka',                              -- Snowflake / BigQuery connectors load from here
  'topic' = 'gold.location_views_hourly',
  'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
  'key.format' = 'json',
  'value.format' = 'json'
);

INSERT INTO location_views_hourly
SELECT window_start, location_id, COUNT(*) AS views
FROM TABLE(TUMBLE(TABLE page_views_once, DESCRIPTOR(event_time), INTERVAL '1' HOUR))
GROUP BY window_start, window_end, location_id;
