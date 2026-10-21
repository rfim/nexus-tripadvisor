CREATE TABLE IF NOT EXISTS `nexus-data.gold.partner_revenue_15m` (
  window_start TIMESTAMP,
  partner STRING,
  clicks INT64,
  revenue_gbp NUMERIC(12,4),
  _loaded_at TIMESTAMP
)
PARTITION BY DATE(window_start)
CLUSTER BY partner;

MERGE INTO `nexus-data.gold.partner_revenue_15m` t
USING `nexus-data.stage.partner_revenue_15m` s
  ON t.window_start = s.window_start AND t.partner = s.partner
WHEN MATCHED AND (t.clicks IS DISTINCT FROM s.clicks OR t.revenue_gbp IS DISTINCT FROM s.revenue_gbp) THEN UPDATE SET clicks = s.clicks, revenue_gbp = s.revenue_gbp, _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (window_start, partner, clicks, revenue_gbp, _loaded_at)
  VALUES (s.window_start, s.partner, s.clicks, s.revenue_gbp, CURRENT_TIMESTAMP());
