CREATE TABLE IF NOT EXISTS NEXUS.GOLD.PARTNER_REVENUE_15M (
  window_start TIMESTAMP_NTZ,
  partner VARCHAR,
  clicks NUMBER(38,0),
  revenue_gbp NUMBER(12,4),
  _loaded_at TIMESTAMP_NTZ
)
CLUSTER BY (TO_DATE(window_start), partner);

MERGE INTO NEXUS.GOLD.PARTNER_REVENUE_15M t
USING NEXUS.STAGE.PARTNER_REVENUE_15M s
  ON t.window_start = s.window_start AND t.partner = s.partner
WHEN MATCHED AND (t.clicks IS DISTINCT FROM s.clicks OR t.revenue_gbp IS DISTINCT FROM s.revenue_gbp) THEN UPDATE SET clicks = s.clicks, revenue_gbp = s.revenue_gbp, _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (window_start, partner, clicks, revenue_gbp, _loaded_at)
  VALUES (s.window_start, s.partner, s.clicks, s.revenue_gbp, CURRENT_TIMESTAMP());
