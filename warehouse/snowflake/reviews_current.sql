CREATE TABLE IF NOT EXISTS NEXUS.GOLD.REVIEWS_CURRENT (
  review_id VARCHAR,
  location_id VARCHAR,
  rating NUMBER(38,0),
  status VARCHAR,
  lang VARCHAR,
  version NUMBER(38,0),
  _loaded_at TIMESTAMP_NTZ
)
CLUSTER BY (location_id);

MERGE INTO NEXUS.GOLD.REVIEWS_CURRENT t
USING NEXUS.STAGE.REVIEWS_CURRENT s
  ON t.review_id = s.review_id
WHEN MATCHED AND s.erased AND s.version > t.version THEN DELETE
WHEN MATCHED AND NOT s.erased AND s.version > t.version THEN UPDATE SET location_id = s.location_id, rating = s.rating, status = s.status, lang = s.lang, version = s.version, _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND NOT s.erased THEN INSERT (review_id, location_id, rating, status, lang, version, _loaded_at)
  VALUES (s.review_id, s.location_id, s.rating, s.status, s.lang, s.version, CURRENT_TIMESTAMP());
