CREATE TABLE IF NOT EXISTS `nexus-data.gold.reviews_current` (
  review_id STRING,
  location_id STRING,
  rating INT64,
  status STRING,
  lang STRING,
  version INT64,
  _loaded_at TIMESTAMP
)
CLUSTER BY location_id;

MERGE INTO `nexus-data.gold.reviews_current` t
USING `nexus-data.stage.reviews_current` s
  ON t.review_id = s.review_id
WHEN MATCHED AND s.erased AND s.version > t.version THEN DELETE
WHEN MATCHED AND NOT s.erased AND s.version > t.version THEN UPDATE SET location_id = s.location_id, rating = s.rating, status = s.status, lang = s.lang, version = s.version, _loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND NOT s.erased THEN INSERT (review_id, location_id, rating, status, lang, version, _loaded_at)
  VALUES (s.review_id, s.location_id, s.rating, s.status, s.lang, s.version, CURRENT_TIMESTAMP());
