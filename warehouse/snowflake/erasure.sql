DELETE FROM NEXUS.RAW.REVIEWS WHERE review_id IN (SELECT value FROM TABLE(FLATTEN(INPUT => PARSE_JSON(:review_ids))));
DELETE FROM NEXUS.GOLD.REVIEWS_CURRENT WHERE review_id IN (SELECT value FROM TABLE(FLATTEN(INPUT => PARSE_JSON(:review_ids))));
-- Time Travel keeps deleted rows for DATA_RETENTION_TIME_IN_DAYS: 1 on raw.reviews, so erasure completes in a day.
