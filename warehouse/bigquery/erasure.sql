DELETE FROM `nexus-data.raw.reviews` WHERE review_id IN UNNEST(@review_ids);
DELETE FROM `nexus-data.gold.reviews_current` WHERE review_id IN UNNEST(@review_ids);
-- Time travel keeps deleted rows for the dataset's max_time_travel_hours: 48 on raw, so erasure completes in two days.
