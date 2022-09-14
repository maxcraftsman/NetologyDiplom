CREATE TABLE `default`.avg_duration_daily (
	`date` Date,
	avg_trip_dur Float
) ENGINE = ReplacingMergeTree(avg_trip_dur)
ORDER BY `date`;
