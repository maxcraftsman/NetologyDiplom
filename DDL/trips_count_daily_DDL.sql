CREATE TABLE `default`.trips_count_daily (
	`date` Date,
	trips_count UInt16
) ENGINE = AggregatingMergeTree()
ORDER BY `date`;
