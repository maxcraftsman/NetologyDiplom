CREATE TABLE `default`.gender_daily (
	`date` Date,
	gender_0 UInt16,
	gender_1 UInt16,
	gender_2 UInt16
) ENGINE = ReplacingMergeTree
ORDER BY `date`;
