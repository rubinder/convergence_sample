CREATE TABLE IF NOT EXISTS convergence.bronze_impressions (
  event_id string, event_ts timestamp, delivery_type string,
  individual_id string, household_id string, campaign_id string,
  creative_id string, segment string, network string, geo string, device string,
  ingest_date string
) PARTITIONED BY (ingest_date)
LOCATION 's3://__BUCKET__/bronze/'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');

CREATE TABLE IF NOT EXISTS convergence.silver_impressions (
  event_id string, event_ts timestamp, event_day date, delivery_type string,
  individual_id string, household_id string, campaign_id string,
  segment string, network string, geo string, device string
) PARTITIONED BY (event_day)
LOCATION 's3://__BUCKET__/silver/'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');

CREATE TABLE IF NOT EXISTS convergence.daily_reach_snapshot (
  day date, campaign_id string, segment string, delivery_type string,
  exact_reach bigint, impressions bigint, hll_sketch varbinary
) PARTITIONED BY (day)
LOCATION 's3://__BUCKET__/gold/'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet');
