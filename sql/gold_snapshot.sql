INSERT INTO convergence.daily_reach_snapshot
SELECT event_day AS day, campaign_id, segment, delivery_type,
       count(distinct individual_id)                         AS exact_reach,
       count(*)                                              AS impressions,
       cast(approx_set(individual_id) AS varbinary)          AS hll_sketch
FROM convergence.silver_impressions
WHERE event_day BETWEEN date '__START__' AND date '__END__'
GROUP BY event_day, campaign_id, segment, delivery_type;
