# AWS console screenshot shot-list — for the WBD share

The live dashboard already shows the AWS pipeline output (see `01-live-dashboard.png`,
captured from the real deployment). To also show the **AWS infrastructure itself**, grab
the console screens below while signed in to account `194611079924`, region **us-east-1**.
Deep links are pre-filtered; each note says what to frame and why it lands.

Suggested order = the story: raw data → catalog → compute → query → orchestration → public front door.

| # | Service | Deep link | Capture | Why it matters |
|---|---------|-----------|---------|----------------|
| 1 | **Live dashboard** | https://miatibcmck.us-east-1.awsapprunner.com | Full page (already have `01-live-dashboard.png`) | The payoff — live reach + the "Running on AWS" panel, no login |
| 2 | **S3 lakehouse** | https://us-east-1.console.aws.amazon.com/s3/buckets/convergence-lakehouse-194611079924 | The prefix list: `bronze/ silver/ gold/ landing/ emr-logs/` | Real medallion layout on S3 |
| 3 | **Glue Data Catalog** | https://us-east-1.console.aws.amazon.com/glue/home?region=us-east-1#/v2/data-catalog/tables | `convergence` DB — `bronze_impressions`, `silver_impressions`, `daily_reach_snapshot` | The cataloged tables behind every query |
| 4 | **EMR Serverless** | https://us-east-1.console.aws.amazon.com/emr/home?region=us-east-1#/serverless | App `convergence-spark` → **Job runs**, showing the 2 SUCCESS runs (2026-07-08) | Proof the Spark ETL actually ran |
| 5 | **Athena** | https://us-east-1.console.aws.amazon.com/athena/home?region=us-east-1#/query-editor | Run the HLL query below; screenshot the result (292) + the query text | Shows sketch-merge math at query time |
| 6 | **MWAA (Airflow)** | https://us-east-1.console.aws.amazon.com/mwaa/home?region=us-east-1#environments | Environment `convergence-mwaa` (Airflow 2.9.2, mw1.small) | Managed orchestration. NOTE: was still `CREATING` — wait for `Available`, then also grab the DAG graph from the Airflow UI |
| 7 | **App Runner** | https://us-east-1.console.aws.amazon.com/apprunner/home?region=us-east-1#/services | Service `convergence-dashboard` — status **Running** + the public URL | The public HTTPS front door |

## Athena query for shot #5 (paste into the query editor, workgroup `convergence-wg`)

```sql
SELECT cardinality(merge(cast(hll_sketch AS HyperLogLog))) AS cumulative_reach
FROM convergence.daily_reach_snapshot
WHERE campaign_id = 'camp_finals'
  AND segment = 'sports'
  AND day BETWEEN date '2026-07-01' AND date '2026-07-05';
-- returns ~292 : the HLL-merged unique reach vs 1,490 naive sum-of-daily
```

## Notes
- Crop out any personal account details you don't want shared (account number appears in ARNs/URLs — it's already in the public S3 URL, so not sensitive, but crop if you prefer).
- Dark mode consoles photograph better against the one-pager's dark theme.
- If MWAA isn't `Available` yet, skip #6 rather than show a half-created environment.
