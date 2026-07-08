# Convergence Reach Lakehouse — Design Spec

**Date:** 2026-07-08
**Status:** Draft for review
**Owner:** Rob Randhawa (Senior Staff Data Engineer, WBD Ad Sales / Convergence)

## 1. Purpose

Demonstrate a functioning, cloud-native reach-measurement lakehouse for **convergence**
advertising — the unification of **digital** (ad server) and **linear** (national TV)
ad delivery into a single, manipulable data product. The demo mirrors the target AWS
architecture (landing → bronze → silver → gold → semantic layer), proves the core
advertising metric **Reach** at exact and approximate (HyperLogLog) grains, is
orchestrated by managed Airflow, exposes a natural-language **agentic** interface, and is
reachable by a **public URL** for stakeholders without AWS access.

Success = by tomorrow morning, a person with only a browser link can:
1. See live daily + cumulative + cross-segment reach computed from processed events.
2. Ask an agent natural-language reach questions and get correct answers.
3. See that the pipeline (ingest + daily compaction) is orchestrated and running.

## 2. Locked decisions

| Decision | Choice |
|---|---|
| Runtime | **Real AWS**, region **us-east-1** (account ready, deploy now) |
| Compute (ETL) | **PySpark on EMR Serverless** (bronze/silver) |
| HLL sketches | **Athena/Presto** (`approx_set`/`merge`/`cardinality`) — NOT spark-alchemy |
| IaC | **Terraform** (single root) |
| Orchestration | **MWAA (managed Airflow) first** — backbone driving EMR + Athena |
| Agentic layer | **Bedrock AgentCore** — Gateway tools + managed-harness agent |
| Public access | **AWS App Runner** public HTTPS URL (FastAPI + HTML dashboard) |
| Shareable summary | **Claude Artifact** (architecture + live-data one-pager) |
| Payoffs | Reach dashboard · semantic/metrics layer · daily compaction DAG |

### 2.1 Key technical finding driving the HLL design

`spark-alchemy` HLL sketches are **not interoperable** with Presto/Athena HLL (different
implementations: AGKN/StreamLib vs. Airlift). Because the dashboard and agent must merge
sketches **live at query time** in the query engine, HLL sketch **generation and merging
happen in Athena/Presto**, not Spark. Spark/EMR does the heavy bronze→silver ETL; Athena
produces the Gold snapshots + sketches and answers all reach queries. This keeps "merge
across segments without rescanning raw" as a single-engine, live query.

## 3. Data domain & model

One event = one **ad exposure**. Unified schema across delivery methods:

| Field | Type | Notes |
|---|---|---|
| `event_id` | string | unique per exposure |
| `event_ts` | timestamp | exposure time (UTC) |
| `delivery_type` | string | `digital` \| `linear` |
| `individual_id` | string | resolved person id (identity) |
| `household_id` | string | resolved household id |
| `campaign_id` | string | advertising campaign |
| `creative_id` | string | ad creative |
| `segment` | string | audience segment (e.g. `sports`, `news`, `drama`) |
| `network` | string | e.g. CNN, TNT, Max, Discovery+ |
| `geo` | string | DMA / region |
| `device` | string | ctv / mobile / desktop / set-top |

**Reach** (business definition): the count of **unique individuals or households** exposed
to an ad within a defined time window. Exact reach = `count(distinct individual_id)`.
Approximate reach = HLL cardinality of the merged sketch (mergeable across days/segments
without rescanning raw impressions).

## 4. Architecture

```
Data generator ─▶ S3 Landing (raw JSON: digital logs + linear feeds)
                      │  (MWAA: convergence_ingest DAG)
                      ▼
              EMR Serverless (PySpark)
                 ├─ Bronze  (Iceberg): typed raw events, append-only
                 └─ Silver  (Iceberg): conformed unified impressions
                                        (digital+linear, deduped, identity-resolved)
                      │
                      ▼
                 Athena (Presto SQL)
                 └─ Gold (Iceberg): daily_reach_snapshot
                        per (day, campaign, segment, delivery_type):
                          exact_reach, impressions, hll_sketch (varbinary)
                      │
        ┌─────────────┼──────────────────────────────┐
        ▼             ▼                               ▼
 Semantic layer   Athena workgroup             MWAA: daily_compaction DAG
 (reach fns:      (ad-hoc query)               OPTIMIZE REWRITE DATA + VACUUM
  daily /                                       across bronze/silver/gold
  cumulative /
  segment-merge)
        │
        ├──────────────▶ AgentCore Gateway tools ─▶ Bedrock agent (managed harness)
        │
        ▼
 App Runner (FastAPI + HTML dashboard, public HTTPS URL)
   ├─ reach charts (daily trend, cumulative curve, segment overlap)
   └─ agent chat (proxied to Bedrock agent)

 Claude Artifact: shareable architecture + live-data one-pager
```

### 4.1 Storage layers (Iceberg tables in Glue Data Catalog on S3)

- **Landing**: `s3://<bucket>/landing/` raw JSON, partitioned by ingest date.
- **Bronze** `convergence.bronze_impressions`: 1:1 typed events, append-only.
- **Silver** `convergence.silver_impressions`: conformed unified exposures — digital+linear
  merged into one schema, deduped on `event_id`, identity fields populated. Retained raw
  for flexible cumulative reach computation.
- **Gold** `convergence.daily_reach_snapshot`: grain `(day, campaign_id, segment,
  delivery_type)` with `exact_reach BIGINT`, `impressions BIGINT`,
  `hll_sketch VARBINARY` (Presto HLL of `individual_id`). Snapshots serve fast dashboard
  reads; sketches serve mergeable approximate reach.

### 4.2 Compute

- **EMR Serverless** PySpark application. Jobs:
  - `bronze_ingest.py`: Landing JSON → Bronze Iceberg (schema enforcement, typing).
  - `silver_conform.py`: Bronze → Silver (union digital+linear, dedup, identity join).
- **Athena** (Presto engine v3) for Gold + all reach queries + compaction:
  - `gold_snapshot.sql`: `INSERT INTO daily_reach_snapshot SELECT day, campaign, segment,
    delivery_type, count(distinct individual_id) exact_reach, count(*) impressions,
    cast(approx_set(individual_id) as varbinary) hll_sketch FROM silver ... GROUP BY ...`.
  - Compaction: `OPTIMIZE <t> REWRITE DATA USING BIN_PACK` + `VACUUM <t>` per table.

### 4.3 Semantic / metrics layer (`semantic/`)

A thin Python module + parameterized Athena SQL. The **callable interface** consumed by
both the dashboard and the agent tools:

- `get_daily_reach(campaign, segment=None, day)` → exact reach from Gold snapshot.
- `get_cumulative_reach(campaign, segment=None, start, end)` →
  `cardinality(merge(cast(hll_sketch as HLL)))` over the window. No Silver rescan.
- `merge_segment_reach(campaign, segments[], start, end)` → merge sketches across the
  listed segments → unique-individual (deduped) reach; also returns per-segment reach and
  the incremental/overlap breakdown.
- `list_campaigns()` / `list_segments()` → dimension discovery for the agent & UI.

Each function returns a small typed dict (value + the SQL executed + latency), so the
dashboard and agent can show provenance.

### 4.4 Orchestration (MWAA)

- **`convergence_ingest`** (hourly or on-demand): trigger EMR Serverless `bronze_ingest` →
  `silver_conform` (sensors on job completion) → Athena `gold_snapshot`. Idempotent per
  ingest window.
- **`daily_compaction`** (daily): iterate the table list (bronze, silver, gold), run Athena
  `OPTIMIZE`/`VACUUM`, log file counts before/after. This is the "daily compaction job that
  looks at various tables."

MWAA is provisioned **first**; both DAGs use `EmrServerlessStartJobOperator` /
`AthenaOperator` (amazon provider). IAM: MWAA execution role can start EMR jobs, run
Athena, read/write S3 + Glue.

### 4.5 Agentic layer (Bedrock AgentCore)

- The four semantic functions are packaged as a **Lambda** (`agent/reach_tools_lambda.py`).
- **AgentCore Gateway** exposes the Lambda as agent-ready tools (MCP-compatible) — this is
  the reusable "interface for agentic systems."
- A **managed-harness Bedrock agent**: model = Claude on Bedrock, system prompt encoding
  the Reach definition and how to pick daily vs cumulative vs segment-merge tools.
- The dashboard's chat panel invokes this agent; responses stream back with the tool calls
  it made (so viewers see the reasoning → tool → reach number path).

### 4.6 Public access

- **App Runner** service runs the FastAPI app (container from ECR). Public HTTPS URL,
  no AWS login for viewers. Server-side it uses its instance IAM role to call Athena and
  the Bedrock agent; browsers never touch AWS credentials.
- **Claude Artifact**: a self-contained HTML one-pager (architecture diagram, Reach concept
  explainer, and a snapshot of sample results) for stakeholders who just want the story.

## 5. Data generator (`data_generator/`)

Python script produces realistic convergence events into S3 Landing:
- Configurable # campaigns, segments, days, and events/day (default: enough for a
  compelling multi-day cumulative curve, small enough to process in minutes).
- Deliberately overlapping audiences across segments and days so cumulative reach `<` sum
  of daily reach, and cross-segment merge shows real dedup (the "wow").
- Mix of `digital` and `linear` events with plausible network/geo/device distributions.

## 6. Repo structure

```
convergence_sample/
  README.md                 # setup, deploy, teardown, demo script
  terraform/                # S3, Glue DB, EMR Serverless, MWAA, Athena WG, Bedrock, App Runner, IAM, ECR
  spark/                    # bronze_ingest.py, silver_conform.py, shared iceberg utils
  sql/                      # gold_snapshot.sql, compaction.sql, ddl/
  dags/                     # convergence_ingest.py, daily_compaction.py
  semantic/                 # reach.py (metric functions) + athena client
  agent/                    # reach_tools_lambda.py, gateway + agent config
  dashboard/               # FastAPI app, templates, static, Dockerfile
  data_generator/           # generate_events.py
  scripts/                  # deploy.sh, seed.sh, run_pipeline.sh, teardown.sh
  docs/superpowers/specs/   # this spec
```

## 7. Build order (MWAA-first, thin-slice-validated)

1. **Foundations**: Terraform for S3, Glue DB, Athena WG, IAM, ECR; DDL for Iceberg tables.
2. **MWAA**: provision environment + execution role (longest lead time — start early).
3. **Data + ETL slice**: generator → Landing; EMR Serverless bronze/silver jobs; Athena
   gold snapshot + sketches. Validate daily + cumulative + cross-segment reach numbers
   locally against a brute-force `count(distinct)` cross-check.
4. **Orchestration**: `convergence_ingest` + `daily_compaction` DAGs on MWAA, end-to-end.
5. **Semantic layer**: reach functions over Athena, with provenance.
6. **Agentic**: Lambda tools → AgentCore Gateway → Bedrock managed-harness agent.
7. **Public app**: FastAPI dashboard + agent chat → App Runner public URL.
8. **Artifact**: shareable Claude Artifact summary.

## 8. Testing & verification

- **Reach correctness**: for each grain, cross-check HLL approximate reach against exact
  `count(distinct)` on the same window; assert relative error within HLL tolerance (~±2%).
- **Cumulative < sum-of-daily** and **segment-merge < sum-of-segments** (proves real dedup).
- **Idempotency**: re-running `convergence_ingest` for a window does not double-count.
- **Compaction**: assert Iceberg data-file count drops after `daily_compaction`.
- **Agent**: golden Q&A set (e.g. "cumulative reach of campaign X across sports+news last
  week") returns the semantic-layer number ±HLL tolerance.
- **Public URL**: fetch the App Runner URL unauthenticated; charts + chat render.

## 9. Risks & mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| MWAA provision time / IAM+VPC friction | Blocks orchestration (chosen as backbone) | Provision first; keep ETL runnable via `scripts/run_pipeline.sh` as fallback path |
| Bedrock/AgentCore availability in region | Agent layer blocked | Confirm region support first; managed harness keeps agent code minimal |
| HLL cross-engine (already resolved) | Wrong reach story | All HLL in Athena/Presto (§2.1) |
| Cost (MWAA always-on, EMR, App Runner) | Ongoing spend | `teardown.sh`; document hourly cost; small MWAA class |
| Region parity (EMR Serverless + MWAA + Bedrock + App Runner) | Deploy fails | Pick one region supporting all four before `terraform apply` |

## 10. Out of scope (YAGNI for tomorrow)

- The full Data Access & Governance layer (Lake Formation fine-grained perms, SageMaker
  Catalog, Horizon/Snowflake federation) — represented conceptually, not built.
- Snowflake ingestion path and Quick Suite BI — the App Runner dashboard stands in.
- Real identity graph — identity resolution is simulated deterministically in the generator.
- Streaming ingestion (Kafka/Kinesis) — batch/micro-batch only for the demo.
