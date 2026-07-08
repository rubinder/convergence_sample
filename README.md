# Convergence Reach Lakehouse

A functioning, cloud-native **reach-measurement lakehouse** for *convergence*
advertising — the unification of **digital** (ad server) and **linear** (national
TV) ad delivery into one data product — running on **real AWS** in `us-east-1`.

It ingests unified ad-impression events, builds a bronze → silver → gold Apache
Iceberg lakehouse, computes the advertising metric **Reach** at exact and
HyperLogLog-approximate grains, exposes a semantic layer, puts a **Claude-on-Bedrock
agent** on top, and serves everything from a **public dashboard** that anyone can
open — no AWS login required.

## What "Reach" means here

**Reach** = the count of **unique individuals** exposed to an ad within a time
window (the core WBD ad-sales metric).

- **Daily reach** — exact `count(distinct individual_id)`, read from Gold snapshots (fast).
- **Cumulative reach** — HyperLogLog sketches **merged across days** → deduped unique
  individuals, *without rescanning raw impressions*.
- **Cross-segment reach** — HLL sketches **merged across audience segments** → unique
  individuals deduped across segments (incremental / overlap reach).

Validated on live data: daily ≈ 298/day, HLL cumulative = 292 (< 1,490 sum-of-daily)
→ cross-day dedup proven; sports+news merged = 563 (< 292+288) → cross-segment dedup proven.

## Live demo (no AWS access needed)

- **Public dashboard:** https://miatibcmck.us-east-1.awsapprunner.com
  - Reach bar chart (per-day exact + HLL cumulative, with the overlap/dedup called out)
  - "Ask the reach agent" — natural-language reach Q&A via Claude on Bedrock
- **Repo:** https://github.com/rubinder/convergence_sample

### Read-only AWS console access for reviewers (auto-expiring)

A time-boxed, **read-only** IAM user lets a reviewer inspect the real
infrastructure (S3, Glue, Athena, EMR Serverless, Lambda, App Runner, MWAA)
without any write ability. Access lapses automatically after 5 days.

- Sign-in URL: `https://194611079924.signin.aws.amazon.com/console`
- Username: `convergence-viewer`
- Password: `terraform output -raw viewer_password` (share out-of-band)
- Expires: see `terraform output viewer_access_expires_utc`

## Architecture

```
Data generator ─▶ S3 Landing (unified digital+linear ad-exposure JSON)
                     │  (MWAA: convergence_ingest DAG)
                     ▼
              EMR Serverless (PySpark)
                 ├─ Bronze (Iceberg): typed raw events
                 └─ Silver (Iceberg): conformed unified impressions (deduped, identity-resolved)
                     │
                     ▼
              Athena / Presto
                 └─ Gold (Iceberg): daily_reach_snapshot
                       (day, campaign, segment, delivery_type) →
                        exact_reach, impressions, hll_sketch (binary)
                     │
        ┌────────────┼─────────────────────────────┐
        ▼            ▼                              ▼
  Semantic layer   Athena workgroup          MWAA: daily_compaction DAG
  (reach fns)      (ad-hoc SQL)              OPTIMIZE + VACUUM across tables
        │
        ├──▶ Claude-on-Bedrock agent (Converse tool-use; reach fns are its tools)
        │
        ▼
  App Runner (public FastAPI dashboard: reach charts + agent chat)
```

Key engineering decision: **HyperLogLog sketches are built and merged in Athena/Presto**
(`approx_set` → `varbinary`, `cardinality(merge(...))`), not in Spark — because
`spark-alchemy` HLL is not interoperable with Presto HLL, and the dashboard/agent must
merge sketches *live at query time* in the query engine. Spark does the heavy
bronze→silver ETL; Athena produces Gold + sketches and answers all reach queries.

## Repo layout

```
data_generator/   synthetic unified digital+linear event generator
spark/            EMR Serverless PySpark jobs (bronze, silver)
sql/              gold snapshot + compaction SQL, table DDL
semantic/         reach metric functions + Athena client + input validation
agent/            Claude-on-Bedrock reach agent + AgentCore Gateway deploy notes
dashboard/        FastAPI public dashboard (charts + agent chat) + Dockerfile
dags/             MWAA DAGs: convergence_ingest, daily_compaction
terraform/        all AWS infra (S3, Glue, Athena, EMR, Lambda, MWAA, App Runner, viewer)
scripts/          deploy/seed/run/teardown helpers
docs/             design spec, implementation plan, shareable artifact
tests/            27 unit tests (generator, reach SQL/HLL, agent tools, dashboard, SQLi)
```

## Prerequisites

- AWS credentials for `us-east-1` (this repo used the `aws login` token bridge in
  `scripts/env.sh`), Terraform ≥ 1.6, Docker, Python 3.11, `jq`.
- Bedrock model access: **Claude Sonnet 4.5** is used by the agent (access-granted on
  the demo account). To use Opus 4.8, enable its model access in the Bedrock console
  and set `BEDROCK_MODEL=us.anthropic.claude-opus-4-8`.

## Run it yourself

```bash
python3.11 -m venv .venv && ./.venv/bin/pip install -r requirements-dev.txt
./.venv/bin/pytest -q                    # 27 unit tests (no AWS needed)

bash scripts/deploy.sh                    # provision base infra + tables + lambda
bash scripts/upload_spark.sh              # push PySpark jobs to S3
bash scripts/seed.sh                      # generate + upload events to the landing zone
bash scripts/run_pipeline.sh              # EMR bronze->silver, then Athena gold+HLL
bash scripts/push_image.sh                # build + push the dashboard image (App Runner auto-deploys)
```

Then open `terraform output -raw service_url`.

## Orchestration (MWAA)

`terraform apply` with `deploy_mwaa = true` (see `terraform/terraform.tfvars`) provisions
a managed Airflow environment in a dedicated VPC and loads two DAGs:

- **`convergence_ingest`** — EMR bronze → silver → Athena gold, on a schedule.
- **`daily_compaction`** — `OPTIMIZE ... REWRITE DATA` + `VACUUM` across bronze/silver/gold
  ("the daily compaction job that looks at various tables").

Open the Airflow UI with `terraform output -raw mwaa_url`, unpause and trigger
`convergence_ingest`.

## Cost & teardown

MWAA (`mw1.small`) bills ~$0.49/hr always-on plus a NAT gateway (~$0.045/hr); EMR
Serverless is pay-per-use with 5-min auto-stop; App Runner is 0.25 vCPU. When done:

```bash
bash scripts/teardown.sh    # empties S3 and terraform destroy (dashboard + MWAA)
```

## Agentic interface (Bedrock AgentCore)

The reach functions are exposed as agent tools two ways:
1. **In-process Converse tool-use loop** (`agent/bedrock_agent.py`) — what the live
   dashboard chat uses today (Claude on Bedrock, reach fns as tools).
2. **AgentCore Gateway + Lambda** (`agent/reach_tools_lambda.py`, `agent/deploy_agent.py`)
   — the production hosting path: the reach Lambda registered as Gateway tools for a
   managed-harness Bedrock agent. `deploy_agent.py` prints the exact `agentcore` CLI steps.

Either way, the reach tools ARE the reusable "interface for agentic systems."
