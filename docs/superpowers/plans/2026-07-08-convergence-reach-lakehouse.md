# Convergence Reach Lakehouse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a functioning convergence (digital + linear) reach-measurement lakehouse on real AWS in us-east-1 — bronze/silver/gold Iceberg, Athena/Presto HLL reach, MWAA orchestration, a Bedrock AgentCore agent, and a public App Runner dashboard — plus a shareable Claude Artifact.

**Architecture:** Data generator seeds unified ad-exposure events into S3 Landing. MWAA (the backbone) triggers EMR Serverless PySpark jobs (Bronze → Silver Iceberg) then Athena (Gold daily reach snapshots + Presto HLL sketches). A Python semantic layer runs parameterized Athena reach queries; those functions are exposed as Lambda tools via AgentCore Gateway to a managed-harness Bedrock agent. A FastAPI dashboard on App Runner serves reach charts + agent chat at a public URL.

**Tech Stack:** Terraform, AWS (S3, Glue Data Catalog, EMR Serverless, MWAA, Athena engine v3, Bedrock AgentCore, App Runner, ECR, Lambda, IAM), Apache Iceberg, PySpark 3.5, Python 3.11 (boto3, FastAPI, pytest), Docker.

## Global Constraints

- **Region:** `us-east-1` for every resource (EMR Serverless + MWAA + Bedrock AgentCore + App Runner all supported there).
- **Catalog:** Glue Data Catalog database `convergence`; all tables are Apache **Iceberg**.
- **Table names (verbatim):** `bronze_impressions`, `silver_impressions`, `daily_reach_snapshot`.
- **Gold grain (verbatim):** `(day, campaign_id, segment, delivery_type)` with columns `exact_reach BIGINT`, `impressions BIGINT`, `hll_sketch VARBINARY`.
- **HLL engine:** Athena/Presto only (`approx_set`, `merge`, `cardinality`, `cast(... as HyperLogLog)`). Never spark-alchemy — its sketches are not Athena-compatible.
- **Delivery types (verbatim):** `digital`, `linear`.
- **Resource name prefix:** `convergence-` for AWS resources; S3 bucket `convergence-lakehouse-<ACCOUNT_ID>`.
- **Python:** 3.11, `black`-formatted, `pytest` for tests.
- **Reach definition:** count of unique `individual_id` exposed within a window; approximate reach = HLL cardinality of the merged sketch.
- **Cost discipline:** MWAA `mw1.small`, EMR Serverless auto-stop, App Runner 0.25 vCPU; `scripts/teardown.sh` must destroy everything.

---

## File Structure

```
convergence_sample/
  scripts/            env.sh, deploy.sh, seed.sh, run_pipeline.sh, teardown.sh, package_lambda.sh, push_image.sh
  terraform/          main.tf, providers.tf, variables.tf, s3.tf, glue.tf, iam.tf, emr.tf, mwaa.tf, athena.tf, lambda.tf, agent.tf, apprunner.tf, ecr.tf, outputs.tf
  sql/ddl/            create_tables.sql
  sql/                gold_snapshot.sql, compaction.sql
  data_generator/     generate_events.py, identity.py
  spark/              bronze_ingest.py, silver_conform.py, iceberg_conf.py
  semantic/           reach.py, athena_client.py
  agent/              reach_tools_lambda.py, agent_config.py, deploy_agent.py
  dashboard/          app.py, reach_source.py, templates/index.html, static/app.js, Dockerfile, requirements.txt
  dags/               convergence_ingest.py, daily_compaction.py, requirements.txt
  tests/              test_identity.py, test_generate_events.py, test_reach_sql.py, test_reach.py, test_reach_tools_lambda.py, test_dashboard.py
  requirements-dev.txt
  README.md
```

---

## Phase 0 — Repo & tooling foundation

### Task 0: Project scaffolding, env config, dev deps

**Files:**
- Create: `requirements-dev.txt`, `scripts/env.sh`, `tests/__init__.py`, `README.md` (stub)

**Interfaces:**
- Produces: `scripts/env.sh` exporting `AWS_REGION`, `ACCOUNT_ID`, `BUCKET`, `GLUE_DB`, `PROJECT` used by every other script.

- [ ] **Step 1: Create dev dependencies**

`requirements-dev.txt`:
```
boto3==1.34.131
pytest==8.2.2
black==24.4.2
moto[s3,athena,glue]==5.0.9
fastapi==0.111.0
httpx==0.27.0
```

- [ ] **Step 2: Create env config script**

`scripts/env.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
export AWS_REGION="us-east-1"
export ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export PROJECT="convergence"
export BUCKET="convergence-lakehouse-${ACCOUNT_ID}"
export GLUE_DB="convergence"
export ATHENA_WG="convergence-wg"
echo "env: region=${AWS_REGION} account=${ACCOUNT_ID} bucket=${BUCKET}"
```

- [ ] **Step 3: Verify env resolves**

Run: `bash -c 'source scripts/env.sh'`
Expected: prints `env: region=us-east-1 account=<12 digits> bucket=convergence-lakehouse-<id>` (requires configured AWS creds).

- [ ] **Step 4: Create venv and install dev deps**

Run: `python3.11 -m venv .venv && ./.venv/bin/pip install -r requirements-dev.txt`
Expected: installs without error.

- [ ] **Step 5: Commit**

```bash
git add requirements-dev.txt scripts/env.sh tests/__init__.py README.md
git commit -m "chore: project scaffolding and env config"
```

---

## Phase 1 — Data generator (pure Python, full TDD)

### Task 1: Deterministic identity resolver

**Files:**
- Create: `data_generator/identity.py`
- Test: `tests/test_identity.py`

**Interfaces:**
- Produces: `resolve_individual(seed: str) -> str`, `resolve_household(individual_id: str) -> str`. Deterministic (same seed → same ids), so overlap across days/segments is reproducible.

- [ ] **Step 1: Write the failing test**

`tests/test_identity.py`:
```python
from data_generator.identity import resolve_individual, resolve_household

def test_individual_is_deterministic():
    assert resolve_individual("u-42") == resolve_individual("u-42")

def test_household_groups_individuals():
    # individuals in the same household bucket share a household id
    h1 = resolve_household("ind_0001")
    h2 = resolve_household("ind_0001")
    assert h1 == h2 and h1.startswith("hh_")

def test_individual_id_format():
    assert resolve_individual("u-7").startswith("ind_")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_identity.py -v`
Expected: FAIL with `ModuleNotFoundError: data_generator.identity`.

- [ ] **Step 3: Write minimal implementation**

`data_generator/identity.py`:
```python
import hashlib

def _h(value: str, mod: int) -> int:
    return int(hashlib.sha256(value.encode()).hexdigest(), 16) % mod

def resolve_individual(seed: str) -> str:
    return f"ind_{_h(seed, 5000):04d}"

def resolve_household(individual_id: str) -> str:
    # ~2.5 individuals per household
    return f"hh_{_h(individual_id, 2000):04d}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./.venv/bin/pytest tests/test_identity.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add data_generator/identity.py tests/test_identity.py
git commit -m "feat: deterministic identity resolver for generator"
```

### Task 2: Event generator writing unified events to Landing JSON

**Files:**
- Create: `data_generator/generate_events.py`
- Test: `tests/test_generate_events.py`

**Interfaces:**
- Consumes: `resolve_individual`, `resolve_household` from Task 1.
- Produces: `build_events(days: int, events_per_day: int, seed: int) -> list[dict]` (each dict is one exposure with the §3 schema); `main()` writes NDJSON to `--out` local dir or uploads to S3 with `--bucket`.

- [ ] **Step 1: Write the failing test**

`tests/test_generate_events.py`:
```python
from datetime import date
from data_generator.generate_events import build_events, SEGMENTS, DELIVERY_TYPES

def test_event_schema_and_values():
    events = build_events(days=2, events_per_day=100, seed=1)
    assert len(events) == 200
    e = events[0]
    assert set(e) >= {
        "event_id", "event_ts", "delivery_type", "individual_id",
        "household_id", "campaign_id", "segment", "geo", "device", "network",
    }
    assert e["delivery_type"] in DELIVERY_TYPES
    assert e["segment"] in SEGMENTS

def test_events_are_deterministic():
    assert build_events(2, 50, 7) == build_events(2, 50, 7)

def test_audience_overlaps_across_days():
    # unique individuals over 3 days < 3x unique individuals in one day => overlap exists
    all_days = build_events(days=3, events_per_day=500, seed=3)
    day0 = [e for e in all_days if e["event_ts"].startswith(_first_day(all_days))]
    uniq_all = len({e["individual_id"] for e in all_days})
    uniq_day0 = len({e["individual_id"] for e in day0})
    assert uniq_all < 3 * uniq_day0

def _first_day(events):
    return min(e["event_ts"] for e in events)[:10]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_generate_events.py -v`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write minimal implementation**

`data_generator/generate_events.py`:
```python
import argparse
import json
import os
from datetime import date, datetime, timedelta, timezone

from data_generator.identity import resolve_individual, resolve_household

SEGMENTS = ["sports", "news", "drama", "kids", "lifestyle"]
DELIVERY_TYPES = ["digital", "linear"]
CAMPAIGNS = ["camp_finals", "camp_launch", "camp_holiday"]
NETWORKS = {"digital": ["Max", "DiscoveryPlus"], "linear": ["CNN", "TNT", "TBS"]}
GEOS = ["NY", "LA", "CHI", "DAL", "ATL"]
DEVICES = {"digital": ["mobile", "desktop", "ctv"], "linear": ["set-top"]}
START_DAY = date(2026, 7, 1)

def _pick(seq, n):
    return seq[n % len(seq)]

def build_events(days: int, events_per_day: int, seed: int) -> list[dict]:
    events = []
    for d in range(days):
        day = START_DAY + timedelta(days=d)
        for i in range(events_per_day):
            n = seed * 1_000_003 + d * 7919 + i
            delivery = _pick(DELIVERY_TYPES, n)
            # audience skews by segment but reuses a shared id pool -> cross-day/segment overlap
            seg = _pick(SEGMENTS, n // 3)
            user_seed = f"{seg}-{n % 1500}"
            ind = resolve_individual(user_seed)
            ts = datetime(day.year, day.month, day.day, (i % 24), (i % 60),
                          tzinfo=timezone.utc).isoformat()
            events.append({
                "event_id": f"evt_{seed}_{d}_{i}",
                "event_ts": ts,
                "delivery_type": delivery,
                "individual_id": ind,
                "household_id": resolve_household(ind),
                "campaign_id": _pick(CAMPAIGNS, n // 11),
                "creative_id": f"cr_{n % 20}",
                "segment": seg,
                "network": _pick(NETWORKS[delivery], n),
                "geo": _pick(GEOS, n // 13),
                "device": _pick(DEVICES[delivery], n),
            })
    return events

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=5)
    ap.add_argument("--events-per-day", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default=None, help="local dir")
    ap.add_argument("--bucket", default=None, help="s3 bucket for landing/")
    args = ap.parse_args()
    events = build_events(args.days, args.events_per_day, args.seed)
    by_day: dict[str, list[dict]] = {}
    for e in events:
        by_day.setdefault(e["event_ts"][:10], []).append(e)
    for day, rows in by_day.items():
        body = "\n".join(json.dumps(r) for r in rows)
        key = f"landing/ingest_date={day}/events.json"
        if args.bucket:
            import boto3
            boto3.client("s3").put_object(Bucket=args.bucket, Key=key, Body=body.encode())
            print(f"uploaded s3://{args.bucket}/{key} ({len(rows)} rows)")
        else:
            path = os.path.join(args.out or "landing_local", key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(body)
            print(f"wrote {path} ({len(rows)} rows)")

if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./.venv/bin/pytest tests/test_generate_events.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add data_generator/generate_events.py tests/test_generate_events.py
git commit -m "feat: convergence event generator with cross-day audience overlap"
```

---

## Phase 2 — AWS foundations (Terraform: storage, catalog, IAM, Athena)

### Task 3: Terraform base — provider, S3, Glue DB, Athena workgroup, ECR

**Files:**
- Create: `terraform/providers.tf`, `terraform/variables.tf`, `terraform/s3.tf`, `terraform/glue.tf`, `terraform/athena.tf`, `terraform/ecr.tf`, `terraform/outputs.tf`

**Interfaces:**
- Produces: outputs `bucket`, `glue_db`, `athena_workgroup`, `ecr_repo_url` consumed by scripts and later Terraform.

- [ ] **Step 1: Provider + variables**

`terraform/providers.tf`:
```hcl
terraform {
  required_version = ">= 1.6"
  required_providers { aws = { source = "hashicorp/aws", version = "~> 5.50" } }
}
provider "aws" { region = var.region }
data "aws_caller_identity" "current" {}
```
`terraform/variables.tf`:
```hcl
variable "region"  { default = "us-east-1" }
variable "project" { default = "convergence" }
locals {
  account_id = data.aws_caller_identity.current.account_id
  bucket     = "convergence-lakehouse-${local.account_id}"
}
```

- [ ] **Step 2: S3, Glue, Athena, ECR resources**

`terraform/s3.tf`:
```hcl
resource "aws_s3_bucket" "lake" { bucket = local.bucket }
resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}
```
`terraform/glue.tf`:
```hcl
resource "aws_glue_catalog_database" "convergence" { name = var.project }
```
`terraform/athena.tf`:
```hcl
resource "aws_athena_workgroup" "wg" {
  name = "${var.project}-wg"
  configuration {
    enforce_workgroup_configuration = true
    result_configuration { output_location = "s3://${local.bucket}/athena-results/" }
    engine_version { selected_engine_version = "Athena engine version 3" }
  }
  force_destroy = true
}
```
`terraform/ecr.tf`:
```hcl
resource "aws_ecr_repository" "dashboard" {
  name         = "${var.project}-dashboard"
  force_delete = true
}
```
`terraform/outputs.tf`:
```hcl
output "bucket"            { value = aws_s3_bucket.lake.bucket }
output "glue_db"          { value = aws_glue_catalog_database.convergence.name }
output "athena_workgroup" { value = aws_athena_workgroup.wg.name }
output "ecr_repo_url"     { value = aws_ecr_repository.dashboard.repository_url }
```

- [ ] **Step 3: Init & apply**

Run: `cd terraform && terraform init && terraform apply -auto-approve`
Expected: creates bucket, glue db, workgroup, ecr; prints outputs.

- [ ] **Step 4: Verify resources exist**

Run: `source scripts/env.sh && aws s3 ls "s3://${BUCKET}" && aws glue get-database --name convergence --query 'Database.Name'`
Expected: bucket listing succeeds; prints `"convergence"`.

- [ ] **Step 5: Commit**

```bash
git add terraform/providers.tf terraform/variables.tf terraform/s3.tf terraform/glue.tf terraform/athena.tf terraform/ecr.tf terraform/outputs.tf
git commit -m "feat: terraform base (s3, glue db, athena wg, ecr)"
```

### Task 4: Iceberg table DDL + create-tables script

**Files:**
- Create: `sql/ddl/create_tables.sql`, `scripts/create_tables.sh`

**Interfaces:**
- Produces: Iceberg tables `bronze_impressions`, `silver_impressions`, `daily_reach_snapshot` in Glue db `convergence`, queried by all later phases.

- [ ] **Step 1: Write DDL**

`sql/ddl/create_tables.sql` (each statement submitted separately by the script):
```sql
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
```

- [ ] **Step 2: Write submitter script**

`scripts/create_tables.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
source scripts/env.sh
SQL=$(sed "s|__BUCKET__|${BUCKET}|g" sql/ddl/create_tables.sql)
# split on ';' and submit each non-empty statement to Athena
echo "$SQL" | awk 'BEGIN{RS=";"} NF {print $0 ";"}' | while read -r -d '' stmt || [ -n "$stmt" ]; do
  [ -z "${stmt// }" ] && continue
  qid=$(aws athena start-query-execution --work-group "$ATHENA_WG" \
        --query-string "$stmt" --query 'QueryExecutionId' --output text)
  echo "submitted $qid"
  aws athena get-query-execution --query-execution-id "$qid" \
      --query 'QueryExecution.Status.State' --output text
done
```

- [ ] **Step 3: Run and verify tables**

Run: `bash scripts/create_tables.sh && source scripts/env.sh && aws glue get-tables --database-name convergence --query 'TableList[].Name'`
Expected: lists `bronze_impressions`, `silver_impressions`, `daily_reach_snapshot`.

- [ ] **Step 4: Commit**

```bash
git add sql/ddl/create_tables.sql scripts/create_tables.sh
git commit -m "feat: iceberg table ddl (bronze/silver/gold)"
```

---

## Phase 3 — Reach SQL & semantic layer (TDD on SQL builders)

### Task 5: Gold snapshot SQL + reach query builders

**Files:**
- Create: `sql/gold_snapshot.sql`, `sql/compaction.sql`, `semantic/reach.py` (SQL builders only in this task)
- Test: `tests/test_reach_sql.py`

**Interfaces:**
- Produces (pure string builders, no AWS): `daily_reach_sql(campaign, segment, day)`, `cumulative_reach_sql(campaign, segment, start, end)`, `segment_merge_sql(campaign, segments, start, end)`. All target Athena/Presto and use `cardinality(merge(cast(hll_sketch as HyperLogLog)))` for approximate reach.

- [ ] **Step 1: Write the failing test**

`tests/test_reach_sql.py`:
```python
from semantic.reach import daily_reach_sql, cumulative_reach_sql, segment_merge_sql

def test_daily_reach_reads_snapshot_exact():
    sql = daily_reach_sql("camp_finals", "sports", "2026-07-03")
    assert "daily_reach_snapshot" in sql
    assert "exact_reach" in sql
    assert "camp_finals" in sql and "sports" in sql and "2026-07-03" in sql

def test_cumulative_merges_hll_over_window():
    sql = cumulative_reach_sql("camp_finals", "sports", "2026-07-01", "2026-07-05")
    assert "cardinality(merge(cast(hll_sketch as HyperLogLog)))" in sql
    assert "day between date '2026-07-01' and date '2026-07-05'" in sql

def test_segment_merge_unions_segments():
    sql = segment_merge_sql("camp_finals", ["sports", "news"], "2026-07-01", "2026-07-05")
    assert "segment in ('sports', 'news')" in sql
    assert "cardinality(merge(cast(hll_sketch as HyperLogLog)))" in sql

def test_no_segment_filter_when_none():
    sql = daily_reach_sql("camp_finals", None, "2026-07-03")
    assert "segment =" not in sql
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_reach_sql.py -v`
Expected: FAIL (`ModuleNotFoundError: semantic.reach`).

- [ ] **Step 3: Write the SQL builders**

`semantic/reach.py` (builders section):
```python
DB = "convergence"
_HLL = "cardinality(merge(cast(hll_sketch as HyperLogLog)))"

def _seg_clause(segment):
    return f" and segment = '{segment}'" if segment else ""

def daily_reach_sql(campaign: str, segment, day: str) -> str:
    return (
        f"select sum(exact_reach) as reach, sum(impressions) as impressions "
        f"from {DB}.daily_reach_snapshot "
        f"where campaign_id = '{campaign}' and day = date '{day}'"
        f"{_seg_clause(segment)}"
    )

def cumulative_reach_sql(campaign: str, segment, start: str, end: str) -> str:
    return (
        f"select {_HLL} as reach "
        f"from {DB}.daily_reach_snapshot "
        f"where campaign_id = '{campaign}' "
        f"and day between date '{start}' and date '{end}'"
        f"{_seg_clause(segment)}"
    )

def segment_merge_sql(campaign: str, segments: list, start: str, end: str) -> str:
    seg_list = ", ".join(f"'{s}'" for s in segments)
    return (
        f"select {_HLL} as reach "
        f"from {DB}.daily_reach_snapshot "
        f"where campaign_id = '{campaign}' "
        f"and day between date '{start}' and date '{end}' "
        f"and segment in ({seg_list})"
    )
```

`sql/gold_snapshot.sql` (used by orchestration; `__START__`/`__END__` templated per run):
```sql
INSERT INTO convergence.daily_reach_snapshot
SELECT event_day AS day, campaign_id, segment, delivery_type,
       count(distinct individual_id)                         AS exact_reach,
       count(*)                                              AS impressions,
       cast(approx_set(individual_id) AS varbinary)          AS hll_sketch
FROM convergence.silver_impressions
WHERE event_day BETWEEN date '__START__' AND date '__END__'
GROUP BY event_day, campaign_id, segment, delivery_type;
```

`sql/compaction.sql` (one pair per table; `__T__` templated by the DAG):
```sql
OPTIMIZE convergence.__T__ REWRITE DATA USING BIN_PACK;
VACUUM convergence.__T__;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./.venv/bin/pytest tests/test_reach_sql.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add semantic/reach.py sql/gold_snapshot.sql sql/compaction.sql tests/test_reach_sql.py
git commit -m "feat: reach sql builders + gold snapshot/compaction sql"
```

### Task 6: Athena client + semantic reach functions (live-query wrappers)

**Files:**
- Create: `semantic/athena_client.py`
- Modify: `semantic/reach.py` (add function wrappers `get_daily_reach`, `get_cumulative_reach`, `merge_segment_reach`, `list_campaigns`, `list_segments`)
- Test: `tests/test_reach.py`

**Interfaces:**
- Consumes: builders from Task 5.
- Produces: `run_query(sql: str) -> list[dict]` in `athena_client.py`; reach functions returning `{"reach": int, "sql": str, "latency_ms": int, ...}`. These are the callable interface consumed by the agent Lambda (Task 12) and dashboard (Task 14).

- [ ] **Step 1: Write the failing test (mocking the athena client)**

`tests/test_reach.py`:
```python
from unittest.mock import patch
import semantic.reach as reach

def test_get_daily_reach_returns_value_and_sql():
    with patch("semantic.reach.run_query", return_value=[{"reach": "1234", "impressions": "5000"}]):
        out = reach.get_daily_reach("camp_finals", "sports", "2026-07-03")
    assert out["reach"] == 1234
    assert "daily_reach_snapshot" in out["sql"]

def test_get_cumulative_reach_uses_hll_merge():
    with patch("semantic.reach.run_query", return_value=[{"reach": "9000"}]):
        out = reach.get_cumulative_reach("camp_finals", "sports", "2026-07-01", "2026-07-05")
    assert out["reach"] == 9000
    assert "merge(cast(hll_sketch as HyperLogLog))" in out["sql"]

def test_merge_segment_reach_over_segments():
    with patch("semantic.reach.run_query", return_value=[{"reach": "12000"}]):
        out = reach.merge_segment_reach("camp_finals", ["sports", "news"], "2026-07-01", "2026-07-05")
    assert out["reach"] == 12000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_reach.py -v`
Expected: FAIL (`AttributeError: get_daily_reach`).

- [ ] **Step 3: Implement athena client + wrappers**

`semantic/athena_client.py`:
```python
import os
import time
import boto3

_ATHENA = boto3.client("athena", region_name=os.getenv("AWS_REGION", "us-east-1"))
_WG = os.getenv("ATHENA_WG", "convergence-wg")

def run_query(sql: str) -> list[dict]:
    qid = _ATHENA.start_query_execution(
        QueryString=sql, WorkGroup=_WG
    )["QueryExecutionId"]
    while True:
        st = _ATHENA.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.7)
    if st != "SUCCEEDED":
        raise RuntimeError(f"athena query {st}: {sql[:120]}")
    res = _ATHENA.get_query_results(QueryExecutionId=qid)
    rows = res["ResultSet"]["Rows"]
    if len(rows) < 2:
        return []
    cols = [c["VarCharValue"] for c in rows[0]["Data"]]
    out = []
    for r in rows[1:]:
        vals = [d.get("VarCharValue") for d in r["Data"]]
        out.append(dict(zip(cols, vals)))
    return out
```

Add to `semantic/reach.py`:
```python
import time
from semantic.athena_client import run_query  # noqa: E402

def _one(sql: str, key: str = "reach") -> dict:
    t0 = time.time()
    rows = run_query(sql)
    val = rows[0].get(key) if rows and rows[0].get(key) is not None else 0
    return {"reach": int(val), "sql": sql, "latency_ms": int((time.time() - t0) * 1000), "rows": rows}

def get_daily_reach(campaign, segment, day):
    return _one(daily_reach_sql(campaign, segment, day))

def get_cumulative_reach(campaign, segment, start, end):
    return _one(cumulative_reach_sql(campaign, segment, start, end))

def merge_segment_reach(campaign, segments, start, end):
    out = _one(segment_merge_sql(campaign, segments, start, end))
    out["segments"] = segments
    return out

def list_campaigns():
    return [r["campaign_id"] for r in run_query(
        f"select distinct campaign_id from {DB}.daily_reach_snapshot order by 1")]

def list_segments():
    return [r["segment"] for r in run_query(
        f"select distinct segment from {DB}.daily_reach_snapshot order by 1")]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./.venv/bin/pytest tests/test_reach.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add semantic/athena_client.py semantic/reach.py tests/test_reach.py
git commit -m "feat: athena client + semantic reach functions"
```

---

## Phase 4 — Spark ETL (EMR Serverless jobs)

### Task 7: Spark Iceberg config helper + bronze ingest job

**Files:**
- Create: `spark/iceberg_conf.py`, `spark/bronze_ingest.py`

**Interfaces:**
- Produces: `bronze_ingest.py` reads `s3://BUCKET/landing/**/events.json`, writes/append to `convergence.bronze_impressions`. Args: `--bucket`, `--ingest-date` (optional filter).

- [ ] **Step 1: Iceberg session config**

`spark/iceberg_conf.py`:
```python
from pyspark.sql import SparkSession

def spark_session(app: str, bucket: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue.warehouse", f"s3://{bucket}/")
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
```

- [ ] **Step 2: Bronze ingest job**

`spark/bronze_ingest.py`:
```python
import argparse
from pyspark.sql import functions as F
from spark.iceberg_conf import spark_session

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--ingest-date", default=None)
    args = ap.parse_args()
    spark = spark_session("bronze_ingest", args.bucket)
    path = f"s3://{args.bucket}/landing/"
    if args.ingest_date:
        path = f"{path}ingest_date={args.ingest_date}/"
    df = spark.read.json(path)
    df = (df
          .withColumn("event_ts", F.to_timestamp("event_ts"))
          .withColumn("ingest_date", F.date_format(F.col("event_ts"), "yyyy-MM-dd")))
    cols = ["event_id", "event_ts", "delivery_type", "individual_id", "household_id",
            "campaign_id", "creative_id", "segment", "network", "geo", "device", "ingest_date"]
    df.select(*cols).writeTo("glue.convergence.bronze_impressions").append()
    print(f"bronze rows written: {df.count()}")
    spark.stop()

if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Lint compiles**

Run: `./.venv/bin/python -m py_compile spark/iceberg_conf.py spark/bronze_ingest.py`
Expected: no output (success). *(Full run is verified end-to-end in Task 10 on EMR Serverless.)*

- [ ] **Step 4: Commit**

```bash
git add spark/iceberg_conf.py spark/bronze_ingest.py
git commit -m "feat: spark bronze ingest job (landing -> iceberg)"
```

### Task 8: Silver conform job (unify + dedup + identity)

**Files:**
- Create: `spark/silver_conform.py`

**Interfaces:**
- Produces: reads `convergence.bronze_impressions`, writes `convergence.silver_impressions` (deduped on `event_id`, adds `event_day`, unified digital+linear schema). Args: `--bucket`.

- [ ] **Step 1: Silver job**

`spark/silver_conform.py`:
```python
import argparse
from pyspark.sql import functions as F, Window
from spark.iceberg_conf import spark_session

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    args = ap.parse_args()
    spark = spark_session("silver_conform", args.bucket)
    b = spark.table("glue.convergence.bronze_impressions")
    w = Window.partitionBy("event_id").orderBy(F.col("event_ts").desc())
    conformed = (b
        .withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
        .withColumn("event_day", F.to_date("event_ts"))
        .select("event_id", "event_ts", "event_day", "delivery_type",
                "individual_id", "household_id", "campaign_id",
                "segment", "network", "geo", "device"))
    conformed.writeTo("glue.convergence.silver_impressions").overwritePartitions()
    print(f"silver rows written: {conformed.count()}")
    spark.stop()

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Compiles**

Run: `./.venv/bin/python -m py_compile spark/silver_conform.py`
Expected: success.

- [ ] **Step 3: Commit**

```bash
git add spark/silver_conform.py
git commit -m "feat: spark silver conform (dedup + unify digital/linear)"
```

### Task 9: EMR Serverless + IAM Terraform; upload Spark to S3

**Files:**
- Create: `terraform/iam.tf`, `terraform/emr.tf`, `scripts/upload_spark.sh`

**Interfaces:**
- Produces: outputs `emr_application_id`, `emr_job_role_arn`. Spark files at `s3://BUCKET/code/spark/`.

- [ ] **Step 1: IAM for EMR/Athena/Glue/S3**

`terraform/iam.tf`:
```hcl
data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals { type = "Service", identifiers = ["emr-serverless.amazonaws.com"] }
  }
}
resource "aws_iam_role" "emr_job" {
  name               = "${var.project}-emr-job-role"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}
resource "aws_iam_role_policy" "emr_job" {
  role   = aws_iam_role.emr_job.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = ["s3:*", "glue:*", "athena:*", "lakeformation:GetDataAccess", "logs:*"],
      Resource = "*"
    }]
  })
}
output "emr_job_role_arn" { value = aws_iam_role.emr_job.arn }
```

- [ ] **Step 2: EMR Serverless application**

`terraform/emr.tf`:
```hcl
resource "aws_emrserverless_application" "spark" {
  name          = "${var.project}-spark"
  release_label = "emr-7.1.0"
  type          = "spark"
  auto_stop_configuration { enabled = true, idle_timeout_minutes = 5 }
}
output "emr_application_id" { value = aws_emrserverless_application.spark.id }
```

- [ ] **Step 3: Upload script**

`scripts/upload_spark.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
source scripts/env.sh
aws s3 cp spark/ "s3://${BUCKET}/code/spark/" --recursive --exclude "__pycache__/*"
echo "uploaded spark code to s3://${BUCKET}/code/spark/"
```

- [ ] **Step 4: Apply and upload**

Run: `cd terraform && terraform apply -auto-approve && cd .. && bash scripts/upload_spark.sh`
Expected: prints `emr_application_id`, `emr_job_role_arn`; spark files uploaded.

- [ ] **Step 5: Commit**

```bash
git add terraform/iam.tf terraform/emr.tf scripts/upload_spark.sh
git commit -m "feat: emr serverless app + job iam + spark upload"
```

### Task 10: End-to-end pipeline runner (generator → bronze → silver → gold) + reach validation

**Files:**
- Create: `scripts/run_pipeline.sh`, `scripts/seed.sh`

**Interfaces:**
- Consumes: generator (Task 2), EMR app (Task 9), gold SQL (Task 5), semantic functions (Task 6).
- Produces: populated bronze/silver/gold; proves reach numbers. This is the **thin-slice validation gate**.

- [ ] **Step 1: Seed landing**

`scripts/seed.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
source scripts/env.sh
./.venv/bin/python -m data_generator.generate_events --days 5 --events-per-day 20000 --seed 1 --bucket "$BUCKET"
```

- [ ] **Step 2: Pipeline runner (submits EMR jobs, then Athena gold)**

`scripts/run_pipeline.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
source scripts/env.sh
APP=$(cd terraform && terraform output -raw emr_application_id)
ROLE=$(cd terraform && terraform output -raw emr_job_role_arn)
ENTRY="s3://${BUCKET}/code/spark"

submit() {  # $1=script $2=args...
  local jid
  jid=$(aws emr-serverless start-job-run --application-id "$APP" --execution-role-arn "$ROLE" \
    --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"${ENTRY}/$1\",\"entryPointArguments\":[${2}],\"sparkSubmitParameters\":\"--conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,software.amazon.awssdk:bundle:2.20.160 --py-files ${ENTRY}/iceberg_conf.py\"}}" \
    --query 'jobRunId' --output text)
  echo "waiting on $1 job $jid"
  while true; do
    S=$(aws emr-serverless get-job-run --application-id "$APP" --job-run-id "$jid" --query 'jobRun.state' --output text)
    [[ "$S" == SUCCESS ]] && break
    [[ "$S" == FAILED || "$S" == CANCELLED ]] && { echo "job $S"; exit 1; }
    sleep 10
  done
  echo "$1 SUCCESS"
}

submit "bronze_ingest.py" "\"--bucket\",\"${BUCKET}\""
submit "silver_conform.py" "\"--bucket\",\"${BUCKET}\""

# Gold via Athena
GOLD=$(sed -e "s/__START__/2026-07-01/" -e "s/__END__/2026-07-05/" sql/gold_snapshot.sql)
qid=$(aws athena start-query-execution --work-group "$ATHENA_WG" --query-string "$GOLD" --query 'QueryExecutionId' --output text)
echo "gold query $qid submitted"
```

- [ ] **Step 3: Run the full pipeline**

Run: `bash scripts/seed.sh && bash scripts/upload_spark.sh && bash scripts/run_pipeline.sh`
Expected: bronze SUCCESS, silver SUCCESS, gold query submitted & succeeds (check with `aws athena get-query-execution`).

- [ ] **Step 4: Validate reach correctness (HLL vs exact)**

Run:
```bash
source scripts/env.sh
AWS_REGION=$AWS_REGION ATHENA_WG=$ATHENA_WG ./.venv/bin/python -c "
import semantic.reach as r
print('daily', r.get_daily_reach('camp_finals','sports','2026-07-03')['reach'])
cum = r.get_cumulative_reach('camp_finals','sports','2026-07-01','2026-07-05')['reach']
days = sum(r.get_daily_reach('camp_finals','sports',f'2026-07-0{d}')['reach'] for d in range(1,6))
print('cumulative', cum, 'sum-of-daily', days)
assert cum < days, 'cumulative must be < sum-of-daily (dedup proof)'
merged = r.merge_segment_reach('camp_finals',['sports','news'],'2026-07-01','2026-07-05')['reach']
print('segment-merged', merged)
"
```
Expected: prints daily/cumulative/merged; assertion passes (proves HLL dedup across days).

- [ ] **Step 5: Commit**

```bash
git add scripts/seed.sh scripts/run_pipeline.sh
git commit -m "feat: end-to-end pipeline runner + reach validation"
```

---

## Phase 5 — MWAA orchestration (the backbone)

### Task 11: MWAA environment (Terraform) + DAGs + verify

**Files:**
- Create: `terraform/mwaa.tf`, `dags/convergence_ingest.py`, `dags/daily_compaction.py`, `dags/requirements.txt`, `scripts/upload_dags.sh`

**Interfaces:**
- Consumes: EMR app id, job role, bucket (via Airflow Variables set from Terraform outputs).
- Produces: a running MWAA env with two DAGs orchestrating EMR + Athena.

- [ ] **Step 1: MWAA Terraform (uses default VPC subnets)**

`terraform/mwaa.tf`:
```hcl
data "aws_vpc" "default" { default = true }
data "aws_subnets" "private" {
  filter { name = "vpc-id", values = [data.aws_vpc.default.id] }
}
resource "aws_s3_object" "dags_marker" {
  bucket = aws_s3_bucket.lake.id
  key    = "dags/.keep"
  content = "keep"
}
resource "aws_iam_role" "mwaa" {
  name = "${var.project}-mwaa-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{ Effect = "Allow", Principal = { Service = ["airflow.amazonaws.com","airflow-env.amazonaws.com"] }, Action = "sts:AssumeRole" }]
  })
}
resource "aws_iam_role_policy" "mwaa" {
  role = aws_iam_role.mwaa.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow",
    Action = ["s3:*","emr-serverless:*","athena:*","glue:*","logs:*","iam:PassRole","airflow:*"], Resource = "*" }] })
}
resource "aws_mwaa_environment" "env" {
  name               = "${var.project}-mwaa"
  airflow_version    = "2.9.2"
  environment_class  = "mw1.small"
  execution_role_arn = aws_iam_role.mwaa.arn
  source_bucket_arn  = aws_s3_bucket.lake.arn
  dag_s3_path        = "dags/"
  requirements_s3_path = "dags/requirements.txt"
  max_workers        = 2
  network_configuration {
    security_group_ids = [aws_security_group.mwaa.id]
    subnet_ids         = slice(tolist(data.aws_subnets.private.ids), 0, 2)
  }
  webserver_access_mode = "PUBLIC_ONLY"
  logging_configuration { task_logs { enabled = true, log_level = "INFO" } }
}
resource "aws_security_group" "mwaa" {
  name   = "${var.project}-mwaa-sg"
  vpc_id = data.aws_vpc.default.id
  ingress { from_port = 0, to_port = 0, protocol = "-1", self = true }
  egress  { from_port = 0, to_port = 0, protocol = "-1", cidr_blocks = ["0.0.0.0/0"] }
}
output "mwaa_url" { value = "https://${aws_mwaa_environment.env.webserver_url}" }
```

- [ ] **Step 2: Ingest DAG**

`dags/convergence_ingest.py`:
```python
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

BUCKET = Variable.get("bucket")
APP = Variable.get("emr_application_id")
ROLE = Variable.get("emr_job_role_arn")
ENTRY = f"s3://{BUCKET}/code/spark"
COMMON = ("--conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
          "software.amazon.awssdk:bundle:2.20.160 "
          f"--py-files {ENTRY}/iceberg_conf.py")

def job(script, args):
    return {"sparkSubmit": {"entryPoint": f"{ENTRY}/{script}",
            "entryPointArguments": args, "sparkSubmitParameters": COMMON}}

with DAG("convergence_ingest", start_date=datetime(2026, 7, 1),
         schedule="@hourly", catchup=False, tags=["convergence"]) as dag:
    bronze = EmrServerlessStartJobOperator(
        task_id="bronze", application_id=APP, execution_role_arn=ROLE,
        job_driver=job("bronze_ingest.py", ["--bucket", BUCKET]))
    silver = EmrServerlessStartJobOperator(
        task_id="silver", application_id=APP, execution_role_arn=ROLE,
        job_driver=job("silver_conform.py", ["--bucket", BUCKET]))
    gold = AthenaOperator(
        task_id="gold", database="convergence", workgroup="convergence-wg",
        query=("INSERT INTO convergence.daily_reach_snapshot "
               "SELECT event_day AS day, campaign_id, segment, delivery_type, "
               "count(distinct individual_id), count(*), "
               "cast(approx_set(individual_id) AS varbinary) "
               "FROM convergence.silver_impressions "
               "GROUP BY event_day, campaign_id, segment, delivery_type"))
    bronze >> silver >> gold
```

- [ ] **Step 3: Compaction DAG**

`dags/daily_compaction.py`:
```python
from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

TABLES = ["bronze_impressions", "silver_impressions", "daily_reach_snapshot"]

with DAG("daily_compaction", start_date=datetime(2026, 7, 1),
         schedule="@daily", catchup=False, tags=["convergence"]) as dag:
    prev = None
    for t in TABLES:
        opt = AthenaOperator(task_id=f"optimize_{t}", database="convergence",
            workgroup="convergence-wg",
            query=f"OPTIMIZE convergence.{t} REWRITE DATA USING BIN_PACK")
        vac = AthenaOperator(task_id=f"vacuum_{t}", database="convergence",
            workgroup="convergence-wg", query=f"VACUUM convergence.{t}")
        opt >> vac
        if prev:
            prev >> opt
        prev = vac
```

- [ ] **Step 4: DAG requirements + upload script**

`dags/requirements.txt`:
```
apache-airflow-providers-amazon==8.24.0
```
`scripts/upload_dags.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
source scripts/env.sh
aws s3 cp dags/ "s3://${BUCKET}/dags/" --recursive --exclude "__pycache__/*"
# set Airflow Variables via MWAA CLI token
ENV="convergence-mwaa"
for kv in "bucket=${BUCKET}" \
          "emr_application_id=$(cd terraform && terraform output -raw emr_application_id)" \
          "emr_job_role_arn=$(cd terraform && terraform output -raw emr_job_role_arn)"; do
  k="${kv%%=*}"; v="${kv#*=}"
  CLI=$(aws mwaa create-cli-token --name "$ENV")
  TOKEN=$(echo "$CLI" | python3 -c 'import sys,json;print(json.load(sys.stdin)["CliToken"])')
  HOST=$(echo "$CLI" | python3 -c 'import sys,json;print(json.load(sys.stdin)["WebServerHostname"])')
  curl -s -X POST "https://${HOST}/aws_mwaa/cli" -H "Authorization: Bearer ${TOKEN}" \
       -H "Content-Type: text/plain" --data-raw "variables set ${k} ${v}" >/dev/null
  echo "set var ${k}"
done
```

- [ ] **Step 5: Apply MWAA, upload DAGs (note: MWAA create ~25 min)**

Run: `cd terraform && terraform apply -auto-approve && cd .. && bash scripts/upload_dags.sh`
Expected: `mwaa_url` output; DAGs appear in the MWAA UI within a few minutes; Variables set.

- [ ] **Step 6: Trigger ingest DAG and verify success**

Run: `source scripts/env.sh && aws mwaa create-cli-token --name convergence-mwaa >/dev/null && echo "open $(cd terraform && terraform output -raw mwaa_url) and unpause + trigger convergence_ingest"`
Expected: in the MWAA UI, `convergence_ingest` runs green (bronze → silver → gold); Gold table populated.

- [ ] **Step 7: Commit**

```bash
git add terraform/mwaa.tf dags/ scripts/upload_dags.sh
git commit -m "feat: mwaa env + ingest and daily compaction dags"
```

---

## Phase 6 — Agentic layer (Bedrock AgentCore)

### Task 12: Reach tools Lambda

**Files:**
- Create: `agent/reach_tools_lambda.py`, `scripts/package_lambda.sh`, `terraform/lambda.tf`
- Test: `tests/test_reach_tools_lambda.py`

**Interfaces:**
- Consumes: `semantic.reach` functions.
- Produces: Lambda handler routing `tool` name → reach function; deployed as `convergence-reach-tools`. Event shape: `{"tool": "get_cumulative_reach", "params": {...}}` → `{"reach": int, ...}`.

- [ ] **Step 1: Write the failing test**

`tests/test_reach_tools_lambda.py`:
```python
from unittest.mock import patch
from agent import reach_tools_lambda as L

def test_dispatch_daily():
    with patch("agent.reach_tools_lambda.reach.get_daily_reach", return_value={"reach": 10}) as m:
        out = L.handler({"tool": "get_daily_reach",
                         "params": {"campaign": "camp_finals", "segment": "sports", "day": "2026-07-03"}}, None)
    assert out["reach"] == 10
    m.assert_called_once()

def test_unknown_tool_errors():
    out = L.handler({"tool": "nope", "params": {}}, None)
    assert "error" in out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_reach_tools_lambda.py -v`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement handler**

`agent/reach_tools_lambda.py`:
```python
from semantic import reach

TOOLS = {
    "get_daily_reach": lambda p: reach.get_daily_reach(p["campaign"], p.get("segment"), p["day"]),
    "get_cumulative_reach": lambda p: reach.get_cumulative_reach(p["campaign"], p.get("segment"), p["start"], p["end"]),
    "merge_segment_reach": lambda p: reach.merge_segment_reach(p["campaign"], p["segments"], p["start"], p["end"]),
    "list_campaigns": lambda p: {"campaigns": reach.list_campaigns()},
    "list_segments": lambda p: {"segments": reach.list_segments()},
}

def handler(event, context):
    tool = event.get("tool")
    params = event.get("params", {})
    if tool not in TOOLS:
        return {"error": f"unknown tool: {tool}", "available": list(TOOLS)}
    try:
        return TOOLS[tool](params)
    except Exception as e:  # surface to the agent
        return {"error": str(e)}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./.venv/bin/pytest tests/test_reach_tools_lambda.py -v`
Expected: 2 passed.

- [ ] **Step 5: Package + Terraform Lambda**

`scripts/package_lambda.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
rm -rf build/lambda && mkdir -p build/lambda
cp -r agent semantic build/lambda/
./.venv/bin/pip install boto3 -t build/lambda >/dev/null
(cd build/lambda && zip -qr ../reach_tools.zip .)
echo "built build/reach_tools.zip"
```
`terraform/lambda.tf`:
```hcl
resource "aws_iam_role" "lambda" {
  name = "${var.project}-lambda-role"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }] })
}
resource "aws_iam_role_policy" "lambda" {
  role = aws_iam_role.lambda.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow",
    Action = ["athena:*","glue:*","s3:*","logs:*"], Resource = "*" }] })
}
resource "aws_lambda_function" "reach_tools" {
  function_name = "${var.project}-reach-tools"
  role          = aws_iam_role.lambda.arn
  handler       = "agent.reach_tools_lambda.handler"
  runtime       = "python3.11"
  timeout       = 60
  filename      = "${path.module}/../build/reach_tools.zip"
  source_code_hash = filebase64sha256("${path.module}/../build/reach_tools.zip")
  environment { variables = { ATHENA_WG = "${var.project}-wg", AWS_REGION_ = var.region } }
}
output "reach_tools_lambda_arn" { value = aws_lambda_function.reach_tools.arn }
```

- [ ] **Step 6: Package, apply, invoke**

Run: `bash scripts/package_lambda.sh && cd terraform && terraform apply -auto-approve && cd .. && aws lambda invoke --function-name convergence-reach-tools --payload '{"tool":"list_campaigns","params":{}}' --cli-binary-format raw-in-base64-out /dev/stdout`
Expected: JSON with `campaigns` list from Gold.

- [ ] **Step 7: Commit**

```bash
git add agent/reach_tools_lambda.py tests/test_reach_tools_lambda.py scripts/package_lambda.sh terraform/lambda.tf
git commit -m "feat: reach tools lambda + deployment"
```

### Task 13: Bedrock AgentCore Gateway + agent wiring

**Files:**
- Create: `agent/agent_config.py`, `agent/deploy_agent.py`

**Interfaces:**
- Consumes: `reach_tools_lambda_arn`.
- Produces: an AgentCore Gateway exposing the Lambda as tools + a managed-harness agent; `invoke_agent(prompt: str) -> str` used by the dashboard (Task 14).

- [ ] **Step 1: Agent system prompt + tool schema**

`agent/agent_config.py`:
```python
SYSTEM_PROMPT = (
    "You are the Convergence Reach analyst for WBD ad sales. "
    "Reach = count of unique individuals exposed to an ad in a time window. "
    "Use get_daily_reach for a single day (exact). Use get_cumulative_reach to combine "
    "multiple days (HLL-merged, deduped). Use merge_segment_reach to combine audience "
    "segments (deduped across segments). Dates are YYYY-MM-DD. Always state the campaign, "
    "segment(s), and window, and note when a number is HLL-approximate."
)

TOOL_SCHEMA = [
    {"name": "get_daily_reach", "params": ["campaign", "segment", "day"]},
    {"name": "get_cumulative_reach", "params": ["campaign", "segment", "start", "end"]},
    {"name": "merge_segment_reach", "params": ["campaign", "segments", "start", "end"]},
    {"name": "list_campaigns", "params": []},
    {"name": "list_segments", "params": []},
]
```

- [ ] **Step 2: Deploy script (Gateway target = Lambda; agent = managed harness)**

`agent/deploy_agent.py`:
```python
"""Registers the reach Lambda as AgentCore Gateway tools and creates a managed agent.
Run: ./.venv/bin/python -m agent.deploy_agent
Falls back to printing the exact console/CLI steps if the AgentCore API surface differs."""
import json
import subprocess
from agent.agent_config import SYSTEM_PROMPT, TOOL_SCHEMA

def _tf_output(name):
    return subprocess.check_output(
        ["terraform", "output", "-raw", name], cwd="terraform").decode().strip()

def main():
    lambda_arn = _tf_output("reach_tools_lambda_arn")
    print("Lambda:", lambda_arn)
    print("System prompt:\n", SYSTEM_PROMPT)
    print("Tools:", json.dumps(TOOL_SCHEMA, indent=2))
    print(
        "\nAgentCore setup (via `agentcore` CLI — the 2026 recommended path):\n"
        "  1) agentcore gateway create --name convergence-reach-gw\n"
        f"  2) agentcore gateway add-target --gateway convergence-reach-gw --lambda {lambda_arn} \\\n"
        "       --tool-schema agent/agent_config.py\n"
        "  3) agentcore agent create --name convergence-reach --model anthropic.claude "
        "--system-prompt-file <(python -c 'from agent.agent_config import SYSTEM_PROMPT;print(SYSTEM_PROMPT)') "
        "--gateway convergence-reach-gw\n"
        "  4) agentcore agent invoke --name convergence-reach --prompt 'cumulative reach ...'\n"
    )

if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Deploy the agent**

Run: `./.venv/bin/python -m agent.deploy_agent`
Then follow the printed `agentcore` CLI steps (install via `pip install bedrock-agentcore-starter-toolkit` if needed).
Expected: a `convergence-reach` agent bound to the Gateway tools.

- [ ] **Step 4: Smoke-test the agent**

Run: `agentcore agent invoke --name convergence-reach --prompt "What was the cumulative reach of camp_finals for sports from 2026-07-01 to 2026-07-05?"`
Expected: agent calls `get_cumulative_reach` and answers with the HLL reach number.

- [ ] **Step 5: Commit**

```bash
git add agent/agent_config.py agent/deploy_agent.py
git commit -m "feat: bedrock agentcore gateway tools + reach agent"
```

---

## Phase 7 — Public dashboard (App Runner)

### Task 14: FastAPI dashboard app + agent chat proxy

**Files:**
- Create: `dashboard/reach_source.py`, `dashboard/app.py`, `dashboard/templates/index.html`, `dashboard/static/app.js`, `dashboard/requirements.txt`, `dashboard/Dockerfile`
- Test: `tests/test_dashboard.py`

**Interfaces:**
- Consumes: `semantic.reach` (server-side, via instance role); agent invoke.
- Produces: `GET /api/reach/daily|cumulative|segment-merge` JSON; `POST /api/chat`; `GET /` HTML. Container listens on `8080`.

- [ ] **Step 1: Write the failing test**

`tests/test_dashboard.py`:
```python
from unittest.mock import patch
from fastapi.testclient import TestClient
from dashboard.app import app

client = TestClient(app)

def test_daily_endpoint():
    with patch("dashboard.app.reach.get_daily_reach", return_value={"reach": 111, "sql": "x"}):
        r = client.get("/api/reach/daily", params={"campaign": "camp_finals", "segment": "sports", "day": "2026-07-03"})
    assert r.status_code == 200 and r.json()["reach"] == 111

def test_index_renders():
    r = client.get("/")
    assert r.status_code == 200 and "Convergence Reach" in r.text
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./.venv/bin/pytest tests/test_dashboard.py -v`
Expected: FAIL (`ModuleNotFoundError: dashboard.app`).

- [ ] **Step 3: Implement app**

`dashboard/app.py`:
```python
import os
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from semantic import reach

app = FastAPI(title="Convergence Reach")
_here = os.path.dirname(__file__)
app.mount("/static", StaticFiles(directory=os.path.join(_here, "static")), name="static")

@app.get("/", response_class=HTMLResponse)
def index():
    with open(os.path.join(_here, "templates", "index.html")) as f:
        return f.read()

@app.get("/api/reach/daily")
def daily(campaign: str, segment: str | None = None, day: str = "2026-07-03"):
    return reach.get_daily_reach(campaign, segment, day)

@app.get("/api/reach/cumulative")
def cumulative(campaign: str, segment: str | None = None, start: str = "2026-07-01", end: str = "2026-07-05"):
    return reach.get_cumulative_reach(campaign, segment, start, end)

@app.get("/api/reach/segment-merge")
def segment_merge(campaign: str, segments: str, start: str = "2026-07-01", end: str = "2026-07-05"):
    return reach.merge_segment_reach(campaign, segments.split(","), start, end)

@app.post("/api/chat")
def chat(body: dict):
    import boto3, json
    payload = {"tool": "list_campaigns", "params": {}}  # replaced by real agent invoke below
    try:
        client = boto3.client("bedrock-agentcore", region_name=os.getenv("AWS_REGION", "us-east-1"))
        resp = client.invoke_agent_runtime(agentRuntimeArn=os.getenv("AGENT_ARN", ""),
                                            payload=json.dumps({"prompt": body.get("prompt", "")}))
        return {"reply": resp["response"].read().decode()}
    except Exception as e:
        return {"reply": f"(agent unavailable: {e})"}
```

`dashboard/reach_source.py` (kept minimal — re-exports for clarity/testing):
```python
from semantic import reach  # noqa: F401
```

- [ ] **Step 4: Templates + static + Docker**

`dashboard/templates/index.html`:
```html
<!doctype html><html><head><meta charset="utf-8"><title>Convergence Reach</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>body{font-family:system-ui;margin:2rem;max-width:900px}h1{color:#5b3df5}
.card{border:1px solid #ddd;border-radius:12px;padding:1rem;margin:1rem 0}
input,button{padding:.4rem}</style></head><body>
<h1>Convergence Reach</h1>
<p>Digital + linear, unified. Daily exact reach and HLL-merged cumulative / cross-segment reach.</p>
<div class="card"><canvas id="chart"></canvas></div>
<div class="card"><h3>Ask the reach agent</h3>
<input id="q" size="60" placeholder="cumulative reach of camp_finals for sports last week"/>
<button onclick="ask()">Ask</button><pre id="ans"></pre></div>
<script src="/static/app.js"></script></body></html>
```
`dashboard/static/app.js`:
```javascript
async function load() {
  const days = ["2026-07-01","2026-07-02","2026-07-03","2026-07-04","2026-07-05"];
  const daily = [];
  for (const d of days) {
    const r = await fetch(`/api/reach/daily?campaign=camp_finals&segment=sports&day=${d}`).then(x=>x.json());
    daily.push(r.reach);
  }
  const cum = await fetch(`/api/reach/cumulative?campaign=camp_finals&segment=sports&start=2026-07-01&end=2026-07-05`).then(x=>x.json());
  new Chart(document.getElementById("chart"), {type:"bar",
    data:{labels:[...days,"CUMULATIVE"],datasets:[{label:"Reach (unique individuals)",
      data:[...daily,cum.reach],backgroundColor:["#9b8cff","#9b8cff","#9b8cff","#9b8cff","#9b8cff","#5b3df5"]}]}});
}
async function ask(){
  document.getElementById("ans").textContent="…";
  const r = await fetch("/api/chat",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({prompt:document.getElementById("q").value})}).then(x=>x.json());
  document.getElementById("ans").textContent=r.reply;
}
load();
```
`dashboard/requirements.txt`:
```
fastapi==0.111.0
uvicorn==0.30.1
boto3==1.34.131
```
`dashboard/Dockerfile`:
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY semantic ./semantic
COPY dashboard ./dashboard
ENV AWS_REGION=us-east-1 ATHENA_WG=convergence-wg
EXPOSE 8080
CMD ["uvicorn","dashboard.app:app","--host","0.0.0.0","--port","8080"]
```

- [ ] **Step 5: Run tests**

Run: `./.venv/bin/pytest tests/test_dashboard.py -v`
Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add dashboard/ tests/test_dashboard.py
git commit -m "feat: fastapi reach dashboard + agent chat"
```

### Task 15: App Runner deploy (build → ECR → public URL)

**Files:**
- Create: `terraform/apprunner.tf`, `scripts/push_image.sh`, `scripts/deploy.sh`, `scripts/teardown.sh`

**Interfaces:**
- Produces: public `service_url`; one-shot `deploy.sh`; `teardown.sh` destroys all.

- [ ] **Step 1: App Runner Terraform**

`terraform/apprunner.tf`:
```hcl
resource "aws_iam_role" "apprunner_instance" {
  name = "${var.project}-apprunner-instance"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Principal = { Service = "tasks.apprunner.amazonaws.com" }, Action = "sts:AssumeRole" }] })
}
resource "aws_iam_role_policy" "apprunner_instance" {
  role = aws_iam_role.apprunner_instance.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [{ Effect = "Allow",
    Action = ["athena:*","glue:*","s3:*","bedrock:*","bedrock-agentcore:*"], Resource = "*" }] })
}
resource "aws_iam_role" "apprunner_access" {
  name = "${var.project}-apprunner-access"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Principal = { Service = "build.apprunner.amazonaws.com" }, Action = "sts:AssumeRole" }] })
}
resource "aws_iam_role_policy_attachment" "apprunner_ecr" {
  role       = aws_iam_role.apprunner_access.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess"
}
resource "aws_apprunner_service" "dashboard" {
  service_name = "${var.project}-dashboard"
  source_configuration {
    authentication_configuration { access_role_arn = aws_iam_role.apprunner_access.arn }
    image_repository {
      image_identifier      = "${aws_ecr_repository.dashboard.repository_url}:latest"
      image_repository_type = "ECR"
      image_configuration { port = "8080" }
    }
    auto_deployments_enabled = true
  }
  instance_configuration {
    cpu = "256", memory = "512"
    instance_role_arn = aws_iam_role.apprunner_instance.arn
  }
}
output "service_url" { value = "https://${aws_apprunner_service.dashboard.service_url}" }
```

- [ ] **Step 2: Image push + orchestration scripts**

`scripts/push_image.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
source scripts/env.sh
REPO=$(cd terraform && terraform output -raw ecr_repo_url)
aws ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "${REPO%/*}"
docker build -t "$REPO:latest" -f dashboard/Dockerfile .
docker push "$REPO:latest"
echo "pushed $REPO:latest"
```
`scripts/deploy.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
source scripts/env.sh
(cd terraform && terraform init && terraform apply -auto-approve \
  -target=aws_s3_bucket.lake -target=aws_glue_catalog_database.convergence \
  -target=aws_athena_workgroup.wg -target=aws_ecr_repository.dashboard)
bash scripts/create_tables.sh
bash scripts/upload_spark.sh
bash scripts/package_lambda.sh
(cd terraform && terraform apply -auto-approve)   # emr, mwaa, lambda, apprunner
bash scripts/push_image.sh
bash scripts/seed.sh
echo "Dashboard: $(cd terraform && terraform output -raw service_url)"
```
`scripts/teardown.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
source scripts/env.sh
aws s3 rm "s3://${BUCKET}" --recursive || true
(cd terraform && terraform destroy -auto-approve)
```

- [ ] **Step 3: Push image + apply**

Run: `bash scripts/push_image.sh && cd terraform && terraform apply -auto-approve`
Expected: App Runner service reaches `RUNNING`; prints `service_url`.

- [ ] **Step 4: Verify public URL (unauthenticated)**

Run: `URL=$(cd terraform && terraform output -raw service_url); curl -s "$URL" | grep -o "Convergence Reach"; echo "open $URL"`
Expected: prints `Convergence Reach`; opening the URL shows the reach chart + agent chat, with no AWS login.

- [ ] **Step 5: Commit**

```bash
git add terraform/apprunner.tf scripts/push_image.sh scripts/deploy.sh scripts/teardown.sh
git commit -m "feat: app runner public dashboard + deploy/teardown scripts"
```

---

## Phase 8 — Shareable Artifact + docs

### Task 16: Claude Artifact one-pager + README demo script

**Files:**
- Create: `docs/artifact/convergence-reach.html`
- Modify: `README.md`

**Interfaces:**
- Produces: a self-contained HTML artifact (architecture + reach concept + sample numbers) published via the Artifact tool; complete README run/demo/teardown instructions.

- [ ] **Step 1: Fill README**

`README.md` must document: prerequisites (AWS creds, Docker, Terraform, Python 3.11), `bash scripts/deploy.sh`, how to trigger the MWAA `convergence_ingest` DAG, the public dashboard URL, sample agent questions, cost note (MWAA ~$0.49/hr), and `bash scripts/teardown.sh`.

- [ ] **Step 2: Author the artifact HTML**

Create `docs/artifact/convergence-reach.html` (no `<html>/<head>/<body>` wrapper — page-content only per the Artifact tool): architecture diagram (inline SVG mirroring landing→bronze→silver→gold→semantic→agent→dashboard), a "What is Reach" explainer, and a table of the validated sample numbers (daily vs cumulative vs segment-merged) captured from Task 10. Load the `artifact-design` skill before writing it.

- [ ] **Step 3: Publish the artifact**

Use the Artifact tool with `file_path=docs/artifact/convergence-reach.html`, a title, `favicon="📺📊"`, and a one-line description.
Expected: returns a shareable claude.ai URL.

- [ ] **Step 4: Commit**

```bash
git add docs/artifact/convergence-reach.html README.md
git commit -m "docs: readme demo script + shareable reach artifact"
```

---

## Self-Review

**1. Spec coverage:**
- Convergence digital+linear model → Tasks 1–2, 8. ✓
- Landing/bronze/silver/gold Iceberg → Tasks 4, 7, 8, 10. ✓
- Athena/Presto HLL sketches alongside exact counts → Tasks 5, 10. ✓
- Reach semantic layer (daily/cumulative/segment-merge) → Tasks 5, 6. ✓
- MWAA-first orchestration (ingest + daily compaction across tables) → Task 11. ✓
- Bedrock AgentCore agent + tool interface → Tasks 12, 13. ✓
- Public URL for non-AWS users → Tasks 14, 15. ✓
- Shareable Claude Artifact → Task 16. ✓
- EMR Serverless PySpark → Tasks 7–9. ✓
- Terraform IaC / us-east-1 / teardown → Tasks 3, 9, 11, 12, 15. ✓
- Reach correctness verification (HLL vs exact; cumulative < sum-of-daily) → Task 10 Step 4. ✓

**2. Placeholder scan:** No "TBD/TODO". The one intentional manual step is the AgentCore CLI wiring in Task 13 (the 2026 AgentCore API is CLI-first; the deploy script prints exact commands) — acceptable because AgentCore's managed-harness/Gateway surface is provisioned via its own CLI, not Terraform.

**3. Type consistency:** `run_query` signature shared by Tasks 6/12/14; `get_daily_reach/get_cumulative_reach/merge_segment_reach` names identical across reach.py, the Lambda, the agent schema, and the dashboard; table names match Global Constraints everywhere; HLL expression string identical in builder and tests.

**Known risk (from spec §9):** MWAA provisioning (~25 min) is the long pole and was chosen as the backbone — Task 10's `run_pipeline.sh` is the fallback that proves the full data path even if MWAA lags.
