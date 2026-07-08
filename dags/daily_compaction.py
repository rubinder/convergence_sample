from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

TABLES = ["bronze_impressions", "silver_impressions", "daily_reach_snapshot"]

with DAG(
    "daily_compaction",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["convergence"],
) as dag:
    prev = None
    for t in TABLES:
        opt = AthenaOperator(
            task_id=f"optimize_{t}",
            database="convergence",
            workgroup="convergence-wg",
            query=f"OPTIMIZE convergence.{t} REWRITE DATA USING BIN_PACK",
        )
        vac = AthenaOperator(
            task_id=f"vacuum_{t}",
            database="convergence",
            workgroup="convergence-wg",
            query=f"VACUUM convergence.{t}",
        )
        opt >> vac
        if prev:
            prev >> opt
        prev = vac
