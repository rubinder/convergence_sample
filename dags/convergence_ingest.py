from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessStartJobOperator,
)

BUCKET = Variable.get("bucket")
APP = Variable.get("emr_application_id")
ROLE = Variable.get("emr_job_role_arn")
ENTRY = f"s3://{BUCKET}/code/spark"
COMMON = (
    "--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar "
    f"--py-files {ENTRY}/iceberg_conf.py"
)


def job(script, args):
    return {
        "sparkSubmit": {
            "entryPoint": f"{ENTRY}/{script}",
            "entryPointArguments": args,
            "sparkSubmitParameters": COMMON,
        }
    }


with DAG(
    "convergence_ingest",
    start_date=datetime(2026, 7, 1),
    schedule="@hourly",
    catchup=False,
    tags=["convergence"],
) as dag:
    bronze = EmrServerlessStartJobOperator(
        task_id="bronze",
        application_id=APP,
        execution_role_arn=ROLE,
        job_driver=job("bronze_ingest.py", ["--bucket", BUCKET]),
    )
    silver = EmrServerlessStartJobOperator(
        task_id="silver",
        application_id=APP,
        execution_role_arn=ROLE,
        job_driver=job("silver_conform.py", ["--bucket", BUCKET]),
    )
    gold = AthenaOperator(
        task_id="gold",
        database="convergence",
        workgroup="convergence-wg",
        query=(
            "INSERT INTO convergence.daily_reach_snapshot "
            "SELECT event_day AS day, campaign_id, segment, delivery_type, "
            "count(distinct individual_id), count(*), "
            "cast(approx_set(individual_id) AS varbinary) "
            "FROM convergence.silver_impressions "
            "GROUP BY event_day, campaign_id, segment, delivery_type"
        ),
    )
    bronze >> silver >> gold
