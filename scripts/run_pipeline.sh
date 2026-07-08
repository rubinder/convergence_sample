#!/usr/bin/env bash
# Runs the data path end-to-end without MWAA: EMR bronze -> silver, then Athena gold.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
APP=$(cd terraform && terraform output -raw emr_application_id)
ROLE=$(cd terraform && terraform output -raw emr_job_role_arn)
ENTRY="s3://${BUCKET}/code/spark"
PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,software.amazon.awssdk:bundle:2.20.160"
SPARK_PARAMS="--conf spark.jars.packages=${PKGS} --py-files ${ENTRY}/iceberg_conf.py"

submit() { # $1=script  $2=json-args
  local jid
  jid=$(aws emr-serverless start-job-run \
        --application-id "$APP" --execution-role-arn "$ROLE" \
        --job-driver "{\"sparkSubmit\":{\"entryPoint\":\"${ENTRY}/$1\",\"entryPointArguments\":[$2],\"sparkSubmitParameters\":\"${SPARK_PARAMS}\"}}" \
        --query 'jobRunId' --output text)
  echo "  $1 -> job $jid"
  while true; do
    S=$(aws emr-serverless get-job-run --application-id "$APP" --job-run-id "$jid" \
        --query 'jobRun.state' --output text)
    case "$S" in
      SUCCESS) echo "  $1 SUCCESS"; break ;;
      FAILED|CANCELLED)
        aws emr-serverless get-job-run --application-id "$APP" --job-run-id "$jid" \
          --query 'jobRun.stateDetails' --output text; exit 1 ;;
      *) sleep 15 ;;
    esac
  done
}

echo "== EMR bronze =="
submit "bronze_ingest.py" "\"--bucket\",\"${BUCKET}\""
echo "== EMR silver =="
submit "silver_conform.py" "\"--bucket\",\"${BUCKET}\""

echo "== Athena gold snapshot + HLL sketches =="
GOLD=$(sed -e "s/__START__/2026-07-01/" -e "s/__END__/2026-07-05/" sql/gold_snapshot.sql)
qid=$(aws athena start-query-execution --work-group "$ATHENA_WG" \
      --query-string "$GOLD" --query 'QueryExecutionId' --output text)
while true; do
  st=$(aws athena get-query-execution --query-execution-id "$qid" \
       --query 'QueryExecution.Status.State' --output text)
  case "$st" in
    SUCCEEDED) echo "  gold SUCCEEDED"; break ;;
    FAILED|CANCELLED)
      aws athena get-query-execution --query-execution-id "$qid" \
        --query 'QueryExecution.Status.StateChangeReason' --output text; exit 1 ;;
    *) sleep 2 ;;
  esac
done
echo "pipeline complete."
