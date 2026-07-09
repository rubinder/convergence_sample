#!/usr/bin/env bash
# Runs the data path end-to-end without MWAA: EMR bronze -> silver, then Athena gold.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
APP=$(cd terraform && terraform output -raw emr_application_id)
ROLE=$(cd terraform && terraform output -raw emr_job_role_arn)
ENTRY="s3://${BUCKET}/code/spark"
# EMR Serverless has no internet egress, so we cannot resolve Maven packages.
# EMR 7.1 ships Iceberg + the AWS SDK on-image; point at the bundled jar.
ICEBERG_JAR="/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"
SPARK_PARAMS="--conf spark.jars=${ICEBERG_JAR} --py-files ${ENTRY}/iceberg_conf.py"

LOGCFG="{\"monitoringConfiguration\":{\"s3MonitoringConfiguration\":{\"logUri\":\"s3://${BUCKET}/emr-logs/\"}}}"

submit() { # $1=script  $2=json-args
  local jid
  jid=$(aws emr-serverless start-job-run \
        --application-id "$APP" --execution-role-arn "$ROLE" \
        --configuration-overrides "$LOGCFG" \
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

run_athena() { # $1 = sql ; returns after SUCCEEDED, exits on failure
  local qid st
  qid=$(aws athena start-query-execution --work-group "$ATHENA_WG" \
        --query-string "$1" --query 'QueryExecutionId' --output text)
  while true; do
    st=$(aws athena get-query-execution --query-execution-id "$qid" \
         --query 'QueryExecution.Status.State' --output text)
    case "$st" in
      SUCCEEDED) return 0 ;;
      FAILED|CANCELLED)
        aws athena get-query-execution --query-execution-id "$qid" \
          --query 'QueryExecution.Status.StateChangeReason' --output text; exit 1 ;;
      *) sleep 2 ;;
    esac
  done
}

echo "== Athena gold snapshot + HLL sketches (idempotent: delete window, then insert) =="
# delete the window first so re-runs replace rather than duplicate snapshots
run_athena "DELETE FROM convergence.daily_reach_snapshot WHERE day BETWEEN date '2026-07-01' AND date '2026-07-05'"
GOLD=$(sed -e "s/__START__/2026-07-01/" -e "s/__END__/2026-07-05/" sql/gold_snapshot.sql)
run_athena "$GOLD"
echo "  gold SUCCEEDED"
echo "pipeline complete."
