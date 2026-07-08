#!/usr/bin/env bash
# Uploads DAGs to the MWAA bucket path and sets Airflow Variables from TF outputs.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
aws s3 cp dags/ "s3://${BUCKET}/dags/" --recursive --exclude "__pycache__/*"

ENV="convergence-mwaa"
run_cli() { # $1 = airflow cli args
  local tok host
  tok=$(aws mwaa create-cli-token --name "$ENV")
  host=$(echo "$tok" | python3 -c 'import sys,json;print(json.load(sys.stdin)["WebServerHostname"])')
  bearer=$(echo "$tok" | python3 -c 'import sys,json;print(json.load(sys.stdin)["CliToken"])')
  curl -s -X POST "https://${host}/aws_mwaa/cli" \
    -H "Authorization: Bearer ${bearer}" -H "Content-Type: text/plain" \
    --data-raw "$1" | python3 -c 'import sys,json,base64;d=json.load(sys.stdin);print(base64.b64decode(d.get("stdout","")).decode())' || true
}

BUCKET_V="$BUCKET"
APP_V=$(cd terraform && terraform output -raw emr_application_id)
ROLE_V=$(cd terraform && terraform output -raw emr_job_role_arn)
run_cli "variables set bucket ${BUCKET_V}"
run_cli "variables set emr_application_id ${APP_V}"
run_cli "variables set emr_job_role_arn ${ROLE_V}"
echo "DAGs uploaded and Airflow Variables set."
