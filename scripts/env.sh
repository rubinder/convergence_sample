#!/usr/bin/env bash
set -euo pipefail
export AWS_REGION="us-east-1"
export ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export PROJECT="convergence"
export BUCKET="convergence-lakehouse-${ACCOUNT_ID}"
export GLUE_DB="convergence"
export ATHENA_WG="convergence-wg"
echo "env: region=${AWS_REGION} account=${ACCOUNT_ID} bucket=${BUCKET}"
