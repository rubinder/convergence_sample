#!/usr/bin/env bash
set -euo pipefail
export AWS_REGION="us-east-1"
export ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export PROJECT="convergence"
export BUCKET="convergence-lakehouse-${ACCOUNT_ID}"
export GLUE_DB="convergence"
export ATHENA_WG="convergence-wg"
# The CLI authenticates via the `aws login` token cache, which the Terraform/Go
# SDK cannot read. Resolve the active session into standard env vars so Terraform
# and boto3 both pick it up.
eval "$(aws configure export-credentials --format env 2>/dev/null)" || true
echo "env: region=${AWS_REGION} account=${ACCOUNT_ID} bucket=${BUCKET}"
