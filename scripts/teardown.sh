#!/usr/bin/env bash
# Destroys ALL convergence AWS resources. Run when the demo is done.
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
echo "Emptying s3://${BUCKET} ..."
aws s3 rm "s3://${BUCKET}" --recursive || true
echo "terraform destroy (includes MWAA if deployed) ..."
(cd terraform && terraform destroy -auto-approve \
  -var deploy_dashboard=true -var deploy_mwaa=true)
echo "teardown complete."
