#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
aws s3 cp spark/ "s3://${BUCKET}/code/spark/" --recursive --exclude "__pycache__/*"
echo "uploaded spark code to s3://${BUCKET}/code/spark/"
