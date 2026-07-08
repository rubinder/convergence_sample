#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
./.venv/bin/python -m data_generator.generate_events \
  --days 5 --events-per-day 20000 --seed 1 --bucket "$BUCKET"
echo "seeded landing zone in s3://${BUCKET}/landing/"
