#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf build/lambda && mkdir -p build/lambda
cp -r agent semantic build/lambda/
find build/lambda -name "__pycache__" -type d -prune -exec rm -rf {} +
./.venv/bin/pip install -q boto3 -t build/lambda >/dev/null
(cd build/lambda && zip -qr ../reach_tools.zip .)
echo "built build/reach_tools.zip"
