#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh
REPO=$(cd terraform && terraform output -raw ecr_repo_url)
REGISTRY="${REPO%/*}"
aws ecr get-login-password --region "$AWS_REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"
docker build --platform linux/amd64 -t "$REPO:latest" -f dashboard/Dockerfile .
docker push "$REPO:latest"
echo "pushed $REPO:latest"
