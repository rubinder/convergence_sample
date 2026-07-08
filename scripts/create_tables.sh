#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/env.sh

wait_query() {
  local qid="$1"
  while true; do
    st=$(aws athena get-query-execution --query-execution-id "$qid" \
         --query 'QueryExecution.Status.State' --output text)
    case "$st" in
      SUCCEEDED) echo "  $qid SUCCEEDED"; return 0 ;;
      FAILED|CANCELLED)
        aws athena get-query-execution --query-execution-id "$qid" \
          --query 'QueryExecution.Status.StateChangeReason' --output text
        return 1 ;;
      *) sleep 1 ;;
    esac
  done
}

# Split DDL on ';' and submit each statement.
python3 - "$BUCKET" <<'PY' > /tmp/convergence_ddl.txt
import sys
bucket = sys.argv[1]
sql = open("sql/ddl/create_tables.sql").read().replace("__BUCKET__", bucket)
stmts = [s.strip() for s in sql.split(";") if s.strip()]
print("\0".join(stmts), end="")
PY

while IFS= read -r -d '' stmt; do
  [ -z "$stmt" ] && continue
  qid=$(aws athena start-query-execution --work-group "$ATHENA_WG" \
        --query-string "$stmt" --query 'QueryExecutionId' --output text)
  echo "submitted $qid"
  wait_query "$qid"
done < /tmp/convergence_ddl.txt

echo "tables:"
aws glue get-tables --database-name "$GLUE_DB" --query 'TableList[].Name' --output text
