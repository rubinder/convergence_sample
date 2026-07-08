import os
import time

import boto3

_ATHENA = boto3.client("athena", region_name=os.getenv("AWS_REGION", "us-east-1"))
_WG = os.getenv("ATHENA_WG", "convergence-wg")


def run_query(sql: str) -> list[dict]:
    qid = _ATHENA.start_query_execution(QueryString=sql, WorkGroup=_WG)[
        "QueryExecutionId"
    ]
    while True:
        st = _ATHENA.get_query_execution(QueryExecutionId=qid)["QueryExecution"][
            "Status"
        ]["State"]
        if st in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.7)
    if st != "SUCCEEDED":
        raise RuntimeError(f"athena query {st}: {sql[:120]}")
    res = _ATHENA.get_query_results(QueryExecutionId=qid)
    rows = res["ResultSet"]["Rows"]
    if len(rows) < 2:
        return []
    cols = [c["VarCharValue"] for c in rows[0]["Data"]]
    out = []
    for r in rows[1:]:
        vals = [d.get("VarCharValue") for d in r["Data"]]
        out.append(dict(zip(cols, vals)))
    return out
