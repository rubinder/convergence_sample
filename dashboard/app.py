import json
import os

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from semantic import reach

app = FastAPI(title="Convergence Reach")
_here = os.path.dirname(__file__)
app.mount("/static", StaticFiles(directory=os.path.join(_here, "static")), name="static")


@app.get("/", response_class=HTMLResponse)
def index():
    with open(os.path.join(_here, "templates", "index.html")) as f:
        return f.read()


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/api/reach/daily")
def daily(campaign: str, segment: str | None = None, day: str = "2026-07-03"):
    return reach.get_daily_reach(campaign, segment, day)


@app.get("/api/reach/cumulative")
def cumulative(
    campaign: str,
    segment: str | None = None,
    start: str = "2026-07-01",
    end: str = "2026-07-05",
):
    return reach.get_cumulative_reach(campaign, segment, start, end)


@app.get("/api/reach/segment-merge")
def segment_merge(
    campaign: str,
    segments: str,
    start: str = "2026-07-01",
    end: str = "2026-07-05",
):
    return reach.merge_segment_reach(campaign, segments.split(","), start, end)


@app.get("/api/dimensions")
def dimensions():
    try:
        return {"campaigns": reach.list_campaigns(), "segments": reach.list_segments()}
    except Exception as e:
        return {"campaigns": [], "segments": [], "error": str(e)}


@app.post("/api/chat")
def chat(body: dict):
    import boto3

    agent_arn = os.getenv("AGENT_ARN", "")
    if not agent_arn:
        return {"reply": "(agent not configured: set AGENT_ARN)"}
    try:
        client = boto3.client(
            "bedrock-agentcore", region_name=os.getenv("AWS_REGION", "us-east-1")
        )
        resp = client.invoke_agent_runtime(
            agentRuntimeArn=agent_arn,
            payload=json.dumps({"prompt": body.get("prompt", "")}),
        )
        return {"reply": resp["response"].read().decode()}
    except Exception as e:
        return {"reply": f"(agent unavailable: {e})"}
