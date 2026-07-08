import logging
import os

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from semantic import reach
from semantic.validate import InvalidInput

log = logging.getLogger("convergence.dashboard")
app = FastAPI(title="Convergence Reach")


def _reach_or_400(fn, *args):
    """Run a reach call, mapping bad input to 400 and hiding internals on 500."""
    try:
        return fn(*args)
    except InvalidInput:
        raise HTTPException(status_code=400, detail="invalid request parameters")
    except Exception:
        log.exception("reach query failed")
        raise HTTPException(status_code=503, detail="reach service unavailable")
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
    return _reach_or_400(reach.get_daily_reach, campaign, segment, day)


@app.get("/api/reach/cumulative")
def cumulative(
    campaign: str,
    segment: str | None = None,
    start: str = "2026-07-01",
    end: str = "2026-07-05",
):
    return _reach_or_400(reach.get_cumulative_reach, campaign, segment, start, end)


@app.get("/api/reach/segment-merge")
def segment_merge(
    campaign: str,
    segments: str,
    start: str = "2026-07-01",
    end: str = "2026-07-05",
):
    return _reach_or_400(
        reach.merge_segment_reach, campaign, segments.split(","), start, end
    )


@app.get("/api/dimensions")
def dimensions():
    try:
        return {"campaigns": reach.list_campaigns(), "segments": reach.list_segments()}
    except Exception:
        log.exception("dimensions lookup failed")
        return {"campaigns": [], "segments": []}


@app.post("/api/chat")
def chat(body: dict):
    prompt = (body.get("prompt") or "").strip()
    if not prompt:
        return {"reply": "Ask a reach question, e.g. 'cumulative reach of camp_finals for sports last week'."}
    try:
        from agent.bedrock_agent import chat as agent_chat

        return {"reply": agent_chat(prompt)}
    except Exception:
        log.exception("agent invoke failed")
        return {"reply": "(agent unavailable)"}
