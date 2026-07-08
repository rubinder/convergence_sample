from unittest.mock import patch

from fastapi.testclient import TestClient

from dashboard.app import app

client = TestClient(app)


def test_daily_endpoint():
    with patch(
        "dashboard.app.reach.get_daily_reach",
        return_value={"reach": 111, "sql": "x"},
    ):
        r = client.get(
            "/api/reach/daily",
            params={"campaign": "camp_finals", "segment": "sports", "day": "2026-07-03"},
        )
    assert r.status_code == 200 and r.json()["reach"] == 111


def test_index_renders():
    r = client.get("/")
    assert r.status_code == 200 and "Convergence Reach" in r.text
