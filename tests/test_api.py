from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient

from edgepower_coordinator.app import create_app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("EDGEPOWER_DB_PATH", str(tmp_path / "coordinator.sqlite3"))
    with TestClient(create_app()) as test_client:
        yield test_client


def register_node(client: TestClient, node_id: str = "node-a") -> dict:
    response = client.post(
        "/nodes",
        json={
            "node_id": node_id,
            "public_key": "test-public-key",
            "capacity": {"cpu": 2, "memory_mb": 2048},
        },
    )
    assert response.status_code == 200
    return response.json()


def test_health_reports_allowed_safe_job_kinds(client: TestClient):
    response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["database"] == "ok"
    assert body["allowed_job_kinds"] == ["checksum", "echo", "sha256", "sleep"]


def test_node_registration_is_idempotent(client: TestClient):
    first = register_node(client)
    second_response = client.post(
        "/nodes",
        json={
            "node_id": "node-a",
            "public_key": "rotated-public-key",
            "capacity": {"cpu": 4},
        },
    )

    assert second_response.status_code == 200
    second = second_response.json()
    assert first["node_id"] == second["node_id"]
    assert second["public_key"] == "rotated-public-key"
    assert second["capacity"] == {"cpu": 4}


def test_lists_registered_nodes(client: TestClient):
    register_node(client, node_id="node-a")
    register_node(client, node_id="node-b")

    response = client.get("/nodes", params={"limit": 10})

    assert response.status_code == 200
    node_ids = {node["node_id"] for node in response.json()["nodes"]}
    assert {"node-a", "node-b"}.issubset(node_ids)


def test_rejects_unsafe_job_kind(client: TestClient):
    response = client.post("/jobs", json={"kind": "shell", "payload": {"cmd": "whoami"}})

    assert response.status_code == 422


@pytest.mark.parametrize("kind", ["echo", "sha256", "sleep", "checksum"])
def test_accepts_only_allowlisted_job_kinds(client: TestClient, kind: str):
    response = client.post("/jobs", json={"kind": kind, "payload": {"value": "hello"}})

    assert response.status_code == 201
    assert response.json()["kind"] == kind
    assert response.json()["status"] == "pending"


def test_assigns_next_pending_job_to_registered_node(client: TestClient):
    register_node(client)
    created = client.post("/jobs", json={"kind": "echo", "payload": {"message": "hello"}}).json()

    response = client.get("/jobs/next", params={"node_id": "node-a"})

    assert response.status_code == 200
    job = response.json()["job"]
    assert job["job_id"] == created["job_id"]
    assert job["kind"] == "echo"
    assert job["payload"] == {"message": "hello"}
    assert job["status"] == "assigned"
    assert job["assigned_node_id"] == "node-a"


def test_lists_jobs_for_admin_ui(client: TestClient):
    first = client.post("/jobs", json={"kind": "echo", "payload": {"message": "hello"}}).json()
    second = client.post("/jobs", json={"kind": "sha256", "payload": {"data": "hello"}}).json()

    response = client.get("/jobs", params={"limit": 10})

    assert response.status_code == 200
    job_ids = [job["job_id"] for job in response.json()["jobs"]]
    assert second["job_id"] in job_ids
    assert first["job_id"] in job_ids


def test_next_job_returns_null_when_queue_empty(client: TestClient):
    register_node(client)

    response = client.get("/jobs/next", params={"node_id": "node-a"})

    assert response.status_code == 200
    assert response.json() == {"job": None}


def test_next_job_requires_registered_node(client: TestClient):
    response = client.get("/jobs/next", params={"node_id": "missing-node"})

    assert response.status_code == 404
    assert response.json()["detail"] == "node not found"


def test_receipt_marks_assigned_job_done_and_is_audited(client: TestClient):
    register_node(client)
    created = client.post("/jobs", json={"kind": "sha256", "payload": {"data": "hello"}}).json()
    client.get("/jobs/next", params={"node_id": "node-a"})

    receipt_response = client.post(
        f"/jobs/{created['job_id']}/receipts",
        json={
            "node_id": "node-a",
            "status": "succeeded",
            "result": {"sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},
            "signature": None,
        },
    )

    assert receipt_response.status_code == 200
    completed = receipt_response.json()
    assert completed["status"] == "succeeded"
    assert completed["result"]["sha256"].startswith("2cf24")
    assert completed["receipts"][0]["node_id"] == "node-a"

    job_response = client.get(f"/jobs/{created['job_id']}")
    assert job_response.status_code == 200
    assert job_response.json()["completed_at"] is not None

    events = client.get("/events").json()["events"]
    event_types = [event["event_type"] for event in events]
    assert "node_registered" in event_types
    assert "job_created" in event_types
    assert "job_assigned" in event_types
    assert "receipt_recorded" in event_types


def test_receipt_from_wrong_node_is_rejected(client: TestClient):
    register_node(client, node_id="node-a")
    register_node(client, node_id="node-b")
    created = client.post("/jobs", json={"kind": "echo", "payload": {"message": "hello"}}).json()
    client.get("/jobs/next", params={"node_id": "node-a"})

    response = client.post(
        f"/jobs/{created['job_id']}/receipts",
        json={
            "node_id": "node-b",
            "status": "failed",
            "result": {"error": "not my job"},
            "signature": "test-signature",
        },
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "job is not assigned to this node"


def test_default_database_path_is_relative_to_runtime(monkeypatch):
    monkeypatch.delenv("EDGEPOWER_DB_PATH", raising=False)
    from edgepower_coordinator.app import get_db_path

    assert os.path.basename(get_db_path()) == "edgepower-coordinator.sqlite3"


def test_cors_preflight_allows_browser_admin_ui(client: TestClient):
    response = client.options(
        "/jobs",
        headers={
            "Origin": "http://localhost:4173",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "*"
