from __future__ import annotations

import json
import os
from contextlib import asynccontextmanager
from typing import Any, Literal, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .store import (
    SAFE_JOB_KINDS,
    SQLiteCoordinatorStore,
    StoreConflictError,
    StoreNotFoundError,
)

MAX_PAYLOAD_BYTES = 64 * 1024
MAX_CAPACITY_BYTES = 16 * 1024


def _ensure_json_object_size(value: dict[str, Any], *, max_bytes: int, label: str) -> dict[str, Any]:
    try:
        encoded = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be JSON serializable") from exc
    if len(encoded.encode("utf-8")) > max_bytes:
        raise ValueError(f"{label} must be at most {max_bytes} bytes when encoded as JSON")
    return value


class NodeRegistration(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    public_key: str = Field(min_length=1, max_length=4096)
    capacity: dict[str, Any] = Field(default_factory=dict)

    @field_validator("capacity")
    @classmethod
    def validate_capacity(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _ensure_json_object_size(value, max_bytes=MAX_CAPACITY_BYTES, label="capacity")


class JobCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in SAFE_JOB_KINDS:
            allowed = ", ".join(sorted(SAFE_JOB_KINDS))
            raise ValueError(f"unsupported job kind; allowed kinds: {allowed}")
        return normalized

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _ensure_json_object_size(value, max_bytes=MAX_PAYLOAD_BYTES, label="payload")


class ReceiptCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    status: Literal["succeeded", "failed"]
    result: dict[str, Any] = Field(default_factory=dict)
    signature: Optional[str] = Field(default=None, max_length=8192)

    @field_validator("result")
    @classmethod
    def validate_result(cls, value: dict[str, Any]) -> dict[str, Any]:
        return _ensure_json_object_size(value, max_bytes=MAX_PAYLOAD_BYTES, label="result")


class HealthResponse(BaseModel):
    status: Literal["ok"]
    database: Literal["ok"]
    allowed_job_kinds: list[str]


class JobEnvelope(BaseModel):
    job: Optional[dict[str, Any]]


def get_db_path() -> str:
    return os.environ.get("EDGEPOWER_DB_PATH", "edgepower-coordinator.sqlite3")


def get_store() -> SQLiteCoordinatorStore:
    return SQLiteCoordinatorStore(get_db_path())


@asynccontextmanager
async def lifespan(app: FastAPI):
    store = get_store()
    store.initialize()
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title="EdgePower Coordinator",
        version="0.1.0",
        description="Safe coordinator for allowlisted distributed edge-compute jobs.",
        lifespan=lifespan,
    )

    @app.get("/health", response_model=HealthResponse)
    def health(store: SQLiteCoordinatorStore = Depends(get_store)) -> HealthResponse:
        store.healthcheck()
        return HealthResponse(
            status="ok",
            database="ok",
            allowed_job_kinds=sorted(SAFE_JOB_KINDS),
        )

    @app.post("/nodes", status_code=status.HTTP_200_OK)
    def upsert_node(
        registration: NodeRegistration,
        store: SQLiteCoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        return store.upsert_node(
            node_id=registration.node_id,
            public_key=registration.public_key,
            capacity=registration.capacity,
        )

    @app.post("/jobs", status_code=status.HTTP_201_CREATED)
    def create_job(job: JobCreate, store: SQLiteCoordinatorStore = Depends(get_store)) -> dict[str, Any]:
        return store.create_job(kind=job.kind, payload=job.payload)

    @app.get("/jobs/next", response_model=JobEnvelope)
    def next_job(
        node_id: str = Query(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$"),
        store: SQLiteCoordinatorStore = Depends(get_store),
    ) -> JobEnvelope:
        try:
            return JobEnvelope(job=store.assign_next_job(node_id=node_id))
        except StoreNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    @app.post("/jobs/{job_id}/receipts")
    def record_receipt(
        job_id: str,
        receipt: ReceiptCreate,
        store: SQLiteCoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        try:
            return store.record_receipt(
                job_id=job_id,
                node_id=receipt.node_id,
                receipt_status=receipt.status,
                result=receipt.result,
                signature=receipt.signature,
            )
        except StoreNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except StoreConflictError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str, store: SQLiteCoordinatorStore = Depends(get_store)) -> dict[str, Any]:
        job = store.get_job(job_id=job_id)
        if job is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="job not found")
        return job

    @app.get("/events")
    def get_events(
        limit: int = Query(default=100, ge=1, le=500),
        store: SQLiteCoordinatorStore = Depends(get_store),
    ) -> dict[str, Any]:
        return {"events": store.list_events(limit=limit)}

    return app


app = create_app()
