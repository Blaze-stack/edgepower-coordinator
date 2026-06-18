from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional
from uuid import uuid4

SAFE_JOB_KINDS = frozenset({"echo", "sha256", "sleep", "checksum"})
TERMINAL_JOB_STATUSES = frozenset({"succeeded", "failed"})


class StoreNotFoundError(Exception):
    """Raised when a required row does not exist."""


class StoreConflictError(Exception):
    """Raised when a requested state transition is invalid."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def encode_json(value: dict[str, Any]) -> str:
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)


def decode_json(value: Optional[str]) -> Optional[dict[str, Any]]:
    if value is None:
        return None
    return json.loads(value)


class SQLiteCoordinatorStore:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        path = Path(self.db_path)
        if path.parent != Path("."):
            path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    public_key TEXT NOT NULL,
                    capacity_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending', 'assigned', 'succeeded', 'failed')),
                    assigned_node_id TEXT,
                    result_json TEXT,
                    signature TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    assigned_at TEXT,
                    completed_at TEXT,
                    FOREIGN KEY (assigned_node_id) REFERENCES nodes(node_id)
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_pending
                    ON jobs(status, created_at, job_id);

                CREATE TABLE IF NOT EXISTS receipts (
                    receipt_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('succeeded', 'failed')),
                    result_json TEXT NOT NULL,
                    signature TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
                    FOREIGN KEY (node_id) REFERENCES nodes(node_id)
                );

                CREATE INDEX IF NOT EXISTS idx_receipts_job_id
                    ON receipts(job_id, created_at);

                CREATE TABLE IF NOT EXISTS audit_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    actor TEXT,
                    resource_type TEXT NOT NULL,
                    resource_id TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_audit_events_created_at
                    ON audit_events(created_at, event_id);
                """
            )

    def healthcheck(self) -> None:
        with self.connect() as connection:
            connection.execute("SELECT 1").fetchone()

    def upsert_node(self, *, node_id: str, public_key: str, capacity: dict[str, Any]) -> dict[str, Any]:
        self.initialize()
        now = utc_now()
        capacity_json = encode_json(capacity)
        with self.connect() as connection:
            existing = connection.execute(
                "SELECT node_id FROM nodes WHERE node_id = ?",
                (node_id,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO nodes (node_id, public_key, capacity_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (node_id, public_key, capacity_json, now, now),
                )
                event_type = "node_registered"
            else:
                connection.execute(
                    """
                    UPDATE nodes
                    SET public_key = ?, capacity_json = ?, updated_at = ?
                    WHERE node_id = ?
                    """,
                    (public_key, capacity_json, now, node_id),
                )
                event_type = "node_updated"
            self._insert_event(
                connection,
                event_type=event_type,
                actor=node_id,
                resource_type="node",
                resource_id=node_id,
                details={"capacity": capacity},
                created_at=now,
            )
            row = connection.execute(
                """
                SELECT node_id, public_key, capacity_json, created_at, updated_at
                FROM nodes
                WHERE node_id = ?
                """,
                (node_id,),
            ).fetchone()
        return self._node_from_row(row)

    def create_job(self, *, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.initialize()
        if kind not in SAFE_JOB_KINDS:
            raise ValueError("unsafe job kind")
        now = utc_now()
        job_id = str(uuid4())
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO jobs (
                    job_id, kind, payload_json, status, created_at, updated_at
                )
                VALUES (?, ?, ?, 'pending', ?, ?)
                """,
                (job_id, kind, encode_json(payload), now, now),
            )
            self._insert_event(
                connection,
                event_type="job_created",
                actor=None,
                resource_type="job",
                resource_id=job_id,
                details={"kind": kind},
                created_at=now,
            )
        job = self.get_job(job_id)
        if job is None:
            raise StoreNotFoundError("job not found after creation")
        return job

    def assign_next_job(self, *, node_id: str) -> Optional[dict[str, Any]]:
        self.initialize()
        now = utc_now()
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            node = connection.execute(
                "SELECT node_id FROM nodes WHERE node_id = ?",
                (node_id,),
            ).fetchone()
            if node is None:
                raise StoreNotFoundError("node not found")

            row = connection.execute(
                """
                SELECT job_id
                FROM jobs
                WHERE status = 'pending'
                ORDER BY created_at ASC, job_id ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None

            job_id = row["job_id"]
            connection.execute(
                """
                UPDATE jobs
                SET status = 'assigned',
                    assigned_node_id = ?,
                    assigned_at = ?,
                    updated_at = ?
                WHERE job_id = ? AND status = 'pending'
                """,
                (node_id, now, now, job_id),
            )
            self._insert_event(
                connection,
                event_type="job_assigned",
                actor=node_id,
                resource_type="job",
                resource_id=job_id,
                details={"node_id": node_id},
                created_at=now,
            )
            job_row = self._fetch_job_row(connection, job_id)
            receipt_rows = self._fetch_receipts(connection, job_id)
        return self._job_from_row(job_row, receipt_rows=receipt_rows)

    def record_receipt(
        self,
        *,
        job_id: str,
        node_id: str,
        receipt_status: str,
        result: dict[str, Any],
        signature: Optional[str],
    ) -> dict[str, Any]:
        self.initialize()
        if receipt_status not in TERMINAL_JOB_STATUSES:
            raise ValueError("invalid receipt status")
        now = utc_now()
        receipt_id = str(uuid4())
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            job_row = self._fetch_job_row(connection, job_id)
            if job_row is None:
                raise StoreNotFoundError("job not found")
            if job_row["status"] in TERMINAL_JOB_STATUSES:
                raise StoreConflictError("job already completed")
            if job_row["status"] != "assigned" or job_row["assigned_node_id"] != node_id:
                raise StoreConflictError("job is not assigned to this node")

            result_json = encode_json(result)
            connection.execute(
                """
                INSERT INTO receipts (
                    receipt_id, job_id, node_id, status, result_json, signature, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (receipt_id, job_id, node_id, receipt_status, result_json, signature, now),
            )
            connection.execute(
                """
                UPDATE jobs
                SET status = ?,
                    result_json = ?,
                    signature = ?,
                    completed_at = ?,
                    updated_at = ?
                WHERE job_id = ?
                """,
                (receipt_status, result_json, signature, now, now, job_id),
            )
            self._insert_event(
                connection,
                event_type="receipt_recorded",
                actor=node_id,
                resource_type="job",
                resource_id=job_id,
                details={"receipt_id": receipt_id, "status": receipt_status},
                created_at=now,
            )
            updated_job_row = self._fetch_job_row(connection, job_id)
            receipt_rows = self._fetch_receipts(connection, job_id)
        return self._job_from_row(updated_job_row, receipt_rows=receipt_rows)

    def get_job(self, job_id: str) -> Optional[dict[str, Any]]:
        self.initialize()
        with self.connect() as connection:
            row = self._fetch_job_row(connection, job_id)
            if row is None:
                return None
            receipt_rows = self._fetch_receipts(connection, job_id)
        return self._job_from_row(row, receipt_rows=receipt_rows)

    def list_events(self, *, limit: int = 100) -> list[dict[str, Any]]:
        self.initialize()
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT event_id, event_type, actor, resource_type, resource_id, details_json, created_at
                FROM audit_events
                ORDER BY event_id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "event_type": row["event_type"],
                "actor": row["actor"],
                "resource_type": row["resource_type"],
                "resource_id": row["resource_id"],
                "details": decode_json(row["details_json"]) or {},
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def _insert_event(
        self,
        connection: sqlite3.Connection,
        *,
        event_type: str,
        actor: Optional[str],
        resource_type: str,
        resource_id: str,
        details: dict[str, Any],
        created_at: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO audit_events (
                event_type, actor, resource_type, resource_id, details_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_type, actor, resource_type, resource_id, encode_json(details), created_at),
        )

    def _fetch_job_row(self, connection: sqlite3.Connection, job_id: str) -> Optional[sqlite3.Row]:
        return connection.execute(
            """
            SELECT
                job_id, kind, payload_json, status, assigned_node_id, result_json, signature,
                created_at, updated_at, assigned_at, completed_at
            FROM jobs
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()

    def _fetch_receipts(self, connection: sqlite3.Connection, job_id: str) -> list[sqlite3.Row]:
        return connection.execute(
            """
            SELECT receipt_id, job_id, node_id, status, result_json, signature, created_at
            FROM receipts
            WHERE job_id = ?
            ORDER BY created_at ASC
            """,
            (job_id,),
        ).fetchall()

    def _node_from_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "node_id": row["node_id"],
            "public_key": row["public_key"],
            "capacity": decode_json(row["capacity_json"]) or {},
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _job_from_row(self, row: Optional[sqlite3.Row], *, receipt_rows: list[sqlite3.Row]) -> dict[str, Any]:
        if row is None:
            raise StoreNotFoundError("job not found")
        return {
            "job_id": row["job_id"],
            "kind": row["kind"],
            "payload": decode_json(row["payload_json"]) or {},
            "status": row["status"],
            "assigned_node_id": row["assigned_node_id"],
            "result": decode_json(row["result_json"]),
            "signature": row["signature"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "assigned_at": row["assigned_at"],
            "completed_at": row["completed_at"],
            "receipts": [
                {
                    "receipt_id": receipt["receipt_id"],
                    "job_id": receipt["job_id"],
                    "node_id": receipt["node_id"],
                    "status": receipt["status"],
                    "result": decode_json(receipt["result_json"]) or {},
                    "signature": receipt["signature"],
                    "created_at": receipt["created_at"],
                }
                for receipt in receipt_rows
            ],
        }
