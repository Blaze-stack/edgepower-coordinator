# EdgePower Coordinator

EdgePower Coordinator is a deployable FastAPI service that coordinates safe,
allowlisted edge-compute jobs. It stores nodes, jobs, receipts, and audit events
in SQLite.

The coordinator intentionally does not run shell commands, execute remote code,
mine cryptocurrency, access credentials, or provide a generic remote execution
primitive. It only queues jobs whose `kind` is one of:

- `echo`
- `sha256`
- `sleep`
- `checksum`

Workers are expected to implement those safe operations locally and submit a
receipt when done.

## Quickstart

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[test]"
pytest -q
uvicorn edgepower_coordinator.app:app --reload --host 127.0.0.1 --port 8000
```

The default database is `edgepower-coordinator.sqlite3` in the process working
directory. Override it with:

```powershell
$env:EDGEPOWER_DB_PATH = "C:\data\edgepower.sqlite3"
```

## Docker

```powershell
docker build -t edgepower-coordinator .
docker run --rm -p 8000:8000 -v edgepower-data:/data edgepower-coordinator
```

## API

### Health

```http
GET /health
```

Returns service and database health plus the allowlisted job kinds.

### Register or update a node

```http
POST /nodes
Content-Type: application/json

{
  "node_id": "node-a",
  "public_key": "base64-or-pem-public-key",
  "capacity": { "cpu": 4, "memory_mb": 8192 }
}
```

### Create a job

```http
POST /jobs
Content-Type: application/json

{
  "kind": "sha256",
  "payload": { "data": "hello" }
}
```

The service rejects any job kind outside the allowlist.

### Fetch the next job for a node

```http
GET /jobs/next?node_id=node-a
```

Returns:

```json
{ "job": null }
```

or a single assigned job:

```json
{
  "job": {
    "job_id": "uuid",
    "kind": "echo",
    "payload": { "message": "hello" },
    "status": "assigned",
    "assigned_node_id": "node-a"
  }
}
```

### Submit a receipt

```http
POST /jobs/{job_id}/receipts
Content-Type: application/json

{
  "node_id": "node-a",
  "status": "succeeded",
  "result": { "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" },
  "signature": null
}
```

Only the node assigned to a job can complete it. Completed jobs cannot be
completed again.

### Inspect a job

```http
GET /jobs/{job_id}
```

Returns the job state, assignment, result, signature, timestamps, and receipts.

### Audit events

```http
GET /events?limit=100
```

Returns the newest audit events first. Events are recorded for node registration
or update, job creation, job assignment, and receipt submission.

## Suggested worker payloads

The coordinator accepts a JSON object payload for every allowlisted kind so
clients can evolve without coordinator-side code execution. A compatible worker
can use these conventions:

- `echo`: return the payload as the result.
- `sha256`: hash the UTF-8 bytes in `payload.data`.
- `sleep`: wait up to a locally enforced safe maximum using `payload.seconds`.
- `checksum`: compute a checksum over `payload.data` or a bounded data block.

Workers should enforce their own resource limits before processing a job.
