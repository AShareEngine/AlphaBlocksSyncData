# Sync API Service

## 1. Overview

This service wraps the existing sync entrypoints and exposes them through HTTP.

Current capabilities:

- start a config-based sync job
- start a single task sync job
- list tasks and configs
- inspect job status
- inspect recent logs
- cancel a running job

Current constraints:

- only one sync job can run at a time
- if a job is already running, starting another one returns `409 Conflict`

Base service files:

- `service/api.py`
- `service/job_manager.py`
- `service/task_registry.py`
- `scripts/run_api_service.py`

## 2. Startup

Install dependencies:

```bash
pip install -r requirements.txt
```

Start service:

```bash
python3 scripts/run_api_service.py
```

Default bind:

- host: `0.0.0.0`
- port: `18080`

OpenAPI / Swagger:

- `GET /docs`
- `GET /openapi.json`

## 3. Authentication

Current version has no authentication layer.

If this service is exposed outside a trusted internal network, add auth before production use.

## 4. Task Model

Tasks are identified by `name`.

Examples:

- `daily_kline`
- `minute_kline`
- `option_basic_info`
- `treasury_yield`
- `margin_detail`

Registered task metadata contains:

- `name`
- `source`
- `target`
- `input_resolver`
- `request_fields`
- `probe_fields`

## 5. Job Model

Every started sync creates a job record.

Job fields:

- `job_id`
- `kind`
  - `config`
  - `task`
  - `registered_task`
- `status`
  - `running`
  - `cancelling`
  - `cancelled`
  - `success`
  - `failed`
  - `interrupted`
- `created_at`
- `started_at`
- `finished_at`
- `cwd`
- `command`
- `log_path`
- `config_path`
- `task`
- `source`
- `target`
- `pid`
- `return_code`
- `error`

## 6. API Endpoints

### 6.1 Health

`GET /health`

Response:

```json
{
  "status": "ok"
}
```

### 6.2 List Tasks

`GET /api/meta/tasks`

Response:

```json
{
  "tasks": ["daily_kline", "minute_kline", "..."],
  "registered_tasks": [
    {
      "name": "daily_kline",
      "source": "amazingdata",
      "target": "ad_market_kline_daily",
      "input_resolver": "market_kline_defaults",
      "request_fields": [
        "name",
        "codes",
        "begin_date",
        "end_date",
        "limit",
        "force",
        "resume",
        "log_level"
      ],
      "probe_fields": [
        "name",
        "source",
        "target",
        "job_id",
        "runtime_path",
        "input_codes",
        "input_begin_date",
        "input_end_date",
        "limit",
        "force",
        "resume",
        "log_level",
        "codes",
        "begin_date",
        "end_date",
        "row_count",
        "status",
        "message",
        "log_path"
      ]
    }
  ]
}
```

### 6.3 Get Task Metadata

`GET /api/meta/tasks/{task_name}`

Example:

```bash
curl http://127.0.0.1:18080/api/meta/tasks/daily_kline
```

Response:

```json
{
  "name": "daily_kline",
  "source": "amazingdata",
  "target": "ad_market_kline_daily",
  "input_resolver": "market_kline_defaults",
  "request_fields": [
    "name",
    "codes",
    "begin_date",
    "end_date",
    "limit",
    "force",
    "resume",
    "log_level"
  ],
  "probe_fields": [
    "name",
    "source",
    "target",
    "job_id",
    "runtime_path",
    "input_codes",
    "input_begin_date",
    "input_end_date",
    "limit",
    "force",
    "resume",
    "log_level",
    "codes",
    "begin_date",
    "end_date",
    "row_count",
    "status",
    "message",
    "log_path"
  ]
}
```

### 6.4 List Configs

`GET /api/meta/configs`

Response:

```json
{
  "configs": [
    "run_sync.full.toml",
    "run_sync.amazingdata.full.toml",
    "run_sync.baostock.full.toml"
  ]
}
```

### 6.5 Start Config Job

`POST /api/jobs/run-config`

Request body:

```json
{
  "config": "run_sync.full.toml",
  "log_level": "INFO"
}
```

Example:

```bash
curl -X POST http://127.0.0.1:18080/api/jobs/run-config \
  -H 'Content-Type: application/json' \
  -d '{"config":"run_sync.full.toml","log_level":"INFO"}'
```

Success response:

```json
{
  "job_id": "abc123def456",
  "kind": "config",
  "status": "running",
  "created_at": "2026-04-21T09:00:00+00:00",
  "started_at": "2026-04-21T09:00:00+00:00",
  "finished_at": null,
  "cwd": "/path/to/project",
  "command": ["python3", "/path/to/project/run_sync.py", "--config", "/path/to/project/run_sync.full.toml"],
  "log_path": "/path/to/project/.service_state/logs/abc123def456.log",
  "config_path": "run_sync.full.toml",
  "task": null,
  "source": null,
  "target": null,
  "pid": 12345,
  "return_code": null,
  "error": null
}
```

Conflict response:

```json
{
  "detail": "another sync job is running job_id=... task=...; cancel it first"
}
```

HTTP status:

- `200` success
- `400` invalid config / bad input
- `409` another sync job is already running

### 6.6 Start Task Job

`POST /api/jobs/run-task`

Request body:

- `name` is recommended
- `task` is still accepted as compatibility alias
- `codes` should be `list[str]`

Request example:

```json
{
  "name": "daily_kline",
  "codes": ["000001.SZ", "510300.SH"],
  "begin_date": 20240101,
  "end_date": 20240131,
  "limit": 20,
  "force": false,
  "resume": false,
  "log_level": "INFO"
}
```

Example:

```bash
curl -X POST http://127.0.0.1:18080/api/jobs/run-task \
  -H 'Content-Type: application/json' \
  -d '{"name":"daily_kline","codes":["000001.SZ"],"begin_date":20240101,"end_date":20240131,"limit":20}'
```

Success response:

```json
{
  "job_id": "abc123def456",
  "kind": "registered_task",
  "status": "running",
  "created_at": "2026-04-21T09:05:00+00:00",
  "started_at": "2026-04-21T09:05:00+00:00",
  "finished_at": null,
  "cwd": "/path/to/project",
  "command": ["python3", "/path/to/project/scripts/run_registered_task.py", "..."],
  "log_path": "/path/to/project/.service_state/logs/abc123def456.log",
  "config_path": null,
  "task": "daily_kline",
  "source": "amazingdata",
  "target": "ad_market_kline_daily",
  "pid": 12346,
  "return_code": null,
  "error": null
}
```

HTTP status:

- `200` success
- `400` invalid request
- `409` another sync job is already running

### 6.7 List Jobs

`GET /api/jobs`

Optional query params:

- `status`
- `task`
- `kind`

Examples:

```bash
curl http://127.0.0.1:18080/api/jobs
curl http://127.0.0.1:18080/api/jobs?status=running
curl http://127.0.0.1:18080/api/jobs?task=daily_kline
curl http://127.0.0.1:18080/api/jobs?kind=registered_task
```

Response:

```json
{
  "jobs": [
    {
      "job_id": "abc123def456",
      "kind": "registered_task",
      "status": "running",
      "created_at": "2026-04-21T09:05:00+00:00",
      "started_at": "2026-04-21T09:05:00+00:00",
      "finished_at": null,
      "cwd": "/path/to/project",
      "command": ["python3", "..."],
      "log_path": "/path/to/project/.service_state/logs/abc123def456.log",
      "config_path": null,
      "task": "daily_kline",
      "source": "amazingdata",
      "target": "ad_market_kline_daily",
      "pid": 12346,
      "return_code": null,
      "error": null
    }
  ]
}
```

### 6.8 Get Job Detail

`GET /api/jobs/{job_id}`

Optional query param:

- `tail_lines`

Example:

```bash
curl http://127.0.0.1:18080/api/jobs/abc123def456
```

Response:

```json
{
  "job_id": "abc123def456",
  "kind": "registered_task",
  "status": "running",
  "created_at": "2026-04-21T09:05:00+00:00",
  "started_at": "2026-04-21T09:05:00+00:00",
  "finished_at": null,
  "cwd": "/path/to/project",
  "command": ["python3", "..."],
  "log_path": "/path/to/project/.service_state/logs/abc123def456.log",
  "config_path": null,
  "task": "daily_kline",
  "source": "amazingdata",
  "target": "ad_market_kline_daily",
  "pid": 12346,
  "return_code": null,
  "error": null,
  "logs_tail": "...\n..."
}
```

### 6.9 Get Job Logs

`GET /api/jobs/{job_id}/logs`

Optional query param:

- `tail_lines`

Response:

```json
{
  "job_id": "abc123def456",
  "logs": "...\n..."
}
```

### 6.10 Cancel Job

`POST /api/jobs/{job_id}/cancel`

Example:

```bash
curl -X POST http://127.0.0.1:18080/api/jobs/abc123def456/cancel
```

Response:

```json
{
  "job_id": "abc123def456",
  "kind": "registered_task",
  "status": "cancelling",
  "created_at": "2026-04-21T09:05:00+00:00",
  "started_at": "2026-04-21T09:05:00+00:00",
  "finished_at": null,
  "cwd": "/path/to/project",
  "command": ["python3", "..."],
  "log_path": "/path/to/project/.service_state/logs/abc123def456.log",
  "config_path": null,
  "task": "daily_kline",
  "source": "amazingdata",
  "target": "ad_market_kline_daily",
  "pid": 12346,
  "return_code": null,
  "error": null
}
```

Final job status after process exit:

- `cancelled`

## 7. Frontend Integration Notes

Recommended frontend flow:

1. call `GET /api/meta/tasks`
2. call `GET /api/meta/tasks/{task_name}` when opening a task form
3. submit `POST /api/jobs/run-task`
4. poll `GET /api/jobs/{job_id}` for status and tail logs
5. use `GET /api/jobs` for task list pages and filtering
6. use `POST /api/jobs/{job_id}/cancel` to stop the running job

Recommended assumptions:

- only one sync job may run at a time
- a `409` means the frontend should block the new submission and prompt the user to cancel or wait
- use `name` instead of `task`
- use `codes: list[str]` instead of comma-separated text

## 8. Current Registered Tasks

AmazingData tasks currently registered in the task registry:

- `50`

Examples:

- `daily_kline`
- `minute_kline`
- `option_basic_info`
- `option_std_ctr_specs`
- `option_mon_ctr_specs`
- `treasury_yield`
- `margin_detail`
- `margin_summary`
- `block_trading`
- `long_hu_bang`

Use `GET /api/meta/tasks` as the source of truth for the full current list.
