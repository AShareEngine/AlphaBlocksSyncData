#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FastAPI service for sync job management."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from sync_data_system.clickhouse_client import ClickHouseConfig, create_clickhouse_client
from sync_data_system.service.job_manager import SyncJobManager
from sync_data_system.wide_table_sync import (
    WideTableSyncStateRepository,
    build_wide_table_metadata,
    run_wide_table_sync_payloads_with_clickhouse,
    wide_table_state_to_dict,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
JOB_MANAGER = SyncJobManager(PROJECT_ROOT)
app = FastAPI(title="AmazingData Sync Service", version="0.1.0")

DATE_FIELD_CANDIDATES = (
    "trade_time",
    "trade_date",
    "ann_date",
    "end_date",
    "report_date",
    "change_date",
    "list_date",
    "in_date",
    "out_date",
    "date",
)

CONFIG_FILE_RE = re.compile(r"^run_sync.*\.toml$", re.IGNORECASE)

DATE_FIELD_CANDIDATES = (
    "trade_time",
    "trade_date",
    "ann_date",
    "end_date",
    "report_date",
    "change_date",
    "list_date",
    "in_date",
    "out_date",
    "date",
)


def _job_error_to_http(exc: Exception) -> HTTPException:
    message = str(exc)
    if "another sync job is running" in message:
        return HTTPException(status_code=409, detail=message)
    if isinstance(exc, (FileNotFoundError, ValueError)):
        return HTTPException(status_code=400, detail=message)
    return HTTPException(status_code=400, detail=message)


class RunConfigRequest(BaseModel):
    config: str = Field(..., description="workspace-relative config path")
    log_level: Optional[str] = None
    runtime_path: Optional[str] = None


class SyncConfigWriteRequest(BaseModel):
    name: str
    content: str = ""


class RunTaskRequest(BaseModel):
    name: Optional[str] = None
    task: Optional[str] = None
    codes: list[str] = Field(default_factory=list)
    day: Optional[int] = None
    begin_date: Optional[int] = None
    end_date: Optional[int] = None
    year: Optional[int] = None
    quarter: Optional[int] = None
    year_type: Optional[str] = None
    limit: int = 0
    force: bool = False
    resume: bool = False
    adjustflag: Optional[str] = None
    frequency: Optional[str] = None
    log_level: Optional[str] = None
    runtime_path: Optional[str] = None

    def resolved_name(self) -> str:
        task_name = (self.name or self.task or "").strip()
        if not task_name:
            raise ValueError("name 不能为空。")
        return task_name


class WideTableInlineRunRequest(BaseModel):
    id: str
    graph_path: Optional[str] = None
    fields_path: Optional[str] = None
    state_database: Optional[str] = None
    runtime_path: Optional[str] = None


@app.get("/health")
@app.get("/api/sync/health")
def health():
    return {"status": "ok"}


def _resolve_sync_config_path(name: str) -> Path:
    clean_name = Path(str(name or "")).name
    if not clean_name or not CONFIG_FILE_RE.match(clean_name):
        raise HTTPException(status_code=400, detail="config name must match run_sync*.toml")
    return JOB_MANAGER.config_root / clean_name


@app.get("/api/sync-configs")
@app.get("/api/sync/configs")
def sync_configs():
    return {"items": JOB_MANAGER.list_configs()}


@app.post("/api/sync-configs")
@app.post("/api/sync/configs")
def save_sync_config(request: SyncConfigWriteRequest):
    file_path = _resolve_sync_config_path(request.name)
    JOB_MANAGER.config_root.mkdir(parents=True, exist_ok=True)
    file_path.write_text(request.content or "", encoding="utf-8")
    return {"ok": True, "name": file_path.name}


@app.get("/api/sync-configs/{name:path}")
@app.get("/api/sync/configs/{name:path}")
def get_sync_config(name: str):
    file_path = _resolve_sync_config_path(name)
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="sync config not found")
    return {"name": file_path.name, "content": file_path.read_text(encoding="utf-8")}


@app.delete("/api/sync-configs/{name:path}")
@app.delete("/api/sync/configs/{name:path}")
def delete_sync_config(name: str):
    file_path = _resolve_sync_config_path(name)
    if not file_path.is_file():
        raise HTTPException(status_code=404, detail="sync config not found")
    file_path.unlink()
    return {"ok": True, "name": file_path.name}

@app.get("/api/sync/meta/tasks")
@app.get("/api/meta/tasks")
def list_tasks():
    return {
        "tasks": JOB_MANAGER.list_tasks(),
        "registered_tasks": JOB_MANAGER.list_registered_tasks(),
    }


@app.get("/api/sync/meta/tasks/{task_name}")
@app.get("/api/meta/tasks/{task_name}")
def get_task_metadata(task_name: str):
    try:
        items = {item["name"]: item for item in JOB_MANAGER.list_registered_tasks()}
        if task_name in items:
            return items[task_name]
        if task_name in JOB_MANAGER.list_tasks():
            return {
                "name": task_name,
                "source": None,
                "target": None,
                "input_resolver": None,
                "request_fields": ["name", "codes", "begin_date", "end_date", "limit", "force", "resume", "log_level"],
                "probe_fields": [],
            }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    raise HTTPException(status_code=404, detail="task not found")


@app.get("/api/sync/meta/configs")
@app.get("/api/meta/configs")
def list_configs():
    return {"configs": JOB_MANAGER.list_configs()}


@app.get("/api/sync-table-status")
def sync_table_status(runtime_path: Optional[str] = Query(None)):
    try:
        task_items = JOB_MANAGER.list_registered_tasks()
        targets = sorted(
            {
                str(item.get("target") or "").strip()
                for item in task_items
                if str(item.get("target") or "").strip()
            }
        )
        if not targets:
            return {"items": []}

        config = ClickHouseConfig.from_env(runtime_path=runtime_path)
        connection = create_clickhouse_client(config)
        try:
            table_rows = connection.query_rows(
                """
                SELECT database, name
                FROM system.tables
                WHERE name IN {targets:Array(String)}
                ORDER BY database, name
                """,
                {"targets": targets},
            )
            target_lookup: dict[str, tuple[str, str]] = {}
            for row in table_rows:
                if len(row) < 2:
                    continue
                database = str(row[0])
                table = str(row[1])
                target_lookup.setdefault(table, (database, table))

            items: list[dict[str, Any]] = []
            resolved_targets = [(database, table) for _, (database, table) in target_lookup.items()]
            columns_by_table: dict[tuple[str, str], list[str]] = {}
            parts_by_table: dict[tuple[str, str], tuple[Any, ...]] = {}
            if resolved_targets:
                dbs = sorted({database for database, _ in resolved_targets})
                table_names = sorted({table for _, table in resolved_targets})
                column_rows = connection.query_rows(
                    """
                    SELECT database, table, name
                    FROM system.columns
                    WHERE database IN {databases:Array(String)}
                      AND table IN {tables:Array(String)}
                    ORDER BY database, table, position
                    """,
                    {"databases": dbs, "tables": table_names},
                )
                for row in column_rows:
                    if len(row) < 3:
                        continue
                    key = (str(row[0]), str(row[1]))
                    columns_by_table.setdefault(key, []).append(str(row[2]))

                part_rows = connection.query_rows(
                    """
                    SELECT
                      database,
                      table,
                      sum(rows) AS row_count,
                      max(modification_time) AS last_update_time
                    FROM system.parts
                    WHERE active = 1
                      AND database IN {databases:Array(String)}
                      AND table IN {tables:Array(String)}
                    GROUP BY database, table
                    """,
                    {"databases": dbs, "tables": table_names},
                )
                parts_by_table = {
                    (str(row[0]), str(row[1])): row
                    for row in part_rows
                    if len(row) >= 4
                }

            for target in targets:
                task_names = [str(item.get("name") or "") for item in task_items if str(item.get("target") or "").strip() == target]
                if target not in target_lookup:
                    items.append(
                        {
                            "target": target,
                            "database": "",
                            "latest_date": "",
                            "row_count": 0,
                            "last_update_time": "",
                            "status": "missing",
                            "tasks": task_names,
                        }
                    )
                    continue

                database, table = target_lookup[target]
                columns = columns_by_table.get((database, table), [])
                latest_field = next((field for field in DATE_FIELD_CANDIDATES if field in columns), None)
                latest_date = ""
                if latest_field:
                    latest_value = connection.query_value(f"SELECT toString(max({latest_field})) FROM {database}.{table}")
                    latest_date = str(latest_value or "")

                part_row = parts_by_table.get((database, table))
                row_count = int(part_row[2]) if part_row else 0
                last_update_time = str(part_row[3]) if part_row and part_row[3] is not None else ""

                items.append(
                    {
                        "target": target,
                        "database": database,
                        "latest_date": latest_date,
                        "row_count": row_count,
                        "last_update_time": last_update_time,
                        "status": "ready" if latest_date else "warning",
                        "tasks": task_names,
                    }
                )
        finally:
            connection.close()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"items": items}


@app.get("/api/sync/wide-tables/states")
def list_wide_table_states(
    state_database: Optional[str] = Query(None),
    runtime_path: Optional[str] = Query(None),
):
    try:
        config = ClickHouseConfig.from_env(runtime_path=runtime_path)
        connection = create_clickhouse_client(config)
        repository = WideTableSyncStateRepository(
            connection,
            database=state_database or config.runtime_state_database,
        )
        repository.ensure_table()
        try:
            states = repository.load_states()
        finally:
            connection.close()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"states": [wide_table_state_to_dict(state) for state in states]}


@app.get("/api/sync/wide-tables/states/{wide_table_name}")
def get_wide_table_state(
    wide_table_name: str,
    state_database: Optional[str] = Query(None),
    runtime_path: Optional[str] = Query(None),
):
    try:
        config = ClickHouseConfig.from_env(runtime_path=runtime_path)
        connection = create_clickhouse_client(config)
        repository = WideTableSyncStateRepository(
            connection,
            database=state_database or config.runtime_state_database,
        )
        repository.ensure_table()
        try:
            state = repository.load_state(wide_table_name)
        finally:
            connection.close()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if state is None:
        raise HTTPException(status_code=404, detail="wide table state not found")
    return wide_table_state_to_dict(state)


@app.post("/api/sync/wide-tables/run-inline")
def run_wide_table_inline(request: WideTableInlineRunRequest):
    try:
        from aiquantbase.wide_table import build_wide_table_export_payload

        payload = build_wide_table_export_payload(
            request.id,
            graph_path=request.graph_path,
            fields_path=request.fields_path,
        )
        spec_name = str(((payload.get("wide_table") or {}).get("name")) or request.id).strip() or request.id
        spec_path = f"inline://{spec_name}.yaml"
        metadata = build_wide_table_metadata(payload, spec_path=spec_path)
        results = run_wide_table_sync_payloads_with_clickhouse(
            {spec_path: payload},
            {spec_path: metadata},
            config=ClickHouseConfig.from_env(runtime_path=request.runtime_path),
            state_database=request.state_database,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {
        "ok": all(item.status == "success" for item in results),
        "results": [item.__dict__ for item in results],
    }


@app.get("/api/sync/jobs")
@app.get("/api/jobs")
def list_jobs(
    status: Optional[str] = Query(None),
    task: Optional[str] = Query(None),
    kind: Optional[str] = Query(None),
):
    return {
        "jobs": [job.__dict__ for job in JOB_MANAGER.list_jobs(status=status, task=task, kind=kind)]
    }


@app.get("/api/sync/jobs/{job_id}")
@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, tail_lines: int = Query(100, ge=1, le=2000)):
    try:
        job = JOB_MANAGER.get_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")
    return {
        **job.__dict__,
        "logs_tail": JOB_MANAGER.read_job_log(job_id, tail_lines=tail_lines),
    }


@app.get("/api/sync/jobs/{job_id}/logs")
@app.get("/api/jobs/{job_id}/logs")
def get_job_logs(job_id: str, tail_lines: int = Query(200, ge=1, le=5000)):
    try:
        return {"job_id": job_id, "logs": JOB_MANAGER.read_job_log(job_id, tail_lines=tail_lines)}
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")


@app.post("/api/sync/jobs/run-config")
@app.post("/api/jobs/run-config")
def run_config(request: RunConfigRequest):
    try:
        job = JOB_MANAGER.create_config_job(
            request.config,
            log_level=request.log_level,
            runtime_path=request.runtime_path,
        )
    except Exception as exc:
        raise _job_error_to_http(exc)
    return {
        **job.__dict__,
        "config": request.config,
    }


@app.post("/api/sync/jobs/run-task")
@app.post("/api/jobs/run-task")
def run_task(request: RunTaskRequest):
    try:
        task_name = request.resolved_name()
        registered_tasks = {item["name"]: item for item in JOB_MANAGER.list_registered_tasks()}
        if task_name in registered_tasks:
            job = JOB_MANAGER.create_registered_task_job(
                task=task_name,
                codes=request.codes,
                day=request.day,
                begin_date=request.begin_date,
                end_date=request.end_date,
                year=request.year,
                quarter=request.quarter,
                year_type=request.year_type,
                limit=request.limit,
                force=request.force,
                resume=request.resume,
                adjustflag=request.adjustflag,
                frequency=request.frequency,
                log_level=request.log_level,
                runtime_path=request.runtime_path,
            )
            task_metadata = registered_tasks[task_name]
        else:
            job = JOB_MANAGER.create_task_job(
                task=task_name,
                codes=request.codes,
                begin_date=request.begin_date,
                end_date=request.end_date,
                limit=request.limit,
                force=request.force,
                resume=request.resume,
                log_level=request.log_level,
                runtime_path=request.runtime_path,
            )
            task_metadata = None
    except Exception as exc:
        raise _job_error_to_http(exc)
    return {
        **job.__dict__,
        "task_metadata": task_metadata,
    }


@app.post("/api/sync/jobs/{job_id}/cancel")
@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    try:
        job = JOB_MANAGER.cancel_job(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")
    return job.__dict__
