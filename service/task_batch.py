#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Sequential execution for cross-provider sync task batches."""

from __future__ import annotations

import argparse
import json
import re
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sync_data_system.clickhouse_client import ClickHouseConfig, create_clickhouse_client
from sync_data_system.scripts.run_provider_sync import run_registered_task
from sync_data_system.service.task_registry import TASK_REGISTRY


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
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def expected_business_date(now: datetime | None = None) -> date:
    local_now = (now or datetime.now(timezone.utc)).astimezone(ZoneInfo("Asia/Shanghai"))
    candidate = local_now.date()
    after_cutoff = (local_now.hour, local_now.minute) >= (18, 30)
    if after_cutoff and candidate.weekday() < 5:
        return candidate
    candidate -= timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def next_business_day(value: date) -> date:
    candidate = value + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def run_task_batch(payload: dict[str, Any], *, results_path: Path, log_path: Path) -> int:
    job_id = str(payload.get("job_id") or "batch").strip() or "batch"
    continue_on_error = bool(payload.get("continue_on_error", True))
    runtime_path = str(payload.get("runtime_path") or "").strip() or None
    log_level = str(payload.get("log_level") or "INFO").strip() or "INFO"
    tasks = list(payload.get("tasks") or [])
    results: list[dict[str, Any]] = []
    clickhouse = None
    failed = 0
    completed = 0

    _append_log(log_path, f"batch={job_id} status=started task_count={len(tasks)}")
    try:
        for index, task in enumerate(tasks, start=1):
            result = {
                "task_id": str(task.get("id") or f"task_{index}"),
                "name": str(task.get("name") or ""),
                "provider": str(task.get("provider") or ""),
                "database": str(task.get("database") or ""),
                "target": str(task.get("target") or ""),
                "status": "pending",
                "started_at": None,
                "finished_at": None,
                "error": None,
                "effective_parameters": {},
            }
            if not task.get("enabled", True):
                result["status"] = "disabled"
                results.append(result)
                _write_results(results_path, job_id, results, status="running")
                continue

            result["started_at"] = utc_now_iso()
            _append_log(
                log_path,
                f"batch={job_id} task={result['name']} progress={index}/{len(tasks)} status=started",
            )
            try:
                metadata = TASK_REGISTRY.get_task_metadata(result["name"])
                parameters = deepcopy(task.get("parameters") or {})
                if str(task.get("date_mode") or "provider_default") == "incremental":
                    if clickhouse is None:
                        clickhouse = create_clickhouse_client(ClickHouseConfig.from_env(runtime_path=runtime_path))
                    incremental = _resolve_incremental_parameters(metadata, parameters, clickhouse)
                    if incremental is None:
                        result["status"] = "skipped"
                        result["finished_at"] = utc_now_iso()
                        completed += 1
                        results.append(result)
                        _append_log(log_path, f"batch={job_id} task={result['name']} status=skipped reason=up_to_date")
                        _write_results(results_path, job_id, results, status="running")
                        continue
                    parameters = incremental
                result["effective_parameters"] = deepcopy(parameters)
                args = _task_namespace(
                    task_name=result["name"],
                    job_id=f"{job_id}:{index}",
                    log_path=log_path,
                    runtime_path=runtime_path,
                    log_level=log_level,
                    parameters=parameters,
                )
                return_code = run_registered_task(args)
                if return_code:
                    raise RuntimeError(f"task returned non-zero status: {return_code}")
                result["status"] = "success"
                completed += 1
                _append_log(log_path, f"batch={job_id} task={result['name']} status=success")
            except Exception as exc:
                failed += 1
                result["status"] = "failed"
                result["error"] = str(exc)
                _append_log(log_path, f"batch={job_id} task={result['name']} status=failed error={exc}")
            finally:
                result["finished_at"] = result["finished_at"] or utc_now_iso()
                if result not in results:
                    results.append(result)
                _write_results(results_path, job_id, results, status="running")
            if result["status"] == "failed" and not continue_on_error:
                break
    finally:
        if clickhouse is not None:
            clickhouse.close()

    status = "success"
    return_code = 0
    if failed:
        status = "partial_success" if completed else "failed"
        return_code = 2 if completed else 1
    _write_results(results_path, job_id, results, status=status)
    _append_log(log_path, f"batch={job_id} status={status} completed={completed} failed={failed}")
    return return_code


def _resolve_incremental_parameters(
    metadata: dict[str, Any],
    parameters: dict[str, Any],
    connection: Any,
) -> dict[str, Any] | None:
    request_fields = set(metadata.get("request_fields") or [])
    if "incrementally" in request_fields:
        parameters["incrementally"] = True
    if "begin_date" not in request_fields or "end_date" not in request_fields:
        return parameters

    database = _safe_identifier(metadata.get("database"), "database")
    target = _safe_identifier(metadata.get("target"), "target")
    cursor_field = str(metadata.get("cursor_field") or "").strip()
    columns = connection.query_rows(
        """
        SELECT name
        FROM system.columns
        WHERE database = {database:String} AND table = {table:String}
        """,
        {"database": database, "table": target},
    )
    available = {str(row[0]) for row in columns if row}
    candidates = [cursor_field, *DATE_FIELD_CANDIDATES]
    date_field = next((item for item in candidates if item and item in available), "")
    if not date_field:
        return parameters
    date_field = _safe_identifier(date_field, "date field")
    latest_value = connection.query_value(f"SELECT max(`{date_field}`) FROM `{database}`.`{target}`")
    latest_date = _parse_date(latest_value)
    if latest_date is None:
        return parameters
    expected_date = expected_business_date()
    if latest_date >= expected_date:
        return None
    parameters["begin_date"] = int(next_business_day(latest_date).strftime("%Y%m%d"))
    parameters["end_date"] = int(expected_date.strftime("%Y%m%d"))
    return parameters


def _task_namespace(
    *,
    task_name: str,
    job_id: str,
    log_path: Path,
    runtime_path: str | None,
    log_level: str,
    parameters: dict[str, Any],
) -> argparse.Namespace:
    def scalar(name: str, default: Any = None) -> Any:
        value = parameters.get(name, default)
        return default if value == "" else value

    codes = parameters.get("codes") or []
    if isinstance(codes, str):
        codes_text = codes
    else:
        codes_text = ",".join(str(item).strip() for item in codes if str(item).strip())
    fields = parameters.get("fields")
    if isinstance(fields, list):
        fields = ",".join(str(item).strip() for item in fields if str(item).strip())
    table_names = parameters.get("table_names")
    if isinstance(table_names, list):
        table_names = ",".join(str(item).strip() for item in table_names if str(item).strip())

    return argparse.Namespace(
        task=task_name,
        job_id=job_id,
        log_path=str(log_path),
        runtime_path=runtime_path,
        codes=codes_text,
        day=_optional_int(scalar("day")),
        begin_date=_optional_int(scalar("begin_date")),
        end_date=_optional_int(scalar("end_date")),
        year=_optional_int(scalar("year")),
        quarter=_optional_int(scalar("quarter")),
        year_type=scalar("year_type"),
        market=scalar("market"),
        index_code=scalar("index_code"),
        table_names=table_names,
        sector_name=scalar("sector_name"),
        code_market=scalar("code_market"),
        period=scalar("period"),
        fields=fields,
        adjust_type=scalar("adjust_type"),
        qmt_adjust_type=scalar("qmt_adjust_type") or scalar("adjust_type"),
        fill_data=_optional_bool(scalar("fill_data"), default=True),
        count=_optional_int(scalar("count"), default=-1),
        incrementally=_optional_bool(scalar("incrementally"), default=False),
        complete=_optional_bool(scalar("complete"), default=False),
        limit=_optional_int(scalar("limit"), default=0),
        force=_optional_bool(scalar("force"), default=False),
        resume=_optional_bool(scalar("resume"), default=False),
        adjustflag=str(scalar("adjustflag", "3") or "3"),
        frequency=str(scalar("frequency", "d") or "d"),
        log_level=log_level,
    )


def _write_results(path: Path, job_id: str, tasks: list[dict[str, Any]], *, status: str) -> None:
    payload = {
        "job_id": job_id,
        "status": status,
        "updated_at": utc_now_iso(),
        "tasks": tasks,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(".json.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def _append_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{utc_now_iso()} {message}\n")


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)):
        text = str(int(value))
        if len(text) >= 13:
            try:
                return datetime.fromtimestamp(int(text[:13]) / 1000, tz=timezone.utc).date()
            except (OverflowError, OSError, ValueError):
                return None
    else:
        text = str(value).strip()
    match = re.search(r"(\d{4})[-/]?(\d{2})[-/]?(\d{2})", text)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def _safe_identifier(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not IDENTIFIER_RE.match(text):
        raise ValueError(f"invalid {label}: {text!r}")
    return text


def _optional_int(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    return int(value)


def _optional_bool(value: Any, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in {"0", "false", "no", "off"}


__all__ = ["expected_business_date", "next_business_day", "run_task_batch"]
