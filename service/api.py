#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""FastAPI service for sync job management."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from sync_data_system.clickhouse_client import ClickHouseConfig, create_clickhouse_client
from sync_data_system.config_paths import resolve_runtime_config_path
from sync_data_system.core.providers import load_provider_registry
from sync_data_system.service.job_manager import SyncJobManager
from sync_data_system.toml_compat import tomllib
from sync_data_system.service.schedule_manager import SyncScheduleManager
from sync_data_system.wide_table_sync import (
    WideTableSyncStateRepository,
    build_wide_table_metadata,
    run_wide_table_sync_payloads_with_clickhouse,
    wide_table_state_to_dict,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
JOB_MANAGER = SyncJobManager(PROJECT_ROOT)
SCHEDULE_MANAGER = SyncScheduleManager(PROJECT_ROOT, JOB_MANAGER)
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
PROVIDER_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")
PROVIDER_PACKAGE_KIND = "alphablocks.sync.provider-package"
PROVIDER_CODE_SUFFIXES = {".py"}

PROVIDER_CONFIG_SCHEMAS: dict[str, list[dict[str, Any]]] = {
    "amazingdata": [
        {"key": "username", "label": "账号", "type": "text", "required": True},
        {"key": "password", "label": "密码", "type": "password", "required": True, "sensitive": True},
        {"key": "host", "label": "服务地址", "type": "text", "required": True},
        {"key": "port", "label": "端口", "type": "number", "required": True},
        {"key": "local_path", "label": "本地数据目录", "type": "text", "required": False},
    ],
    "baostock": [
        {"key": "user_id", "label": "用户 ID", "type": "text", "required": False},
        {"key": "password", "label": "密码", "type": "password", "required": False, "sensitive": True},
    ],
    "qmt": [
        {"key": "base_url", "label": "服务地址", "type": "text", "required": True},
        {"key": "api_key", "label": "API Key", "type": "password", "required": False, "sensitive": True},
        {"key": "timeout", "label": "超时秒数", "type": "number", "required": False},
    ],
}

def _job_error_to_http(exc: Exception) -> HTTPException:
    message = str(exc)
    if "another sync job is running" in message:
        return HTTPException(status_code=409, detail=message)
    if isinstance(exc, (FileNotFoundError, ValueError)):
        return HTTPException(status_code=400, detail=message)
    return HTTPException(status_code=400, detail=message)


def _model_to_dict(model: BaseModel, **kwargs) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(**kwargs)
    return model.dict(**kwargs)


class RunConfigRequest(BaseModel):
    config: str = Field(..., description="workspace-relative config path")
    log_level: Optional[str] = None
    runtime_path: Optional[str] = None


class SyncConfigWriteRequest(BaseModel):
    name: str
    content: str = ""


class ProviderConfigUpdateRequest(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    runtime_path: Optional[str] = None


class ProviderConfigImportRequest(BaseModel):
    package: dict[str, Any]
    include_code: bool = False
    overwrite: bool = True
    runtime_path: Optional[str] = None


class RunTaskRequest(BaseModel):
    name: str
    codes: list[str] = Field(default_factory=list)
    day: Optional[int] = None
    begin_date: Optional[int] = None
    end_date: Optional[int] = None
    year: Optional[int] = None
    quarter: Optional[int] = None
    year_type: Optional[str] = None
    market: Optional[str] = None
    index_code: Optional[str] = None
    table_names: Optional[str] = None
    sector_name: Optional[str] = None
    code_market: Optional[str] = None
    period: Optional[str] = None
    fields: Optional[str] = None
    adjust_type: Optional[str] = None
    qmt_adjust_type: Optional[str] = None
    fill_data: Optional[bool] = None
    count: Optional[int] = None
    incrementally: Optional[bool] = None
    complete: Optional[bool] = None
    limit: int = 0
    force: bool = False
    resume: bool = False
    adjustflag: Optional[str] = None
    frequency: Optional[str] = None
    log_level: Optional[str] = None
    runtime_path: Optional[str] = None

    def resolved_name(self) -> str:
        task_name = self.name.strip()
        if not task_name:
            raise ValueError("name 不能为空。")
        return task_name


class ScheduleCreateRequest(BaseModel):
    name: str
    enabled: bool = True
    target_type: str = "config"
    target: str
    frequency: str = "daily"
    time: str = "18:00"
    weekdays: list[str] = Field(default_factory=lambda: ["1", "2", "3", "4", "5"])
    interval_minutes: int = 60
    timezone: str = "Asia/Shanghai"
    log_level: Optional[str] = "INFO"
    concurrency_policy: str = "skip"
    retry_attempts: int = 0


class ScheduleUpdateRequest(BaseModel):
    name: Optional[str] = None
    enabled: Optional[bool] = None
    target_type: Optional[str] = None
    target: Optional[str] = None
    frequency: Optional[str] = None
    time: Optional[str] = None
    weekdays: Optional[list[str]] = None
    interval_minutes: Optional[int] = None
    timezone: Optional[str] = None
    log_level: Optional[str] = None
    concurrency_policy: Optional[str] = None
    retry_attempts: Optional[int] = None


class WideTableInlineRunRequest(BaseModel):
    id: str
    payload: Optional[dict[str, Any]] = None
    nodes_path: Optional[str] = None
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


def _resolve_provider_config_path(runtime_path: Optional[str] = None) -> Path:
    return resolve_runtime_config_path(runtime_path)


def _load_runtime_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    if not path.is_file():
        raise ValueError(f"runtime config path is not a file: {path}")
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"runtime config root must be a mapping: {path}")
    return payload


def _save_runtime_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, allow_unicode=True, sort_keys=False)


def _provider_config_schema(provider: str, values: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    schema = [dict(item) for item in PROVIDER_CONFIG_SCHEMAS.get(provider, [])]
    known_keys = {str(item.get("key") or "") for item in schema}
    for key, value in sorted((values or {}).items()):
        if key in known_keys:
            continue
        schema.append(
            {
                "key": key,
                "label": key,
                "type": "number" if isinstance(value, int) and not isinstance(value, bool) else "text",
                "required": False,
                "sensitive": key.lower() in {"password", "token", "api_key", "secret"},
            }
        )
    return schema


def _is_empty_provider_config_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    return False


def _coerce_provider_config_value(field: dict[str, Any], value: Any) -> Any:
    field_type = str(field.get("type") or "text")
    if field_type == "number":
        if value is None or str(value).strip() == "":
            return 0
        return int(value)
    if field_type == "boolean":
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "on", "开启"}
    return "" if value is None else str(value)


def _provider_config_payload(provider: str, runtime_path: Optional[str] = None) -> dict[str, Any]:
    registry = load_provider_registry(PROJECT_ROOT)
    try:
        manifest = registry.get(provider)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown provider: {provider}")

    config_path = _resolve_provider_config_path(runtime_path)
    payload = _load_runtime_payload(config_path)
    sync_payload = payload.get("sync") if isinstance(payload.get("sync"), dict) else {}
    config_key = manifest.runtime_config_key or manifest.name
    values = sync_payload.get(config_key) if isinstance(sync_payload.get(config_key), dict) else {}
    values = dict(values or {})
    fields = _provider_config_schema(manifest.name, values)
    missing_required = [
        str(field.get("key") or "")
        for field in fields
        if field.get("required") and _is_empty_provider_config_value(values.get(str(field.get("key") or "")))
    ]
    return {
        "provider": manifest.name,
        "display_name": manifest.display_name,
        "runtime_config_key": config_key,
        "default_database": manifest.default_database,
        "config_path": str(config_path),
        "configured": not missing_required,
        "missing_required": missing_required,
        "fields": fields,
        "values": values,
    }


def _update_provider_config(provider: str, values: dict[str, Any], runtime_path: Optional[str] = None) -> dict[str, Any]:
    registry = load_provider_registry(PROJECT_ROOT)
    try:
        manifest = registry.get(provider)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown provider: {provider}")

    config_path = _resolve_provider_config_path(runtime_path)
    payload = _load_runtime_payload(config_path)
    sync_payload = payload.setdefault("sync", {})
    if not isinstance(sync_payload, dict):
        raise ValueError("runtime config field sync must be a mapping")

    config_key = manifest.runtime_config_key or manifest.name
    current_values = sync_payload.get(config_key) if isinstance(sync_payload.get(config_key), dict) else {}
    current_values = dict(current_values or {})
    fields = _provider_config_schema(manifest.name, {**current_values, **(values or {})})
    field_by_key = {str(field.get("key") or ""): field for field in fields}

    next_values = dict(current_values)
    for key, value in (values or {}).items():
        clean_key = str(key or "").strip()
        if not clean_key:
            continue
        next_values[clean_key] = _coerce_provider_config_value(field_by_key.get(clean_key, {"type": "text"}), value)

    sync_payload[config_key] = next_values
    _save_runtime_payload(config_path, payload)
    return _provider_config_payload(manifest.name, runtime_path)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _clean_provider_name(provider: str) -> str:
    provider_name = str(provider or "").strip()
    if not provider_name or not PROVIDER_NAME_RE.match(provider_name):
        raise HTTPException(status_code=400, detail="invalid provider name")
    return provider_name


def _read_text_package_file(path: Path) -> dict[str, str]:
    return {
        "path": path.resolve().relative_to(PROJECT_ROOT).as_posix(),
        "content": path.read_text(encoding="utf-8"),
    }


def _provider_code_files(provider_root: Path) -> list[dict[str, str]]:
    files: list[dict[str, str]] = []
    if not provider_root.exists():
        return files
    for path in sorted(provider_root.rglob("*")):
        if not path.is_file():
            continue
        if "__pycache__" in path.parts or path.suffix not in PROVIDER_CODE_SUFFIXES:
            continue
        files.append(_read_text_package_file(path))
    return files


def _provider_plan_files(provider_root: Path, plans_path: Optional[Path]) -> list[dict[str, str]]:
    root = plans_path or (provider_root / "plans")
    if not root.exists():
        return []
    return [_read_text_package_file(path) for path in sorted(root.glob("*.toml")) if path.is_file()]


def _sync_config_files_for_provider(provider: str) -> list[dict[str, str]]:
    files: list[dict[str, str]] = []
    if not JOB_MANAGER.config_root.exists():
        return files
    for path in sorted(JOB_MANAGER.config_root.glob("run_sync*.toml")):
        if not path.is_file():
            continue
        try:
            with path.open("rb") as handle:
                payload = tomllib.load(handle)
        except Exception:
            continue
        if str(payload.get("source") or "").strip() == provider:
            files.append(_read_text_package_file(path))
    return files


def _provider_export_package(provider: str, include_code: bool = False, runtime_path: Optional[str] = None) -> dict[str, Any]:
    provider_name = _clean_provider_name(provider)
    registry = load_provider_registry(PROJECT_ROOT)
    try:
        manifest = registry.get(provider_name)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"unknown provider: {provider_name}")

    config = _provider_config_payload(manifest.name, runtime_path)
    provider_root = manifest.root.resolve()
    manifest_file = provider_root / "provider.toml"
    package: dict[str, Any] = {
        "kind": PROVIDER_PACKAGE_KIND,
        "version": 1,
        "exported_at": _utc_now_iso(),
        "provider": manifest.name,
        "display_name": manifest.display_name,
        "runtime_config_key": manifest.runtime_config_key or manifest.name,
        "default_database": manifest.default_database,
        "include_code": include_code,
        "sections": {
            "runtime": {
                "config_path": config["config_path"],
                "configured": config["configured"],
                "values": config["values"],
            },
            "provider_manifest": _read_text_package_file(manifest_file) if manifest_file.is_file() else None,
            "provider_plans": _provider_plan_files(provider_root, manifest.plans_path),
            "sync_configs": _sync_config_files_for_provider(manifest.name),
            "code_files": _provider_code_files(provider_root) if include_code else [],
        },
    }
    return package


def _safe_package_path(raw_path: str, *, provider: str, section: str) -> Path:
    clean_path = str(raw_path or "").strip()
    if not clean_path or Path(clean_path).is_absolute():
        raise ValueError(f"{section} contains invalid path: {raw_path!r}")
    path = (PROJECT_ROOT / clean_path).resolve()
    provider_root = (PROJECT_ROOT / "providers" / provider).resolve()
    sync_root = JOB_MANAGER.config_root.resolve()
    project_root = PROJECT_ROOT.resolve()
    try:
        relative = path.relative_to(project_root)
    except ValueError:
        raise ValueError(f"{section} path escapes project root: {raw_path}")
    if "__pycache__" in relative.parts:
        raise ValueError(f"{section} path is not allowed: {raw_path}")
    if section in {"provider_manifest", "provider_plans", "code_files"}:
        path.relative_to(provider_root)
    elif section == "sync_configs":
        path.relative_to(sync_root)
        if not CONFIG_FILE_RE.match(path.name):
            raise ValueError(f"sync config name must match run_sync*.toml: {raw_path}")
    else:
        raise ValueError(f"unknown import section: {section}")
    if section == "code_files" and path.suffix not in PROVIDER_CODE_SUFFIXES:
        raise ValueError(f"code file suffix is not allowed: {raw_path}")
    return path


def _write_package_file(file_payload: dict[str, Any], *, provider: str, section: str, overwrite: bool) -> str:
    if not isinstance(file_payload, dict):
        raise ValueError(f"{section} item must be an object")
    target = _safe_package_path(str(file_payload.get("path") or ""), provider=provider, section=section)
    if target.exists() and not overwrite:
        return target.relative_to(PROJECT_ROOT).as_posix()
    content = file_payload.get("content")
    if not isinstance(content, str):
        raise ValueError(f"{section} item content must be a string: {file_payload.get('path')}")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    return target.relative_to(PROJECT_ROOT).as_posix()


def _import_provider_package(
    package: dict[str, Any],
    *,
    include_code: bool = False,
    overwrite: bool = True,
    runtime_path: Optional[str] = None,
) -> dict[str, Any]:
    if not isinstance(package, dict) or package.get("kind") != PROVIDER_PACKAGE_KIND:
        raise ValueError("not an AlphaBlocks provider sync package")
    provider = _clean_provider_name(str(package.get("provider") or ""))
    sections = package.get("sections")
    if not isinstance(sections, dict):
        raise ValueError("provider package sections must be an object")

    imported_files: list[str] = []
    manifest_payload = sections.get("provider_manifest")
    if isinstance(manifest_payload, dict):
        imported_files.append(
            _write_package_file(
                manifest_payload,
                provider=provider,
                section="provider_manifest",
                overwrite=overwrite,
            )
        )

    for item in sections.get("provider_plans") or []:
        imported_files.append(
            _write_package_file(item, provider=provider, section="provider_plans", overwrite=overwrite)
        )

    for item in sections.get("sync_configs") or []:
        imported_files.append(
            _write_package_file(item, provider=provider, section="sync_configs", overwrite=overwrite)
        )

    imported_code_files: list[str] = []
    if include_code:
        for item in sections.get("code_files") or []:
            imported_code_files.append(
                _write_package_file(item, provider=provider, section="code_files", overwrite=overwrite)
            )
        imported_files.extend(imported_code_files)

    runtime_section = sections.get("runtime") if isinstance(sections.get("runtime"), dict) else {}
    runtime_values = runtime_section.get("values") if isinstance(runtime_section.get("values"), dict) else {}
    provider_config = None
    if runtime_values:
        provider_config = _update_provider_config(provider, runtime_values, runtime_path)

    return {
        "provider": provider,
        "imported_files": imported_files,
        "imported_code_files": imported_code_files,
        "provider_config": provider_config or _provider_config_payload(provider, runtime_path),
    }


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
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    raise HTTPException(status_code=404, detail="task not found")


@app.get("/api/sync/meta/configs")
@app.get("/api/meta/configs")
def list_configs():
    return {"configs": JOB_MANAGER.list_configs()}


@app.get("/api/sync/meta/providers")
@app.get("/api/meta/providers")
def list_providers():
    try:
        return {"providers": JOB_MANAGER.list_providers()}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/sync/provider-configs")
@app.get("/api/provider-configs")
def list_provider_configs(runtime_path: Optional[str] = Query(None)):
    try:
        providers = [item["name"] for item in JOB_MANAGER.list_providers()]
        return {"providers": [_provider_config_payload(provider, runtime_path) for provider in providers]}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/sync/provider-configs/{provider}")
@app.get("/api/provider-configs/{provider}")
def get_provider_config(provider: str, runtime_path: Optional[str] = Query(None)):
    try:
        return _provider_config_payload(provider, runtime_path)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.patch("/api/sync/provider-configs/{provider}")
@app.patch("/api/provider-configs/{provider}")
def update_provider_config(
    provider: str,
    request: ProviderConfigUpdateRequest,
    runtime_path: Optional[str] = Query(None),
):
    try:
        provider_config = _update_provider_config(provider, request.values, runtime_path or request.runtime_path)
        return {"ok": True, "provider_config": provider_config}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/sync/provider-configs/{provider}/export")
@app.get("/api/provider-configs/{provider}/export")
def export_provider_config_package(
    provider: str,
    include_code: bool = Query(False),
    runtime_path: Optional[str] = Query(None),
):
    try:
        return _provider_export_package(provider, include_code=include_code, runtime_path=runtime_path)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/sync/provider-configs/import")
@app.post("/api/provider-configs/import")
def import_provider_config_package(request: ProviderConfigImportRequest):
    try:
        result = _import_provider_package(
            request.package,
            include_code=request.include_code,
            overwrite=request.overwrite,
            runtime_path=request.runtime_path,
        )
        return {"ok": True, **result}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.get("/api/sync/schedules")
@app.get("/api/schedules")
def list_schedules(
    enabled: Optional[bool] = Query(None),
    target_type: Optional[str] = Query(None),
):
    try:
        return {
            "schedules": [
                schedule.__dict__
                for schedule in SCHEDULE_MANAGER.list_schedules(
                    enabled=enabled,
                    target_type=target_type,
                )
            ]
        }
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/api/sync/schedules")
@app.post("/api/schedules")
def create_schedule(request: ScheduleCreateRequest):
    try:
        schedule = SCHEDULE_MANAGER.create_schedule(_model_to_dict(request))
    except Exception as exc:
        raise _job_error_to_http(exc)
    return {"ok": True, "schedule": schedule.__dict__}


@app.get("/api/sync/schedules/{schedule_id}")
@app.get("/api/schedules/{schedule_id}")
def get_schedule(schedule_id: str):
    try:
        schedule = SCHEDULE_MANAGER.get_schedule(schedule_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="schedule not found")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return schedule.__dict__


@app.patch("/api/sync/schedules/{schedule_id}")
@app.patch("/api/schedules/{schedule_id}")
def update_schedule(schedule_id: str, request: ScheduleUpdateRequest):
    try:
        schedule = SCHEDULE_MANAGER.update_schedule(schedule_id, _model_to_dict(request, exclude_unset=True))
    except KeyError:
        raise HTTPException(status_code=404, detail="schedule not found")
    except Exception as exc:
        raise _job_error_to_http(exc)
    return {"ok": True, "schedule": schedule.__dict__}


@app.delete("/api/sync/schedules/{schedule_id}")
@app.delete("/api/schedules/{schedule_id}")
def delete_schedule(schedule_id: str):
    try:
        schedule = SCHEDULE_MANAGER.delete_schedule(schedule_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="schedule not found")
    return {"ok": True, "schedule": schedule.__dict__}


@app.post("/api/sync/schedules/{schedule_id}/pause")
@app.post("/api/schedules/{schedule_id}/pause")
def pause_schedule(schedule_id: str):
    try:
        schedule = SCHEDULE_MANAGER.set_enabled(schedule_id, False)
    except KeyError:
        raise HTTPException(status_code=404, detail="schedule not found")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"ok": True, "schedule": schedule.__dict__}


@app.post("/api/sync/schedules/{schedule_id}/resume")
@app.post("/api/schedules/{schedule_id}/resume")
def resume_schedule(schedule_id: str):
    try:
        schedule = SCHEDULE_MANAGER.set_enabled(schedule_id, True)
    except KeyError:
        raise HTTPException(status_code=404, detail="schedule not found")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"ok": True, "schedule": schedule.__dict__}


@app.post("/api/sync/schedules/{schedule_id}/run-now")
@app.post("/api/schedules/{schedule_id}/run-now")
def run_schedule_now(schedule_id: str):
    try:
        schedule, job = SCHEDULE_MANAGER.run_schedule_now(schedule_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="schedule not found")
    except Exception as exc:
        raise _job_error_to_http(exc)
    return {
        "ok": True,
        "schedule": schedule.__dict__,
        "job": job.__dict__,
    }


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
        payload = request.payload
        if not isinstance(payload, dict):
            raise ValueError(
                "payload is required. Build the wide-table payload in AlphaBlocks, then send it to the sync service."
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
                market=request.market,
                index_code=request.index_code,
                table_names=request.table_names,
                sector_name=request.sector_name,
                code_market=request.code_market,
                period=request.period,
                fields=request.fields,
                adjust_type=request.adjust_type,
                qmt_adjust_type=request.qmt_adjust_type or request.adjust_type,
                fill_data=request.fill_data,
                count=request.count,
                incrementally=request.incrementally,
                complete=request.complete,
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
            raise ValueError(f"unknown registered task: {task_name}")
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
