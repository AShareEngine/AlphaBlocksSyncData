#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Decorator-based sync task registry for API execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from sync_data_system.core.providers import ProviderManifest, ProviderTaskManifest, load_provider_registry
from sync_data_system.data_models import normalize_code_list
from sync_data_system.providers.amazingdata import runner as amazingdata_runner


RUN_TASK_REQUEST_FIELDS = (
    "name",
    "codes",
    "begin_date",
    "end_date",
    "limit",
    "force",
    "resume",
    "log_level",
)

PROBE_PUBLIC_FIELDS = (
    "name",
    "source",
    "target",
    "database",
    "job_id",
    "runtime_path",
    "input_codes",
    "input_day",
    "input_begin_date",
    "input_end_date",
    "input_year",
    "input_quarter",
    "input_year_type",
    "input_market",
    "input_index_code",
    "input_table_names",
    "input_sector_name",
    "input_code_market",
    "input_period",
    "input_fields",
    "input_adjust_type",
    "input_fill_data",
    "input_count",
    "input_incrementally",
    "input_complete",
    "limit",
    "force",
    "resume",
    "adjustflag",
    "frequency",
    "log_level",
    "codes",
    "begin_date",
    "end_date",
    "row_count",
    "status",
    "message",
    "log_path",
)


@dataclass(frozen=True)
class TaskDefinition:
    name: str
    source: str
    target: str
    database: Optional[str]
    input_resolver: Optional[str]
    request_fields: tuple[str, ...]
    supports_incremental: bool
    cursor_field: str
    freshness_mode: str
    handler: Callable[["SyncTaskProbe"], Any]


@dataclass
class SyncTaskProbe:
    name: str
    source: str
    target: str
    job_id: str
    project_root: Path
    log_path: Path
    database: Optional[str] = None
    runtime_path: Optional[str] = None
    input_codes: list[str] = field(default_factory=list)
    input_day: Optional[int] = None
    input_begin_date: Optional[int] = None
    input_end_date: Optional[int] = None
    input_year: Optional[int] = None
    input_quarter: Optional[int] = None
    input_year_type: Optional[str] = None
    input_market: Optional[str] = None
    input_index_code: Optional[str] = None
    input_table_names: Optional[str] = None
    input_sector_name: Optional[str] = None
    input_code_market: Optional[str] = None
    input_period: Optional[str] = None
    input_fields: Optional[str] = None
    input_adjust_type: Optional[str] = None
    input_fill_data: bool = True
    input_count: int = -1
    input_incrementally: bool = False
    input_complete: bool = False
    limit: int = 0
    force: bool = False
    resume: bool = False
    adjustflag: str = "3"
    frequency: str = "d"
    log_level: Optional[str] = None
    codes: list[str] = field(default_factory=list)
    begin_date: Optional[int] = None
    end_date: Optional[int] = None
    row_count: int = 0
    status: str = "created"
    message: str = ""
    context: Any = None

    def log(self, message: str) -> None:
        timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(f"{timestamp} {message}\n")

    def set_status(self, status: str, message: str = "") -> None:
        self.status = status
        if message:
            self.message = message
            self.log(message)

    def set_row_count(self, row_count: int) -> None:
        self.row_count = int(row_count)

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source": self.source,
            "target": self.target,
            "database": self.database,
            "job_id": self.job_id,
            "runtime_path": self.runtime_path,
            "input_codes": list(self.input_codes),
            "input_day": self.input_day,
            "input_begin_date": self.input_begin_date,
            "input_end_date": self.input_end_date,
            "input_year": self.input_year,
            "input_quarter": self.input_quarter,
            "input_year_type": self.input_year_type,
            "input_market": self.input_market,
            "input_index_code": self.input_index_code,
            "input_table_names": self.input_table_names,
            "input_sector_name": self.input_sector_name,
            "input_code_market": self.input_code_market,
            "input_period": self.input_period,
            "input_fields": self.input_fields,
            "input_adjust_type": self.input_adjust_type,
            "input_fill_data": self.input_fill_data,
            "input_count": self.input_count,
            "input_incrementally": self.input_incrementally,
            "input_complete": self.input_complete,
            "limit": self.limit,
            "force": self.force,
            "resume": self.resume,
            "adjustflag": self.adjustflag,
            "frequency": self.frequency,
            "log_level": self.log_level,
            "codes": list(self.codes),
            "begin_date": self.begin_date,
            "end_date": self.end_date,
            "row_count": self.row_count,
            "status": self.status,
            "message": self.message,
            "log_path": str(self.log_path),
        }


class SyncTaskRegistry:
    def __init__(self) -> None:
        self._tasks: dict[str, TaskDefinition] = {}
        self._resolvers: dict[str, Callable[[SyncTaskProbe], None]] = {}

    def register_task(
        self,
        name: str,
        source: str,
        target: str,
        input_resolver: Optional[str],
        handler,
        *,
        database: Optional[str] = None,
        request_fields: tuple[str, ...] | None = None,
        supports_incremental: bool = False,
        cursor_field: str = "",
        freshness_mode: str = "daily",
    ) -> Callable:
        if name in self._tasks:
            raise ValueError(f"duplicate task definition: {name}")
        self._tasks[name] = TaskDefinition(
            name=name,
            source=source,
            target=target,
            database=database,
            input_resolver=input_resolver,
            request_fields=tuple(request_fields or RUN_TASK_REQUEST_FIELDS),
            supports_incremental=bool(supports_incremental),
            cursor_field=str(cursor_field or "").strip(),
            freshness_mode=str(freshness_mode or "daily").strip() or "daily",
            handler=handler,
        )
        return handler

    def register_resolver(self, name: str, resolver: Callable[[SyncTaskProbe], None]) -> Callable[[SyncTaskProbe], None]:
        if name in self._resolvers:
            raise ValueError(f"duplicate input resolver: {name}")
        self._resolvers[name] = resolver
        return resolver

    def get_task(self, name: str) -> TaskDefinition:
        if name not in self._tasks:
            raise KeyError(name)
        return self._tasks[name]

    def list_tasks(self) -> list[TaskDefinition]:
        return [self._tasks[key] for key in sorted(self._tasks.keys())]

    def list_task_metadata(self) -> list[dict[str, Any]]:
        return [
            {
                "name": task.name,
                "source": task.source,
                "database": task.database,
                "target": task.target,
                "input_resolver": task.input_resolver,
                "request_fields": list(task.request_fields),
                "supports_incremental": task.supports_incremental,
                "cursor_field": task.cursor_field,
                "freshness_mode": task.freshness_mode,
                "probe_fields": list(PROBE_PUBLIC_FIELDS),
            }
            for task in self.list_tasks()
        ]

    def get_task_metadata(self, name: str) -> dict[str, Any]:
        task = self.get_task(name)
        return {
            "name": task.name,
            "source": task.source,
            "database": task.database,
            "target": task.target,
            "input_resolver": task.input_resolver,
            "request_fields": list(task.request_fields),
            "supports_incremental": task.supports_incremental,
            "cursor_field": task.cursor_field,
            "freshness_mode": task.freshness_mode,
            "probe_fields": list(PROBE_PUBLIC_FIELDS),
        }

    def resolve_inputs(self, probe: SyncTaskProbe) -> SyncTaskProbe:
        task = self.get_task(probe.name)
        if not task.input_resolver:
            probe.codes = list(probe.input_codes)
            probe.begin_date = probe.input_begin_date
            probe.end_date = probe.input_end_date
            return probe
        resolver = self._resolvers[task.input_resolver]
        resolver(probe)
        return probe


TASK_REGISTRY = SyncTaskRegistry()


def sync_task(
    name: str,
    source: str,
    target: str,
    input_resolver: Optional[str] = None,
    *,
    database: Optional[str] = None,
    request_fields: tuple[str, ...] | None = None,
    supports_incremental: bool = False,
    cursor_field: str = "",
    freshness_mode: str = "daily",
):
    def decorator(handler):
        return TASK_REGISTRY.register_task(
            name=name,
            source=source,
            target=target,
            input_resolver=input_resolver,
            handler=handler,
            database=database,
            request_fields=request_fields,
            supports_incremental=supports_incremental,
            cursor_field=cursor_field,
            freshness_mode=freshness_mode,
        )

    return decorator


def register_input_resolver(name: str):
    def decorator(resolver):
        return TASK_REGISTRY.register_resolver(name=name, resolver=resolver)

    return decorator


def build_provider_context(source: str, runtime_path: Optional[str] = None, database: Optional[str] = None) -> Any:
    manifest = load_provider_registry().get(source)
    builder = manifest.load_context_builder()
    return builder(runtime_path=runtime_path, database=database or manifest.default_database)


def _provider_task_name(registry_name: str, source: str) -> str:
    prefix = f"{source}."
    return registry_name[len(prefix):] if str(registry_name).startswith(prefix) else registry_name


@register_input_resolver("run_sync_defaults")
def resolve_run_sync_defaults(probe: SyncTaskProbe) -> None:
    if probe.context is None:
        raise RuntimeError("probe.context is required for run_sync_defaults")

    task = _provider_task_name(probe.name, probe.source)
    ignores_date_range = amazingdata_runner.task_ignores_date_range(task)
    begin_date, end_date = amazingdata_runner.resolve_date_window(
        provider=probe.context.provider,
        begin_date=None if ignores_date_range else probe.input_begin_date,
        end_date=None if ignores_date_range else probe.input_end_date,
    )

    codes: list[str] = []
    if amazingdata_runner.task_requires_code_list(task):
        if task == "backward_factor":
            codes = amazingdata_runner.resolve_backward_factor_code_list(
                base_data=probe.context.base_data,
                raw_codes=",".join(probe.input_codes),
                limit=probe.limit,
            )
        elif task in {"industry_constituent", "industry_weight", "industry_daily"}:
            codes = amazingdata_runner.resolve_industry_code_list(
                info_data=probe.context.info_data,
                raw_codes=",".join(probe.input_codes),
                limit=probe.limit,
            )
        else:
            codes = amazingdata_runner.resolve_code_list(
                base_data=probe.context.base_data,
                task=task,
                raw_codes=",".join(probe.input_codes),
                limit=probe.limit,
                local_path=probe.context.sdk_config.local_path,
                end_date=end_date,
            )
        task_spec = amazingdata_runner.TaskRunSpec(
            task=task,
            codes_raw=",".join(probe.input_codes),
            begin_date=None if ignores_date_range else probe.input_begin_date,
            end_date=None if ignores_date_range else probe.input_end_date,
            limit=probe.limit,
            force=probe.force,
            resume=probe.resume,
        )
        codes = amazingdata_runner.filter_code_list_for_resume(
            context=probe.context,
            task_spec=task_spec,
            code_list=codes,
            begin_date=begin_date,
            end_date=end_date,
        )

    probe.codes = codes
    probe.begin_date = begin_date
    probe.end_date = end_date


@register_input_resolver("market_kline_defaults")
def resolve_market_kline_defaults(probe: SyncTaskProbe) -> None:
    if probe.context is None:
        raise RuntimeError("probe.context is required for market_kline_defaults")

    probe.begin_date, probe.end_date = amazingdata_runner.resolve_date_window(
        provider=probe.context.provider,
        begin_date=probe.input_begin_date,
        end_date=probe.input_end_date,
    )
    if probe.input_codes:
        codes = normalize_code_list(probe.input_codes)
        if probe.limit and probe.limit > 0:
            codes = codes[: probe.limit]
    else:
        codes = amazingdata_runner.resolve_market_kline_code_list(
            base_data=probe.context.base_data,
            task=_provider_task_name(probe.name, probe.source),
            limit=probe.limit,
        )
    task_name = _provider_task_name(probe.name, probe.source)
    task_spec = amazingdata_runner.TaskRunSpec(
        task=task_name,
        codes_raw=",".join(probe.input_codes),
        begin_date=probe.input_begin_date,
        end_date=probe.input_end_date,
        limit=probe.limit,
        force=probe.force,
        resume=probe.resume,
    )
    probe.codes = amazingdata_runner.filter_code_list_for_resume(
        context=probe.context,
        task_spec=task_spec,
        code_list=codes,
        begin_date=probe.begin_date,
        end_date=probe.end_date,
    )


def _register_provider_task(manifest: ProviderManifest, task: ProviderTaskManifest) -> None:
    registry_name = f"{manifest.name}.{task.name}"
    input_resolver = None
    if manifest.name == "amazingdata":
        input_resolver = amazingdata_runner.TASK_INPUT_RESOLVER_MAP.get(task.name, "run_sync_defaults")

    @sync_task(
        name=registry_name,
        source=manifest.name,
        database=manifest.default_database,
        target=task.target,
        input_resolver=input_resolver,
        request_fields=task.request_fields or RUN_TASK_REQUEST_FIELDS,
        supports_incremental=task.supports_incremental,
        cursor_field=task.cursor_field,
        freshness_mode=task.freshness_mode,
    )
    def _generated_provider_task(probe: SyncTaskProbe) -> int:
        runner = manifest.load_registered_task_runner()
        return runner(probe)


def _register_manifest_provider_tasks() -> None:
    for manifest in load_provider_registry().list():
        if not manifest.entrypoints.registered_task_runner:
            continue
        for task in manifest.tasks:
            _register_provider_task(manifest, task)


_register_manifest_provider_tasks()


def create_probe(
    *,
    task_name: str,
    job_id: str,
    project_root: Path,
    log_path: Path,
    runtime_path: Optional[str] = None,
    codes: Optional[list[str]] = None,
    day: Optional[int] = None,
    begin_date: Optional[int] = None,
    end_date: Optional[int] = None,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    year_type: Optional[str] = None,
    market: Optional[str] = None,
    index_code: Optional[str] = None,
    table_names: Optional[str] = None,
    sector_name: Optional[str] = None,
    code_market: Optional[str] = None,
    period: Optional[str] = None,
    fields: Optional[str] = None,
    qmt_adjust_type: Optional[str] = None,
    fill_data: bool = True,
    count: int = -1,
    incrementally: bool = False,
    complete: bool = False,
    limit: int = 0,
    force: bool = False,
    resume: bool = False,
    adjustflag: str = "3",
    frequency: str = "d",
    log_level: Optional[str] = None,
) -> SyncTaskProbe:
    definition = TASK_REGISTRY.get_task(task_name)
    return SyncTaskProbe(
        name=definition.name,
        source=definition.source,
        target=definition.target,
        database=definition.database,
        job_id=job_id,
        project_root=project_root,
        log_path=log_path,
        runtime_path=runtime_path,
        input_codes=list(codes or []),
        input_day=day,
        input_begin_date=begin_date,
        input_end_date=end_date,
        input_year=year,
        input_quarter=quarter,
        input_year_type=year_type,
        input_market=market,
        input_index_code=index_code,
        input_table_names=table_names,
        input_sector_name=sector_name,
        input_code_market=code_market,
        input_period=period,
        input_fields=fields,
        input_adjust_type=qmt_adjust_type,
        input_fill_data=fill_data,
        input_count=count,
        input_incrementally=incrementally,
        input_complete=complete,
        limit=limit,
        force=force,
        resume=resume,
        adjustflag=str(adjustflag or "3").strip() or "3",
        frequency=str(frequency or "d").strip() or "d",
        log_level=log_level,
    )


__all__ = [
    "SyncTaskProbe",
    "TASK_REGISTRY",
    "TaskDefinition",
    "build_provider_context",
    "create_probe",
    "register_input_resolver",
    "sync_task",
]
