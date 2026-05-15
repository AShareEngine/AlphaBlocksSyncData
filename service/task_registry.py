#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Decorator-based sync task registry for API execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from sync_data_system.amazingdata_sdk_provider import AmazingDataSDKConfig, AmazingDataSDKProvider
from sync_data_system.base_data import BaseData
from sync_data_system.clickhouse_client import ClickHouseConfig
from sync_data_system.data_models import normalize_code_list
from sync_data_system.info_data import InfoData
from sync_data_system.market_data import MarketData
from sync_data_system import run_sync as run_sync_module


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
        )

    return decorator


def register_input_resolver(name: str):
    def decorator(resolver):
        return TASK_REGISTRY.register_resolver(name=name, resolver=resolver)

    return decorator


@dataclass
class ApiSyncExecutionContext:
    sdk_config: AmazingDataSDKConfig
    provider: AmazingDataSDKProvider
    base_data: BaseData
    info_data: InfoData
    market_data: MarketData

    def close(self) -> None:
        try:
            self.market_data.close()
        except Exception:
            pass
        try:
            self.info_data.close()
        except Exception:
            pass
        try:
            self.base_data.close()
        except Exception:
            pass
        self.provider.close()


@dataclass
class BaoStockExecutionContext:
    provider: Any
    repository: Any
    connection: Any

    def close(self) -> None:
        try:
            self.connection.close()
        except Exception:
            pass
        self.provider.close()


@dataclass
class QmtExecutionContext:
    provider: Any
    repository: Any
    connection: Any

    def close(self) -> None:
        try:
            self.connection.close()
        except Exception:
            pass
        self.provider.close()


def build_amazingdata_context(runtime_path: Optional[str] = None) -> ApiSyncExecutionContext:
    sdk_config = AmazingDataSDKConfig.from_env(runtime_path=runtime_path)
    clickhouse_config = ClickHouseConfig.from_env(runtime_path=runtime_path)
    provider = AmazingDataSDKProvider(sdk_config)
    base_data = BaseData.from_clickhouse_config(clickhouse_config, sync_provider=provider)
    info_data = InfoData.from_clickhouse_config(clickhouse_config, sync_provider=provider)
    market_data = MarketData.from_clickhouse_config(clickhouse_config, sync_provider=provider)
    return ApiSyncExecutionContext(
        sdk_config=sdk_config,
        provider=provider,
        base_data=base_data,
        info_data=info_data,
        market_data=market_data,
    )


def build_baostock_context(runtime_path: Optional[str] = None, database: str = "baostock") -> BaoStockExecutionContext:
    from sync_data_system.sources.baostock.provider import BaoStockConfig, BaoStockProvider
    from sync_data_system.sources.baostock.repository import BaoStockRepository
    from sync_data_system.sync_core.clickhouse import create_clickhouse_client

    clickhouse_config = ClickHouseConfig.from_env(runtime_path=runtime_path)
    provider = BaoStockProvider(BaoStockConfig.from_env(runtime_path=runtime_path))
    connection = create_clickhouse_client(clickhouse_config)
    repository = BaoStockRepository(connection, database=database)
    repository.ensure_tables()
    return BaoStockExecutionContext(provider=provider, repository=repository, connection=connection)


def build_qmt_context(runtime_path: Optional[str] = None, database: str = "qmt") -> QmtExecutionContext:
    from sync_data_system.sources.qmt.provider import QmtConfig, QmtProvider
    from sync_data_system.sources.qmt.repository import QmtRepository
    from sync_data_system.sync_core.clickhouse import create_clickhouse_client

    clickhouse_config = ClickHouseConfig.from_env(runtime_path=runtime_path)
    provider = QmtProvider(QmtConfig.from_env(runtime_path=runtime_path))
    connection = create_clickhouse_client(clickhouse_config)
    repository = QmtRepository(connection, database=database)
    repository.ensure_tables()
    return QmtExecutionContext(provider=provider, repository=repository, connection=connection)


@register_input_resolver("run_sync_defaults")
def resolve_run_sync_defaults(probe: SyncTaskProbe) -> None:
    if probe.context is None:
        raise RuntimeError("probe.context is required for run_sync_defaults")

    task = probe.name
    ignores_date_range = run_sync_module.task_ignores_date_range(task)
    begin_date, end_date = run_sync_module.resolve_date_window(
        provider=probe.context.provider,
        begin_date=None if ignores_date_range else probe.input_begin_date,
        end_date=None if ignores_date_range else probe.input_end_date,
    )

    codes: list[str] = []
    if run_sync_module.task_requires_code_list(task):
        if task == "backward_factor":
            codes = run_sync_module.resolve_backward_factor_code_list(
                base_data=probe.context.base_data,
                raw_codes=",".join(probe.input_codes),
                limit=probe.limit,
            )
        elif task in {"industry_constituent", "industry_weight", "industry_daily"}:
            codes = run_sync_module.resolve_industry_code_list(
                info_data=probe.context.info_data,
                raw_codes=",".join(probe.input_codes),
                limit=probe.limit,
            )
        else:
            codes = run_sync_module.resolve_code_list(
                base_data=probe.context.base_data,
                task=task,
                raw_codes=",".join(probe.input_codes),
                limit=probe.limit,
                local_path=probe.context.sdk_config.local_path,
                end_date=end_date,
            )
        task_spec = run_sync_module.TaskRunSpec(
            task=task,
            codes_raw=",".join(probe.input_codes),
            begin_date=None if ignores_date_range else probe.input_begin_date,
            end_date=None if ignores_date_range else probe.input_end_date,
            limit=probe.limit,
            force=probe.force,
            resume=probe.resume,
        )
        codes = run_sync_module.filter_code_list_for_resume(
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

    probe.begin_date, probe.end_date = run_sync_module.resolve_date_window(
        provider=probe.context.provider,
        begin_date=probe.input_begin_date,
        end_date=probe.input_end_date,
    )
    if probe.input_codes:
        codes = normalize_code_list(probe.input_codes)
        if probe.limit and probe.limit > 0:
            codes = codes[: probe.limit]
    else:
        codes = run_sync_module.resolve_market_kline_code_list(
            base_data=probe.context.base_data,
            task=probe.name,
            limit=probe.limit,
        )
    task_spec = run_sync_module.TaskRunSpec(
        task=probe.name,
        codes_raw=",".join(probe.input_codes),
        begin_date=probe.input_begin_date,
        end_date=probe.input_end_date,
        limit=probe.limit,
        force=probe.force,
        resume=probe.resume,
    )
    probe.codes = run_sync_module.filter_code_list_for_resume(
        context=probe.context,
        task_spec=task_spec,
        code_list=codes,
        begin_date=probe.begin_date,
        end_date=probe.end_date,
    )


def _execute_via_run_sync(probe: SyncTaskProbe) -> int:
    task_spec = run_sync_module.TaskRunSpec(
        task=probe.name,
        codes_raw=",".join(probe.codes),
        begin_date=probe.begin_date,
        end_date=probe.end_date,
        limit=probe.limit,
        force=probe.force,
        resume=probe.resume,
    )
    return run_sync_module.execute_task_spec(probe.context, task_spec)


def _register_run_sync_task(name: str, target: str, input_resolver: str = "run_sync_defaults") -> None:
    @sync_task(
        name=name,
        source="amazingdata",
        database="starlight",
        target=target,
        input_resolver=input_resolver,
        request_fields=RUN_TASK_REQUEST_FIELDS,
    )
    def _generated_task(probe: SyncTaskProbe) -> int:
        inserted = _execute_via_run_sync(probe)
        probe.set_row_count(inserted)
        return inserted


for _task_name in run_sync_module.TASK_CHOICES:
    _register_run_sync_task(
        _task_name,
        run_sync_module.TASK_TARGET_TABLE_MAP[_task_name],
        input_resolver=run_sync_module.TASK_INPUT_RESOLVER_MAP.get(_task_name, "run_sync_defaults"),
    )


def _baostock_request_fields(spec) -> tuple[str, ...]:
    fields = ["name"]
    if spec.uses_code:
        fields.append("codes")
    if spec.uses_day:
        fields.append("day")
    if spec.uses_begin_end:
        fields.extend(["begin_date", "end_date"])
    if spec.uses_year:
        fields.append("year")
    if spec.uses_quarter:
        fields.append("quarter")
    if spec.uses_year_type:
        fields.append("year_type")
    fields.extend(["limit", "force", "log_level"])
    if spec.task == "daily_kline":
        fields.extend(["adjustflag", "frequency"])
    return tuple(dict.fromkeys(fields))


def _format_optional_int(value: Optional[int]) -> str:
    return "" if value is None else str(value)


def _register_baostock_task(task_name: str, spec) -> None:
    registry_name = f"baostock.{task_name}"

    @sync_task(
        name=registry_name,
        source="baostock",
        database="baostock",
        target=spec.table_name,
        input_resolver=None,
        request_fields=_baostock_request_fields(spec),
    )
    def _generated_baostock_task(probe: SyncTaskProbe) -> int:
        from sync_data_system.sources.baostock.runner import SyncArgs, run_sync_args

        args = SyncArgs(
            task=task_name,
            codes_raw=",".join(probe.input_codes),
            begin_date=_format_optional_int(probe.input_begin_date),
            end_date=_format_optional_int(probe.input_end_date),
            day=_format_optional_int(probe.input_day),
            year=probe.input_year,
            quarter=probe.input_quarter,
            year_type=str(probe.input_year_type or "").strip(),
            adjustflag=str(probe.adjustflag or "3").strip() or "3",
            frequency=str(probe.frequency or "d").strip() or "d",
            limit=probe.limit,
            force=probe.force,
            continue_on_error=False,
            runtime_path=probe.runtime_path,
            database="baostock",
            log_level=str(probe.log_level or "INFO"),
        )
        inserted = run_sync_args(args, probe.context.provider, probe.context.repository)
        probe.set_row_count(inserted)
        return inserted


try:
    from sync_data_system.sources.baostock.specs import BAOSTOCK_TASK_SPECS

    for _baostock_task_name, _baostock_spec in BAOSTOCK_TASK_SPECS.items():
        _register_baostock_task(_baostock_task_name, _baostock_spec)
except Exception:
    pass


def _qmt_request_fields(spec) -> tuple[str, ...]:
    fields = ["name"]
    if spec.uses_symbols:
        fields.append("codes")
    if spec.uses_symbol or spec.uses_stock_code:
        fields.append("codes")
    if spec.uses_market:
        fields.append("market")
    if spec.uses_index_code:
        fields.append("index_code")
    if spec.uses_table_names:
        fields.append("table_names")
    if spec.uses_sector_name:
        fields.append("sector_name")
    if spec.uses_code_market:
        fields.append("code_market")
    if spec.uses_begin_end:
        fields.extend(["begin_date", "end_date"])
    if spec.uses_period:
        fields.append("period")
    if spec.uses_fields:
        fields.append("fields")
    if spec.uses_adjust_type:
        fields.append("adjust_type")
    if spec.uses_fill_data:
        fields.append("fill_data")
    if spec.uses_count:
        fields.append("count")
    if spec.uses_incrementally:
        fields.append("incrementally")
    if spec.uses_complete:
        fields.append("complete")
    fields.extend(["limit", "force", "log_level"])
    return tuple(dict.fromkeys(fields))


def _register_qmt_task(task_name: str, spec) -> None:
    registry_name = f"qmt.{task_name}"

    @sync_task(
        name=registry_name,
        source="qmt",
        database="qmt",
        target=spec.table_name,
        input_resolver=None,
        request_fields=_qmt_request_fields(spec),
    )
    def _generated_qmt_task(probe: SyncTaskProbe) -> int:
        from sync_data_system.sources.qmt.runner import SyncArgs, run_sync_args

        codes = ",".join(probe.input_codes)
        args = SyncArgs(
            task=task_name,
            symbols_raw=codes,
            symbol=codes.split(",", 1)[0] if codes else "",
            market=str(getattr(probe, "input_market", "") or ""),
            index_code=str(getattr(probe, "input_index_code", "") or ""),
            stock_code=codes.split(",", 1)[0] if codes else "",
            table_names_raw=str(getattr(probe, "input_table_names", "") or ""),
            sector_name=str(getattr(probe, "input_sector_name", "") or ""),
            code_market=str(getattr(probe, "input_code_market", "") or ""),
            begin_time=_format_optional_int(probe.input_begin_date),
            end_time=_format_optional_int(probe.input_end_date),
            period=str(getattr(probe, "input_period", "") or ""),
            fields_raw=str(getattr(probe, "input_fields", "") or ""),
            adjust_type=str(getattr(probe, "input_adjust_type", "none") or "none"),
            fill_data=bool(getattr(probe, "input_fill_data", True)),
            count=int(getattr(probe, "input_count", -1) or -1),
            incrementally=bool(getattr(probe, "input_incrementally", False)),
            complete=bool(getattr(probe, "input_complete", False)),
            limit=probe.limit,
            force=probe.force,
            continue_on_error=False,
            runtime_path=probe.runtime_path,
            database="qmt",
            log_level=str(probe.log_level or "INFO"),
        )
        inserted = run_sync_args(args, probe.context.provider, probe.context.repository)
        probe.set_row_count(inserted)
        return inserted


try:
    from sync_data_system.sources.qmt.specs import QMT_TASK_SPECS

    for _qmt_task_name, _qmt_spec in QMT_TASK_SPECS.items():
        _register_qmt_task(_qmt_task_name, _qmt_spec)
except Exception:
    pass


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
    "ApiSyncExecutionContext",
    "SyncTaskProbe",
    "TASK_REGISTRY",
    "TaskDefinition",
    "BaoStockExecutionContext",
    "QmtExecutionContext",
    "build_amazingdata_context",
    "build_baostock_context",
    "build_qmt_context",
    "create_probe",
    "register_input_resolver",
    "sync_task",
]
