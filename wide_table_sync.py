#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Wide table sync planning utilities."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

from sync_data_system.clickhouse_client import ClickHouseConfig, ClickHouseConnection, create_clickhouse_client


DEFAULT_RUNTIME_STATE_DATABASE = "alphablocks"
WIDE_TABLE_SYNC_STATE_TABLE = "wide_table_sync_state"


@dataclass(frozen=True)
class WideTableTarget:
    database: str
    table: str
    engine: str
    partition_by: tuple[str, ...]
    order_by: tuple[str, ...]
    version_field: str


@dataclass(frozen=True)
class WideTableMetadata:
    spec_path: str
    spec_name: str
    wide_table_id: str
    source_node: str
    target: WideTableTarget
    fields: tuple[str, ...]
    key_fields: tuple[str, ...]
    status: str


@dataclass(frozen=True)
class WideTableValidation:
    ok: bool
    messages: tuple[str, ...]


@dataclass(frozen=True)
class WideTableSyncPlan:
    spec_path: str
    wide_table_name: str
    target_database: str
    target_table: str
    wide_table_signature: str
    plan_signature: str
    action: str
    validation: WideTableValidation
    reason: str


@dataclass(frozen=True)
class WideTableRunResult:
    wide_table_name: str
    action: str
    status: str
    message: str


@dataclass(frozen=True)
class WideTableSyncStateRow:
    wide_table_id: str
    wide_table_name: str
    source_node: str
    target_database: str
    target_table: str
    spec_path: str
    wide_table_signature: str
    plan_signature: str
    last_status: str
    last_action: str
    last_message: str
    last_started_at: Optional[datetime]
    last_finished_at: Optional[datetime]
    updated_at: datetime


class WideTableSyncStateRepository:
    COLUMNS = (
        "wide_table_id",
        "wide_table_name",
        "source_node",
        "target_database",
        "target_table",
        "spec_path",
        "wide_table_signature",
        "plan_signature",
        "last_status",
        "last_action",
        "last_message",
        "last_started_at",
        "last_finished_at",
        "updated_at",
    )

    def __init__(
        self,
        client: ClickHouseConnection,
        *,
        database: str,
        table: str = WIDE_TABLE_SYNC_STATE_TABLE,
    ) -> None:
        self.client = client
        self.database = str(database).strip() or "default"
        self.table = str(table).strip() or WIDE_TABLE_SYNC_STATE_TABLE

    @property
    def table_ref(self) -> str:
        return f"{self.database}.{self.table}"

    def ensure_table(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_ref}
            (
                wide_table_id String,
                wide_table_name String,
                source_node String,
                target_database String,
                target_table String,
                spec_path String,
                wide_table_signature String,
                plan_signature String,
                last_status String,
                last_action String,
                last_message String,
                last_started_at Nullable(DateTime64(3)),
                last_finished_at Nullable(DateTime64(3)),
                updated_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (wide_table_id, target_database, target_table)
            """
        )

    def load_target_exists_lookup(self, targets: Sequence[tuple[str, str]]) -> dict[tuple[str, str], bool]:
        if not targets:
            return {}
        databases = sorted({database for database, _table in targets})
        tables = sorted({table for _database, table in targets})
        rows = self.client.query_rows(
            """
            SELECT database, name
            FROM system.tables
            WHERE database IN {databases:Array(String)}
              AND name IN {tables:Array(String)}
            """,
            {"databases": databases, "tables": tables},
        )
        existing = {(str(row[0]), str(row[1])) for row in rows if len(row) >= 2}
        return {target: target in existing for target in targets}

    def load_previous_signature_lookup(
        self,
        targets: Sequence[tuple[str, str]],
    ) -> dict[tuple[str, str], tuple[Optional[str], Optional[str]]]:
        if not targets:
            return {}
        databases = sorted({database for database, _table in targets})
        tables = sorted({table for _database, table in targets})
        rows = self.client.query_rows(
            f"""
            SELECT
                target_database,
                target_table,
                argMax(wide_table_signature, updated_at) AS wide_table_signature,
                argMax(plan_signature, updated_at) AS plan_signature
            FROM {self.table_ref}
            WHERE target_database IN {{databases:Array(String)}}
              AND target_table IN {{tables:Array(String)}}
            GROUP BY target_database, target_table
            """,
            {"databases": databases, "tables": tables},
        )
        lookup = {
            (str(row[0]), str(row[1])): (
                str(row[2]) if row[2] is not None else None,
                str(row[3]) if row[3] is not None else None,
            )
            for row in rows
            if len(row) >= 4
        }
        for target in targets:
            lookup.setdefault(target, (None, None))
        return lookup

    def save_plan_states(self, metadata_map: dict[str, WideTableMetadata], plans: Sequence[WideTableSyncPlan]) -> int:
        if not plans:
            return 0
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows: list[tuple[Any, ...]] = []
        for plan in plans:
            metadata = metadata_map[plan.spec_path]
            last_status = "failed" if not plan.validation.ok else "pending"
            rows.append(
                (
                    metadata.wide_table_id,
                    metadata.spec_name,
                    metadata.source_node,
                    metadata.target.database,
                    metadata.target.table,
                    metadata.spec_path,
                    plan.wide_table_signature,
                    plan.plan_signature,
                    last_status,
                    plan.action,
                    plan.reason,
                    None,
                    None,
                    now,
                )
            )
        self.client.insert_rows(self.table_ref, self.COLUMNS, rows)
        return len(rows)

    def save_state(
        self,
        metadata: WideTableMetadata,
        *,
        wide_table_signature: str,
        plan_signature: str,
        last_status: str,
        last_action: str,
        last_message: str,
        last_started_at: Optional[datetime],
        last_finished_at: Optional[datetime],
    ) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        self.client.insert_rows(
            self.table_ref,
            self.COLUMNS,
            [
                (
                    metadata.wide_table_id,
                    metadata.spec_name,
                    metadata.source_node,
                    metadata.target.database,
                    metadata.target.table,
                    metadata.spec_path,
                    wide_table_signature,
                    plan_signature,
                    last_status,
                    last_action,
                    last_message,
                    last_started_at,
                    last_finished_at,
                    now,
                )
            ],
        )

    def load_states(self) -> list[WideTableSyncStateRow]:
        rows = self.client.query_rows(
            f"""
            SELECT
                wide_table_id,
                wide_table_name,
                source_node,
                target_database,
                target_table,
                spec_path,
                wide_table_signature,
                plan_signature,
                last_status,
                last_action,
                last_message,
                last_started_at,
                last_finished_at,
                updated_at
            FROM {self.table_ref}
            ORDER BY wide_table_name, updated_at DESC
            """
        )
        latest_by_name: dict[str, WideTableSyncStateRow] = {}
        for row in rows:
            if len(row) < 14:
                continue
            state = WideTableSyncStateRow(*row)
            latest_by_name.setdefault(state.wide_table_name, state)
        return list(latest_by_name.values())

    def load_state(self, wide_table_name: str) -> Optional[WideTableSyncStateRow]:
        rows = self.client.query_rows(
            f"""
            SELECT
                wide_table_id,
                wide_table_name,
                source_node,
                target_database,
                target_table,
                spec_path,
                wide_table_signature,
                plan_signature,
                last_status,
                last_action,
                last_message,
                last_started_at,
                last_finished_at,
                updated_at
            FROM {self.table_ref}
            WHERE wide_table_name = {{wide_table_name:String}}
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            {"wide_table_name": wide_table_name},
        )
        if not rows:
            return None
        row = rows[0]
        return WideTableSyncStateRow(*row)


def parse_wide_table_metadata(spec_path: str, payload: dict[str, Any]) -> WideTableMetadata:
    wide_table = payload.get("wide_table")
    if not isinstance(wide_table, dict):
        raise ValueError(f"wide_table section missing: {spec_path}")

    target = wide_table.get("target")
    if not isinstance(target, dict):
        raise ValueError(f"wide_table.target missing: {spec_path}")

    spec_name = str(wide_table.get("name") or "").strip()
    return WideTableMetadata(
        spec_path=spec_path,
        spec_name=spec_name,
        wide_table_id=str(wide_table.get("id") or ""),
        source_node=str(wide_table.get("source_node") or ""),
        target=WideTableTarget(
            database=str(target.get("database") or ""),
            table=str(target.get("table") or ""),
            engine=str(target.get("engine") or ""),
            partition_by=tuple(str(item) for item in (target.get("partition_by") or [])),
            order_by=tuple(str(item) for item in (target.get("order_by") or [])),
            version_field=str(target.get("version_field") or ""),
        ),
        fields=tuple(str(item) for item in (wide_table.get("fields") or [])),
        key_fields=tuple(str(item) for item in (wide_table.get("key_fields") or [])),
        status=str(wide_table.get("status") or ""),
    )


def build_wide_table_metadata(payload: dict[str, Any], *, spec_path: str = "inline://wide_table.yaml") -> WideTableMetadata:
    return parse_wide_table_metadata(spec_path, payload)


def validate_wide_table_payload(metadata: WideTableMetadata, payload: dict[str, Any]) -> WideTableValidation:
    messages: list[str] = []
    if not metadata.wide_table_id:
        messages.append("wide_table.id missing")
    if not metadata.target.database:
        messages.append("wide_table.target.database missing")
    if not metadata.target.table:
        messages.append("wide_table.target.table missing")
    if not metadata.target.engine:
        messages.append("wide_table.target.engine missing")
    if not metadata.fields:
        messages.append("wide_table.fields missing")
    if not metadata.key_fields:
        messages.append("wide_table.key_fields missing")

    # Current exported sample in this repo contains graph_bundle but no materialization_bundle.
    materialization_bundle = payload.get("materialization_bundle")
    if materialization_bundle is None:
        messages.append("materialization_bundle missing")
    elif not isinstance(materialization_bundle, dict):
        messages.append("materialization_bundle invalid")
    else:
        if not isinstance(materialization_bundle.get("query_plan"), dict):
            messages.append("materialization_bundle.query_plan missing")
        if not isinstance(materialization_bundle.get("base_context"), dict):
            messages.append("materialization_bundle.base_context missing")
        preview_sql = materialization_bundle.get("preview_sql")
        if not isinstance(preview_sql, str) or not preview_sql.strip():
            messages.append("materialization_bundle.preview_sql missing")
    return WideTableValidation(ok=not messages, messages=tuple(messages))


def compute_wide_table_signature(payload: dict[str, Any]) -> str:
    wide_table = payload.get("wide_table") or {}
    target = wide_table.get("target") or {}
    signature_payload = {
        "target": {
            "database": target.get("database"),
            "table": target.get("table"),
            "engine": target.get("engine"),
            "partition_by": target.get("partition_by") or [],
            "order_by": target.get("order_by") or [],
            "version_field": target.get("version_field") or "",
        },
        "fields": wide_table.get("fields") or [],
        "key_fields": wide_table.get("key_fields") or [],
    }
    return _sha1_json(signature_payload)


def compute_plan_signature(payload: dict[str, Any]) -> str:
    materialization_bundle = payload.get("materialization_bundle")
    if isinstance(materialization_bundle, dict):
        signature_payload = {
            "query_plan": materialization_bundle.get("query_plan") or {},
            "base_context": materialization_bundle.get("base_context") or {},
        }
        return _sha1_json(signature_payload)

    # Fallback for current sample YAMLs that only expose graph_bundle.
    graph_bundle = payload.get("graph_bundle") or {}
    signature_payload = {
        "graph_bundle.scope": graph_bundle.get("scope") or {},
        "graph_bundle.snapshot": graph_bundle.get("snapshot") or {},
    }
    return _sha1_json(signature_payload)


def plan_wide_table_sync(
    metadata: WideTableMetadata,
    payload: dict[str, Any],
    *,
    target_exists: bool,
    previous_wide_table_signature: Optional[str] = None,
    previous_plan_signature: Optional[str] = None,
) -> WideTableSyncPlan:
    validation = validate_wide_table_payload(metadata, payload)
    wide_sig = compute_wide_table_signature(payload)
    plan_sig = compute_plan_signature(payload)

    if not validation.ok:
        return WideTableSyncPlan(
            spec_path=metadata.spec_path,
            wide_table_name=metadata.spec_name,
            target_database=metadata.target.database,
            target_table=metadata.target.table,
            wide_table_signature=wide_sig,
            plan_signature=plan_sig,
            action="invalid",
            validation=validation,
            reason="; ".join(validation.messages),
        )

    if not target_exists:
        return WideTableSyncPlan(
            spec_path=metadata.spec_path,
            wide_table_name=metadata.spec_name,
            target_database=metadata.target.database,
            target_table=metadata.target.table,
            wide_table_signature=wide_sig,
            plan_signature=plan_sig,
            action="create_and_sync",
            validation=validation,
            reason="target table missing",
        )

    if previous_wide_table_signature != wide_sig or previous_plan_signature != plan_sig:
        return WideTableSyncPlan(
            spec_path=metadata.spec_path,
            wide_table_name=metadata.spec_name,
            target_database=metadata.target.database,
            target_table=metadata.target.table,
            wide_table_signature=wide_sig,
            plan_signature=plan_sig,
            action="rebuild",
            validation=validation,
            reason="signature changed",
        )

    return WideTableSyncPlan(
        spec_path=metadata.spec_path,
        wide_table_name=metadata.spec_name,
        target_database=metadata.target.database,
        target_table=metadata.target.table,
        wide_table_signature=wide_sig,
        plan_signature=plan_sig,
        action="sync",
        validation=validation,
        reason="signatures unchanged",
    )


def _resolve_runtime_state_database(config: Any, state_database: Optional[str] = None) -> str:
    return str(
        state_database
        or getattr(config, "runtime_state_database", "")
        or DEFAULT_RUNTIME_STATE_DATABASE
    ).strip() or DEFAULT_RUNTIME_STATE_DATABASE


def run_wide_table_sync_payloads_with_clickhouse(
    payload_map: dict[str, dict[str, Any]],
    metadata_map: dict[str, WideTableMetadata],
    *,
    config: ClickHouseConfig,
    state_database: Optional[str] = None,
) -> list[WideTableRunResult]:
    connection = create_clickhouse_client(config)
    repository = WideTableSyncStateRepository(
        connection,
        database=_resolve_runtime_state_database(config, state_database),
    )
    try:
        repository.ensure_table()
        targets = [(meta.target.database, meta.target.table) for meta in metadata_map.values()]
        target_exists_lookup = repository.load_target_exists_lookup(targets)
        previous_signature_lookup = repository.load_previous_signature_lookup(targets)
        results: list[WideTableRunResult] = []
        for spec_path, metadata in metadata_map.items():
            payload = payload_map[spec_path]
            plan = plan_wide_table_sync(
                metadata,
                payload,
                target_exists=target_exists_lookup.get((metadata.target.database, metadata.target.table), False),
                previous_wide_table_signature=previous_signature_lookup.get(
                    (metadata.target.database, metadata.target.table),
                    (None, None),
                )[0],
                previous_plan_signature=previous_signature_lookup.get(
                    (metadata.target.database, metadata.target.table),
                    (None, None),
                )[1],
            )
            started_at = datetime.now(timezone.utc).replace(tzinfo=None)
            if not plan.validation.ok:
                status = "failed"
                message = plan.reason
            else:
                repository.save_state(
                    metadata,
                    wide_table_signature=plan.wide_table_signature,
                    plan_signature=plan.plan_signature,
                    last_status="running",
                    last_action=plan.action,
                    last_message=f"wide table execution started action={plan.action}",
                    last_started_at=started_at,
                    last_finished_at=None,
                )
                try:
                    _execute_wide_table_plan(connection, metadata, payload, plan)
                    status = "success"
                    message = f"wide table execution finished action={plan.action}"
                except Exception as exc:
                    status = "failed"
                    message = str(exc)
            finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
            repository.save_state(
                metadata,
                wide_table_signature=plan.wide_table_signature,
                plan_signature=plan.plan_signature,
                last_status=status,
                last_action=plan.action,
                last_message=message,
                last_started_at=started_at,
                last_finished_at=finished_at,
            )
            results.append(
                WideTableRunResult(
                    wide_table_name=metadata.spec_name,
                    action=plan.action,
                    status=status,
                    message=message,
                )
            )
        return results
    finally:
        connection.close()


def _execute_wide_table_plan(
    client: ClickHouseConnection,
    metadata: WideTableMetadata,
    payload: dict[str, Any],
    plan: WideTableSyncPlan,
) -> None:
    materialization_bundle = payload.get("materialization_bundle") or {}
    base_context = materialization_bundle.get("base_context") or {}
    preview_sql = str(materialization_bundle.get("preview_sql") or "").strip().rstrip(";")
    if not preview_sql:
        raise ValueError("materialization_bundle.preview_sql missing")

    base_table = str(base_context.get("base_table") or "").strip()
    target_ref = f"{metadata.target.database}.{metadata.target.table}"
    if base_table and target_ref == base_table:
        raise ValueError(
            f"target table {target_ref} equals base table {base_table}; refusing to overwrite source table"
        )

    client.command(f"CREATE DATABASE IF NOT EXISTS {metadata.target.database}")

    if plan.action in {"create_and_sync", "sync", "rebuild"}:
        _drop_target_table_if_exists(client, metadata)
        _create_target_table_from_select(client, metadata, preview_sql)
        _full_refresh_target_table(client, metadata, preview_sql, base_context=base_context)
        return

    raise ValueError(f"unsupported wide table plan action: {plan.action}")


def _drop_target_table_if_exists(
    client: ClickHouseConnection,
    metadata: WideTableMetadata,
) -> None:
    if not _target_table_exists(client, metadata.target.database, metadata.target.table):
        return
    client.command(f"DROP TABLE IF EXISTS {metadata.target.database}.{metadata.target.table}")


def _target_table_exists(
    client: ClickHouseConnection,
    database: str,
    table: str,
) -> bool:
    rows = client.query_rows(
        """
        SELECT database, name
        FROM system.tables
        WHERE database = {database:String}
          AND name = {table:String}
        LIMIT 1
        """,
        {"database": database, "table": table},
    )
    return bool(rows)


def _engine_clause(target: WideTableTarget) -> str:
    if target.engine == "Memory":
        return "Memory SETTINGS compress = 1"
    if target.engine == "ReplacingMergeTree":
        version_field = str(target.version_field or "").strip()
        version_expr = f"({version_field})" if version_field else ""
        partition_expr = f"PARTITION BY ({', '.join(target.partition_by)}) " if target.partition_by else ""
        order_expr = f"ORDER BY ({', '.join(target.order_by)})"
        return f"ReplacingMergeTree{version_expr} {partition_expr}{order_expr}".strip()
    raise ValueError(f"unsupported engine: {target.engine}")


def _create_target_table_from_select(
    client: ClickHouseConnection,
    metadata: WideTableMetadata,
    select_sql: str,
    *,
    target_ref: str | None = None,
) -> None:
    ref = target_ref or f"{metadata.target.database}.{metadata.target.table}"
    engine_clause = _engine_clause(metadata.target)
    client.command(
        f"CREATE TABLE IF NOT EXISTS {ref} "
        f"ENGINE = {engine_clause} "
        f"AS {select_sql} LIMIT 0"
    )


def _full_refresh_target_table(
    client: ClickHouseConnection,
    metadata: WideTableMetadata,
    select_sql: str,
    *,
    target_ref: str | None = None,
    base_context: dict[str, Any] | None = None,
) -> None:
    ref = target_ref or f"{metadata.target.database}.{metadata.target.table}"
    client.command(f"TRUNCATE TABLE {ref}")
    if base_context and _has_time_partition_context(base_context):
        _insert_select_by_month(client, ref, select_sql, base_context)
        return
    client.command(f"INSERT INTO {ref} {select_sql}")


def _rebuild_target_table(
    client: ClickHouseConnection,
    metadata: WideTableMetadata,
    select_sql: str,
    *,
    base_context: dict[str, Any] | None = None,
) -> None:
    target_ref = f"{metadata.target.database}.{metadata.target.table}"
    temp_ref = f"{metadata.target.database}.{metadata.target.table}__rebuild_tmp"
    backup_ref = f"{metadata.target.database}.{metadata.target.table}__rebuild_old"

    client.command(f"DROP TABLE IF EXISTS {temp_ref}")
    client.command(f"DROP TABLE IF EXISTS {backup_ref}")
    _create_target_table_from_select(client, metadata, select_sql, target_ref=temp_ref)
    _full_refresh_target_table(client, metadata, select_sql, target_ref=temp_ref, base_context=base_context)
    client.command(f"RENAME TABLE {target_ref} TO {backup_ref}, {temp_ref} TO {target_ref}")
    client.command(f"DROP TABLE IF EXISTS {backup_ref}")


def _has_time_partition_context(base_context: dict[str, Any]) -> bool:
    return bool(
        str(base_context.get("base_table") or "").strip()
        and str(base_context.get("time_key") or "").strip()
    )


def _insert_select_by_month(
    client: ClickHouseConnection,
    target_ref: str,
    select_sql: str,
    base_context: dict[str, Any],
) -> None:
    base_table = str(base_context.get("base_table") or "").strip()
    time_key = str(base_context.get("time_key") or "").strip()
    time_key_expression = str(base_context.get("time_key_expression") or time_key).strip()
    if not time_key:
        client.command(f"INSERT INTO {target_ref} {select_sql}")
        return

    month_rows = client.query_rows(
        f"""
        SELECT DISTINCT toYYYYMM({time_key_expression}) AS ym
        FROM {base_table}
        ORDER BY ym
        """
    )
    if not month_rows:
        client.command(f"INSERT INTO {target_ref} {select_sql} SETTINGS max_threads = 1")
        return

    for row in month_rows:
        if not row:
            continue
        ym = int(row[0])
        month_start, month_end = _month_window(ym)
        filtered_sql = _apply_month_window_to_preview_sql(select_sql, month_start, month_end, base_context)
        client.command(
            f"""
            INSERT INTO {target_ref}
            {filtered_sql}
            SETTINGS max_threads = 1
            """
        )


def _render_base_filters(base_filters: Sequence[dict[str, Any]]) -> list[str]:
    rendered: list[str] = []
    for item in base_filters:
        if not isinstance(item, dict):
            continue
        field = str(item.get("field") or "").strip()
        op = str(item.get("op") or "").strip()
        value = item.get("value")
        if not field or not op:
            continue
        if op == "=":
            rendered.append(f"{field} = {_sql_literal(value)}")
        elif op.lower() == "in" and isinstance(value, (list, tuple)):
            values = ", ".join(_sql_literal(v) for v in value)
            rendered.append(f"{field} IN ({values})")
    return rendered


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _month_window(yyyymm: int) -> tuple[str, str]:
    year = yyyymm // 100
    month = yyyymm % 100
    month_start = datetime(year, month, 1)
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    return month_start.strftime("%Y-%m-%d %H:%M:%S"), next_month.strftime("%Y-%m-%d %H:%M:%S")


def _apply_month_window_to_preview_sql(
    select_sql: str,
    month_start: str,
    month_end: str,
    base_context: dict[str, Any] | None = None,
) -> str:
    base_context = base_context or {}
    base_table = str(base_context.get("base_table") or "").strip()
    time_key_expression = str(base_context.get("time_key_expression") or base_context.get("time_key") or "").strip()
    generic_replacements: dict[str, str] = {}
    if base_table and time_key_expression:
        escaped_table = re.escape(base_table)
        generic_replacements[rf"FROM\s+{escaped_table}\s+b0"] = (
            f"FROM (SELECT * FROM {base_table} "
            f"WHERE {time_key_expression} >= toDate('{month_start[:10]}') "
            f"AND {time_key_expression} < toDate('{month_end[:10]}')) b0"
        )
    replacements = {
        **generic_replacements,
        r"FROM\s+starlight\.ad_market_kline_daily\s+b0": (
            "FROM (SELECT * FROM starlight.ad_market_kline_daily "
            f"WHERE trade_time >= '{month_start}' AND trade_time < '{month_end}') b0"
        ),
        r"ANY LEFT JOIN\s+starlight\.ad_history_stock_status\s+t1": (
            "ANY LEFT JOIN (SELECT * FROM starlight.ad_history_stock_status "
            f"WHERE trade_date >= toDate('{month_start}') AND trade_date < toDate('{month_end}')) t1"
        ),
        r"LEFT JOIN\s+starlight\.ad_history_stock_status\s+t3": (
            "LEFT JOIN (SELECT * FROM starlight.ad_history_stock_status "
            f"WHERE trade_date >= toDate('{month_start}') AND trade_date < toDate('{month_end}')) t3"
        ),
        r"ANY LEFT JOIN\s+starlight\.ad_backward_factor\s+t2": (
            "ANY LEFT JOIN (SELECT * FROM starlight.ad_backward_factor "
            f"WHERE trade_date >= toDate('{month_start}') AND trade_date < toDate('{month_end}')) t2"
        ),
        r"ASOF LEFT JOIN\s+starlight\.ad_equity_structure\s+t4": (
            "ASOF LEFT JOIN (SELECT * FROM starlight.ad_equity_structure "
            f"WHERE change_date < toDate('{month_end}')) t4"
        ),
        r"ASOF LEFT JOIN\s+starlight\.ad_stock_basic\s+t5": (
            "ASOF LEFT JOIN (SELECT * FROM starlight.ad_stock_basic "
            f"WHERE snapshot_date < toDate('{month_end}')) t5"
        ),
    }
    rewritten = select_sql
    for pattern, replacement in replacements.items():
        rewritten = re.sub(pattern, replacement, rewritten)
    rewritten = re.sub(
        r"ANY LEFT JOIN\s+starlight\.ad_backward_factor\s+(t\d+)",
        lambda match: (
            "ANY LEFT JOIN (SELECT * FROM starlight.ad_backward_factor "
            f"WHERE trade_date >= toDate('{month_start[:10]}') "
            f"AND trade_date < toDate('{month_end[:10]}')) {match.group(1)}"
        ),
        rewritten,
    )
    rewritten = re.sub(
        r"LEFT JOIN\s+starlight\.ad_history_stock_status\s+(t\d+)",
        lambda match: (
            "LEFT JOIN (SELECT * FROM starlight.ad_history_stock_status "
            f"WHERE trade_date >= toDate('{month_start[:10]}') "
            f"AND trade_date < toDate('{month_end[:10]}')) {match.group(1)}"
        ),
        rewritten,
    )
    rewritten = re.sub(
        r"ASOF LEFT JOIN\s+starlight\.ad_equity_structure\s+(t\d+)",
        lambda match: (
            "ASOF LEFT JOIN (SELECT * FROM starlight.ad_equity_structure "
            f"WHERE change_date < toDate('{month_end[:10]}')) {match.group(1)}"
        ),
        rewritten,
    )
    return rewritten


def wide_table_state_to_dict(state: WideTableSyncStateRow) -> dict[str, Any]:
    return {
        "wide_table_id": state.wide_table_id,
        "wide_table_name": state.wide_table_name,
        "source_node": state.source_node,
        "target_database": state.target_database,
        "target_table": state.target_table,
        "spec_path": state.spec_path,
        "wide_table_signature": state.wide_table_signature,
        "plan_signature": state.plan_signature,
        "last_status": state.last_status,
        "last_action": state.last_action,
        "last_message": state.last_message,
        "last_started_at": state.last_started_at.isoformat() if state.last_started_at is not None else None,
        "last_finished_at": state.last_finished_at.isoformat() if state.last_finished_at is not None else None,
        "updated_at": state.updated_at.isoformat(),
    }


def _sha1_json(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


__all__ = [
    "WideTableMetadata",
    "WideTableRunResult",
    "WideTableSyncPlan",
    "WideTableTarget",
    "WideTableValidation",
    "compute_plan_signature",
    "compute_wide_table_signature",
    "parse_wide_table_metadata",
    "plan_wide_table_sync",
    "run_wide_table_sync_payloads_with_clickhouse",
    "validate_wide_table_payload",
    "WideTableSyncStateRepository",
    "WideTableSyncStateRow",
    "DEFAULT_RUNTIME_STATE_DATABASE",
    "WIDE_TABLE_SYNC_STATE_TABLE",
    "wide_table_state_to_dict",
    "build_wide_table_metadata",
]
