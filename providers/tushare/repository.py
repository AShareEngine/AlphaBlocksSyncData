#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ClickHouse persistence for the metadata-driven Tushare provider."""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Any, Mapping, Sequence

from sync_data_system.providers.tushare.specs import TushareTaskSpec
from sync_data_system.sync_core.clickhouse import ClickHouseConnection
from sync_data_system.sync_core.sync_models import SyncCheckpointRow, SyncTaskLogRow


logger = logging.getLogger(__name__)

TS_SYNC_TASK_LOG_TABLE = "ts_sync_task_log"
TS_SYNC_CHECKPOINT_TABLE = "ts_sync_checkpoint"
SAFE_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
LEGACY_META_COLUMNS = (
    "_row_hash",
    "_scope_key",
    "_cursor_value",
    "_ingested_at",
)


class TushareRepository:
    SYNC_TASK_LOG_COLUMNS = (
        "task_name",
        "scope_key",
        "run_date",
        "status",
        "target_table",
        "start_date",
        "end_date",
        "row_count",
        "message",
        "started_at",
        "finished_at",
    )
    SYNC_CHECKPOINT_COLUMNS = (
        "task_name",
        "scope_key",
        "run_date",
        "status",
        "target_table",
        "checkpoint_date",
        "row_count",
        "message",
        "finished_at",
    )

    def __init__(
        self,
        client: ClickHouseConnection,
        *,
        database: str = "tushare",
        insert_batch_size: int = 5000,
    ) -> None:
        self.client = client
        self.database = _safe_identifier(database)
        self.insert_batch_size = max(1, int(insert_batch_size))
        self._ensured_tables: set[str] = set()

    def ensure_tables(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(self._create_sync_task_log_ddl())
        self.client.command(self._create_sync_checkpoint_ddl())
        self._ensure_table_layout(
            TS_SYNC_TASK_LOG_TABLE,
            engine="MergeTree",
            key_fields=("run_date", "task_name", "started_at", "scope_key"),
            partition_by="toYYYYMM(run_date)",
        )
        self._ensure_table_layout(
            TS_SYNC_CHECKPOINT_TABLE,
            engine="ReplacingMergeTree",
            key_fields=("task_name", "scope_key"),
        )

    def ensure_task_table(
        self,
        spec: TushareTaskSpec,
        *,
        observed_fields: Sequence[str] = (),
    ) -> None:
        field_names = _dedupe_fields(
            (*spec.output_names, *spec.business_key_fields, *observed_fields)
        )
        if spec.table_name not in self._ensured_tables:
            self.client.command(self._create_task_table_ddl(spec, field_names))
            self._ensure_no_legacy_meta_columns(spec)
            self._ensure_table_layout(
                spec.table_name,
                engine="ReplacingMergeTree",
                key_fields=spec.business_key_fields,
            )
            self._ensured_tables.add(spec.table_name)
        for field in observed_fields:
            normalized = _safe_identifier(field)
            if normalized not in spec.output_names:
                self.client.command(
                    f"ALTER TABLE {self._table_ref(spec.table_name)} "
                    f"ADD COLUMN IF NOT EXISTS {_quote_identifier(normalized)} String"
                )

    def save_rows(
        self,
        spec: TushareTaskSpec,
        rows: Sequence[Mapping[str, Any]],
        *,
        scope_key: str,
    ) -> int:
        if not rows:
            return 0
        observed_fields = _dedupe_fields(
            tuple(str(field) for row in rows for field in row)
        )
        self.ensure_task_table(spec, observed_fields=observed_fields)
        columns = _dedupe_fields((*spec.output_names, *observed_fields))
        del scope_key
        insert_rows = [
            tuple(_stringify(row.get(column)) for column in columns)
            for row in rows
        ]
        return self._insert_rows_in_batches(
            self._table_ref(spec.table_name),
            columns,
            insert_rows,
        )

    def load_latest_cursor(
        self,
        spec: TushareTaskSpec,
        *,
        code: str = "",
    ) -> str | None:
        if not spec.cursor_field:
            return None
        self.ensure_task_table(spec)
        cursor_column = _quote_identifier(spec.cursor_field)
        clauses = [f"{cursor_column} != ''"]
        params: dict[str, Any] = {}
        if code and spec.code_field:
            clauses.append(f"{_quote_identifier(spec.code_field)} = {{code:String}}")
            params["code"] = str(code).strip()
        value = self.client.query_value(
            f"""
            SELECT max({cursor_column})
            FROM {self._table_ref(spec.table_name)}
            WHERE {' AND '.join(clauses)}
            """,
            params,
        )
        text = _stringify(value)
        return text or None

    def load_latest_cursors(
        self,
        spec: TushareTaskSpec,
        codes: Sequence[str],
    ) -> dict[str, str]:
        normalized_codes = list(dict.fromkeys(str(code).strip() for code in codes if str(code).strip()))
        if not spec.cursor_field or not spec.code_field or not normalized_codes:
            return {}
        self.ensure_task_table(spec)
        code_column = _quote_identifier(spec.code_field)
        cursor_column = _quote_identifier(spec.cursor_field)
        rows = self.client.query_rows(
            f"""
            SELECT {code_column}, max({cursor_column})
            FROM {self._table_ref(spec.table_name)}
            WHERE {code_column} IN {{codes:Array(String)}}
              AND {cursor_column} != ''
            GROUP BY {code_column}
            """,
            {"codes": normalized_codes},
        )
        return {
            str(row[0]).strip(): _stringify(row[1])
            for row in rows
            if len(row) >= 2 and str(row[0]).strip() and _stringify(row[1])
        }

    def load_universe_codes(self, spec: TushareTaskSpec) -> list[str]:
        """Load a previously synchronized security universe from ClickHouse."""

        code_field = spec.code_field
        if not code_field and "ts_code" in spec.output_names:
            code_field = "ts_code"
        if not code_field:
            return []
        self.ensure_task_table(spec)
        code_column = _quote_identifier(code_field)
        rows = self.client.query_rows(
            f"""
            SELECT DISTINCT {code_column}
            FROM {self._table_ref(spec.table_name)}
            WHERE {code_column} != ''
            ORDER BY {code_column}
            """
        )
        return sorted(
            {
                str(row[0]).strip().upper()
                for row in rows
                if row and str(row[0]).strip()
            }
        )

    def insert_sync_log(self, row: SyncTaskLogRow) -> None:
        self.client.insert_rows(
            self._table_ref(TS_SYNC_TASK_LOG_TABLE),
            self.SYNC_TASK_LOG_COLUMNS,
            [
                (
                    row.task_name,
                    row.scope_key,
                    row.run_date,
                    row.status,
                    row.target_table,
                    row.start_date,
                    row.end_date,
                    row.row_count,
                    row.message,
                    row.started_at,
                    row.finished_at,
                )
            ],
        )
        self.upsert_sync_checkpoint(
            SyncCheckpointRow(
                task_name=row.task_name,
                scope_key=row.scope_key,
                run_date=row.run_date,
                status=row.status,
                target_table=row.target_table,
                checkpoint_date=row.end_date or row.start_date,
                row_count=row.row_count,
                message=row.message,
                finished_at=row.finished_at,
            )
        )

    def upsert_sync_checkpoint(self, row: SyncCheckpointRow) -> None:
        self.client.insert_rows(
            self._table_ref(TS_SYNC_CHECKPOINT_TABLE),
            self.SYNC_CHECKPOINT_COLUMNS,
            [
                (
                    row.task_name,
                    row.scope_key,
                    row.run_date,
                    row.status,
                    row.target_table,
                    row.checkpoint_date,
                    row.row_count,
                    row.message,
                    row.finished_at,
                )
            ],
        )

    def has_successful_sync_today(self, task_name: str, scope_key: str, run_date: date) -> bool:
        count = self.client.query_value(
            f"""
            SELECT count()
            FROM {self._table_ref(TS_SYNC_TASK_LOG_TABLE)}
            WHERE task_name = {{task_name:String}}
              AND scope_key = {{scope_key:String}}
              AND run_date = {{run_date:Date}}
              AND status = 'success'
            """,
            {
                "task_name": task_name,
                "scope_key": scope_key,
                "run_date": run_date,
            },
        )
        return bool(count)

    def _insert_rows_in_batches(
        self,
        table: str,
        columns: Sequence[str],
        rows: Sequence[Sequence[Any]],
    ) -> int:
        total = 0
        for offset in range(0, len(rows), self.insert_batch_size):
            batch = rows[offset : offset + self.insert_batch_size]
            self.client.insert_rows(table, columns, batch)
            total += len(batch)
            logger.info("Inserted %s rows into %s", len(batch), table)
        return total

    def _table_ref(self, table_name: str) -> str:
        return (
            f"{_quote_identifier(self.database)}."
            f"{_quote_identifier(_safe_identifier(table_name))}"
        )

    def _ensure_no_legacy_meta_columns(self, spec: TushareTaskSpec) -> None:
        rows = self.client.query_rows(
            """
            SELECT name
            FROM system.columns
            WHERE database = {database:String}
              AND table = {table:String}
              AND name IN {columns:Array(String)}
            """,
            {
                "database": self.database,
                "table": spec.table_name,
                "columns": list(LEGACY_META_COLUMNS),
            },
        )
        found = sorted(str(row[0]) for row in rows if row)
        if not found:
            return
        raise RuntimeError(
            f"Tushare table {self.database}.{spec.table_name} still contains legacy "
            f"internal columns: {', '.join(found)}. Run "
            "`python3 scripts/migrate_tushare_remove_internal_columns.py` before syncing."
        )

    def _ensure_table_layout(
        self,
        table_name: str,
        *,
        engine: str,
        key_fields: Sequence[str],
        partition_by: str = "",
    ) -> None:
        rows = self.client.query_rows(
            """
            SELECT engine, sorting_key, primary_key, partition_key
            FROM system.tables
            WHERE database = {database:String}
              AND name = {table:String}
            """,
            {"database": self.database, "table": table_name},
        )
        if not rows:
            # Some test doubles and restricted ClickHouse accounts cannot read
            # system.tables.  CREATE TABLE remains authoritative in that case.
            return
        actual_engine, sorting_key, primary_key, partition_key = (
            str(value) for value in rows[0][:4]
        )
        expected_key = _normalized_key_expression(key_fields)
        if (
            actual_engine == engine
            and _normalize_key_expression(sorting_key) == expected_key
            and _normalize_key_expression(primary_key) == expected_key
            and _normalize_key_expression(partition_key)
            == _normalize_key_expression(partition_by)
        ):
            return
        raise RuntimeError(
            f"Tushare table {self.database}.{table_name} has an outdated "
            f"MergeTree layout: engine={actual_engine}, sorting_key={sorting_key!r}, "
            f"primary_key={primary_key!r}, partition_key={partition_key!r}; "
            f"expected engine={engine}, key={','.join(key_fields)}, "
            f"partition_key={partition_by!r}. Run "
            "`python3 scripts/migrate_tushare_remove_internal_columns.py` before syncing."
        )

    def _create_task_table_ddl(
        self,
        spec: TushareTaskSpec,
        field_names: Sequence[str],
    ) -> str:
        column_defs = [
            f"{_quote_identifier(_safe_identifier(field))} String"
            for field in field_names
        ]
        columns_sql = ",\n            ".join(column_defs)
        key_values = ", ".join(
            _quote_identifier(_safe_identifier(field))
            for field in spec.business_key_fields
        )
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_ref(spec.table_name)}
        (
            {columns_sql}
        )
        ENGINE = ReplacingMergeTree()
        PRIMARY KEY ({key_values})
        ORDER BY ({key_values})
        """

    def _create_sync_task_log_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_ref(TS_SYNC_TASK_LOG_TABLE)}
        (
            task_name String,
            scope_key String,
            run_date Date,
            status String,
            target_table String,
            start_date Nullable(Date),
            end_date Nullable(Date),
            row_count Int64,
            message Nullable(String),
            started_at DateTime64(3),
            finished_at DateTime64(3)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(run_date)
        PRIMARY KEY (run_date, task_name, started_at, scope_key)
        ORDER BY (run_date, task_name, started_at, scope_key)
        """

    def _create_sync_checkpoint_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_ref(TS_SYNC_CHECKPOINT_TABLE)}
        (
            task_name String,
            scope_key String,
            run_date Date,
            status String,
            target_table String,
            checkpoint_date Nullable(Date),
            row_count Int64,
            message Nullable(String),
            finished_at DateTime64(3)
        )
        ENGINE = ReplacingMergeTree(finished_at)
        PRIMARY KEY (task_name, scope_key)
        ORDER BY (task_name, scope_key)
        """


def _dedupe_fields(fields: Sequence[str]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for field in fields:
        normalized = _safe_identifier(field)
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return tuple(result)


def _safe_identifier(value: Any) -> str:
    text = str(value or "").strip()
    if not SAFE_IDENTIFIER_RE.fullmatch(text):
        raise ValueError(f"unsafe ClickHouse identifier: {value!r}")
    return text


def _quote_identifier(value: Any) -> str:
    return f"`{_safe_identifier(value)}`"


def _normalized_key_expression(fields: Sequence[str]) -> str:
    return ",".join(_safe_identifier(field) for field in fields)


def _normalize_key_expression(value: Any) -> str:
    text = re.sub(r"[\s`]", "", str(value or ""))
    while text.startswith("tuple(") and text.endswith(")"):
        text = text[6:-1]
    return text


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return str(value)


__all__ = [
    "LEGACY_META_COLUMNS",
    "TS_SYNC_CHECKPOINT_TABLE",
    "TS_SYNC_TASK_LOG_TABLE",
    "TushareRepository",
]
