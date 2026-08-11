#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QMT ClickHouse persistence layer."""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any, Mapping, Sequence

from sync_data_system.providers.qmt.provider import iter_qmt_rows, normalize_qmt_code
from sync_data_system.providers.qmt.specs import (
    QMT_DYNAMIC_ROW_KINDS,
    QMT_TASK_SPECS,
    QmtTaskSpec,
    order_by_columns_for_spec,
)
from sync_data_system.sync_core.clickhouse import ClickHouseConnection
from sync_data_system.sync_core.sync_models import SyncCheckpointRow, SyncTaskLogRow


logger = logging.getLogger(__name__)

QMT_SYNC_TASK_LOG_TABLE = "qmt_sync_task_log"
QMT_SYNC_CHECKPOINT_TABLE = "qmt_sync_checkpoint"

COMMON_TASK_COLUMN_DEFINITIONS = (
    ("task", "String"),
    ("symbol", "String"),
    ("stock_code", "String"),
    ("index_code", "String"),
    ("market", "String"),
    ("sector_name", "String"),
    ("table_name", "String"),
    ("period", "String"),
    ("date", "String"),
    ("time_ms", "Int64"),
    ("request_start_time", "String"),
    ("request_end_time", "String"),
)

ROW_KIND_COLUMN_DEFINITIONS: dict[str, tuple[tuple[str, str], ...]] = {
    "bar": (
        ("open", "Float64"),
        ("high", "Float64"),
        ("low", "Float64"),
        ("close", "Float64"),
        ("volume", "Float64"),
        ("amount", "Float64"),
        ("settle", "Float64"),
        ("open_interest", "Float64"),
        ("pre_close", "Float64"),
        ("suspend_flag", "Int64"),
        ("extra_fields", "Map(String, String)"),
    ),
    "tick": (
        ("last_price", "Float64"),
        ("open", "Float64"),
        ("high", "Float64"),
        ("low", "Float64"),
        ("last_close", "Float64"),
        ("amount", "Float64"),
        ("volume", "Float64"),
        ("pvolume", "Float64"),
        ("open_int", "Float64"),
        ("stock_status", "Int64"),
        ("last_settlement_price", "Float64"),
        ("ask_price", "Array(Float64)"),
        ("bid_price", "Array(Float64)"),
        ("ask_vol", "Array(Float64)"),
        ("bid_vol", "Array(Float64)"),
        ("transaction_num", "Int64"),
        ("extra_fields", "Map(String, String)"),
    ),
    "quote": (
        ("last_price", "Float64"),
        ("open", "Float64"),
        ("high", "Float64"),
        ("low", "Float64"),
        ("last_close", "Float64"),
        ("amount", "Float64"),
        ("volume", "Float64"),
        ("pvolume", "Float64"),
        ("open_int", "Float64"),
        ("stock_status", "Int64"),
        ("last_settlement_price", "Float64"),
        ("ask_price", "Array(Float64)"),
        ("bid_price", "Array(Float64)"),
        ("ask_vol", "Array(Float64)"),
        ("bid_vol", "Array(Float64)"),
        ("transaction_num", "Int64"),
        ("extra_fields", "Map(String, String)"),
    ),
    "order": (
        ("price", "Float64"),
        ("volume", "Float64"),
        ("entrust_no", "Int64"),
        ("entrust_type", "Int64"),
        ("entrust_direction", "Int64"),
        ("extra_fields", "Map(String, String)"),
    ),
    "transaction": (
        ("price", "Float64"),
        ("volume", "Float64"),
        ("amount", "Float64"),
        ("trade_index", "Int64"),
        ("buy_no", "Int64"),
        ("sell_no", "Int64"),
        ("trade_type", "Int64"),
        ("trade_flag", "Int64"),
        ("extra_fields", "Map(String, String)"),
    ),
    "component": (
        ("weight", "Float64"),
        ("extra_fields", "Map(String, String)"),
    ),
}

DYNAMIC_COLUMN_DEFINITIONS = (
    ("record_index", "UInt32"),
    ("field_name", "String"),
    ("field_value", "String"),
)


class QmtRepository:
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
        database: str = "qmt",
        insert_batch_size: int = 5000,
    ) -> None:
        self.client = client
        self.database = str(database).strip() or "qmt"
        self.insert_batch_size = max(1, int(insert_batch_size))

    def ensure_tables(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(self._create_sync_task_log_ddl())
        self.client.command(self._create_sync_checkpoint_ddl())
        recreated_tasks: list[str] = []
        for spec in QMT_TASK_SPECS.values():
            self.client.command(self._create_task_table_ddl(spec))
            if self._recreate_outdated_task_table(spec):
                recreated_tasks.append(spec.task)
        if recreated_tasks:
            self.client.command(
                f"TRUNCATE TABLE {self._table_ref(QMT_SYNC_TASK_LOG_TABLE)}"
            )
            self.client.command(
                f"TRUNCATE TABLE {self._table_ref(QMT_SYNC_CHECKPOINT_TABLE)}"
            )
            logger.warning(
                "Cleared QMT sync logs and checkpoints after rebuilding %s business tables",
                len(recreated_tasks),
            )

    def _recreate_outdated_task_table(self, spec: QmtTaskSpec) -> bool:
        """Drop an old generic QMT table; callers intentionally resync it."""

        rows = self.client.query_rows(
            """
            SELECT name
            FROM system.columns
            WHERE database = {database:String}
              AND table = {table:String}
            """,
            {"database": self.database, "table": spec.table_name},
        )
        actual_columns = {str(row[0]) for row in rows if row}
        expected_columns = set(self.table_columns_for_spec(spec))
        if not actual_columns or actual_columns == expected_columns:
            return False

        table = self._table_ref(spec.table_name)
        logger.warning(
            "Recreating QMT table %s because its columns are outdated; existing rows will be resynced",
            table,
        )
        self.client.command(f"DROP TABLE IF EXISTS {table}")
        self.client.command(self._create_task_table_ddl(spec))
        return True

    def save_task_response(
        self,
        task: str,
        envelope: Mapping[str, Any],
        *,
        request_meta: Mapping[str, Any],
    ) -> int:
        spec = QMT_TASK_SPECS[task]
        columns = self.table_columns_for_spec(spec)
        rows = self._materialize_rows(
            spec,
            iter_qmt_rows(spec, envelope, request_meta),
            columns,
        )
        if not rows:
            return 0
        return self._insert_rows_in_batches(self._table_ref(spec.table_name), columns, rows)

    def insert_sync_log(self, row: SyncTaskLogRow) -> None:
        rows = [(
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
        )]
        self.client.insert_rows(self._table_ref(QMT_SYNC_TASK_LOG_TABLE), self.SYNC_TASK_LOG_COLUMNS, rows)
        checkpoint_date = row.end_date or row.start_date
        self.upsert_sync_checkpoint(
            SyncCheckpointRow(
                task_name=row.task_name,
                scope_key=row.scope_key,
                run_date=row.run_date,
                status=row.status,
                target_table=row.target_table,
                checkpoint_date=checkpoint_date,
                row_count=row.row_count,
                message=row.message,
                finished_at=row.finished_at,
            )
        )

    def upsert_sync_checkpoint(self, row: SyncCheckpointRow) -> None:
        rows = [(
            row.task_name,
            row.scope_key,
            row.run_date,
            row.status,
            row.target_table,
            row.checkpoint_date,
            row.row_count,
            row.message,
            row.finished_at,
        )]
        self.client.insert_rows(self._table_ref(QMT_SYNC_CHECKPOINT_TABLE), self.SYNC_CHECKPOINT_COLUMNS, rows)

    def has_successful_sync_today(self, task_name: str, scope_key: str, run_date: date) -> bool:
        sql = f"""
        SELECT count()
        FROM {self._table_ref(QMT_SYNC_TASK_LOG_TABLE)}
        WHERE task_name = {{task_name:String}}
          AND scope_key = {{scope_key:String}}
          AND run_date = {{run_date:Date}}
          AND status = 'success'
        """
        count = self.client.query_value(sql, {"task_name": task_name, "scope_key": scope_key, "run_date": run_date})
        return bool(count)

    def has_task_data_for_request(self, task: str, request_meta: Mapping[str, Any]) -> bool:
        spec = QMT_TASK_SPECS[task]
        clauses: list[str] = []
        parameters: dict[str, Any] = {"task": task}

        clauses.append("task = {task:String}")
        for key, column in (
            ("symbol", "symbol"),
            ("stock_code", "stock_code"),
            ("index_code", "index_code"),
            ("market", "market"),
            ("sector_name", "sector_name"),
            ("table_name", "table_name"),
            ("period", "period"),
            ("start_time", "request_start_time"),
            ("end_time", "request_end_time"),
        ):
            value = self._normalize_lookup_value(key, request_meta.get(key))
            if value == "":
                continue
            clauses.append(f"{column} = {{{column}:String}}")
            parameters[column] = value

        if len(clauses) <= 1:
            return False

        sql = f"""
        SELECT count()
        FROM {self._table_ref(spec.table_name)}
        WHERE {' AND '.join(clauses)}
        """
        count = self.client.query_value(sql, parameters)
        return bool(count)

    def load_latest_cursor(self, task: str, *, symbol: str | None = None) -> str | None:
        spec = QMT_TASK_SPECS[task]
        if not spec.cursor_path:
            return None
        clauses = ["task = {task:String}"]
        parameters: dict[str, Any] = {"task": task}
        if symbol:
            clauses.append("symbol = {symbol:String}")
            parameters["symbol"] = normalize_qmt_code(symbol)
        column = "time_ms" if spec.cursor_path == ("time_ms",) else "date"
        if column == "date":
            clauses.append("date != ''")
        else:
            clauses.append("time_ms != 0")
        sql = f"""
        SELECT max({column})
        FROM {self._table_ref(spec.table_name)}
        WHERE {' AND '.join(clauses)}
        """
        value = self.client.query_value(sql, parameters)
        text = str(value or "").strip()
        return text or None

    def _insert_rows_in_batches(
        self,
        table: str,
        columns: Sequence[str],
        rows: Sequence[Sequence[Any]],
    ) -> int:
        total = 0
        batch: list[Sequence[Any]] = []
        for row in rows:
            batch.append(row)
            if len(batch) >= self.insert_batch_size:
                self.client.insert_rows(table, columns, batch)
                total += len(batch)
                logger.info("Inserted %s rows into %s", len(batch), table)
                batch = []
        if batch:
            self.client.insert_rows(table, columns, batch)
            total += len(batch)
            logger.info("Inserted %s rows into %s", len(batch), table)
        return total

    @classmethod
    def table_column_definitions_for_spec(
        cls,
        spec: QmtTaskSpec,
    ) -> tuple[tuple[str, str], ...]:
        if spec.row_kind in QMT_DYNAMIC_ROW_KINDS:
            business = DYNAMIC_COLUMN_DEFINITIONS
        else:
            business = ROW_KIND_COLUMN_DEFINITIONS.get(spec.row_kind, ())
        return (*COMMON_TASK_COLUMN_DEFINITIONS, *business)

    @classmethod
    def table_columns_for_spec(cls, spec: QmtTaskSpec) -> tuple[str, ...]:
        return tuple(name for name, _ in cls.table_column_definitions_for_spec(spec))

    def _materialize_rows(
        self,
        spec: QmtTaskSpec,
        source_rows: Sequence[Mapping[str, Any]],
        columns: Sequence[str],
    ) -> list[tuple[Any, ...]]:
        materialized: list[tuple[Any, ...]] = []
        for record_index, source in enumerate(source_rows):
            base = self._common_values(source)
            payload = source.get("payload")
            if spec.row_kind in QMT_DYNAMIC_ROW_KINDS:
                fields = self._flatten_payload(payload)
                if not fields:
                    fields = [("value", self._string_value(payload))]
                for field_name, field_value in fields:
                    values = {
                        **base,
                        "record_index": record_index,
                        "field_name": field_name,
                        "field_value": field_value,
                    }
                    materialized.append(tuple(values.get(column, "") for column in columns))
                continue

            business_definitions = ROW_KIND_COLUMN_DEFINITIONS.get(spec.row_kind, ())
            business_names = {name for name, _ in business_definitions}
            payload_map = payload if isinstance(payload, Mapping) else {}
            values = dict(base)
            for name, column_type in business_definitions:
                if name == "extra_fields":
                    continue
                values[name] = self._coerce_value(payload_map.get(name), column_type)
            if "extra_fields" in business_names:
                ignored = business_names | {"symbol", "date", "time_ms"}
                values["extra_fields"] = {
                    name: value
                    for name, value in self._flatten_payload(payload_map)
                    if self._field_root(name) not in ignored
                }
            materialized.append(tuple(values.get(column, self._default_value(column)) for column in columns))
        return materialized

    @staticmethod
    def _common_values(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "task": str(row.get("task") or ""),
            "symbol": normalize_qmt_code(row.get("symbol")),
            "stock_code": normalize_qmt_code(row.get("stock_code")),
            "index_code": str(row.get("index_code") or ""),
            "market": str(row.get("market") or ""),
            "sector_name": str(row.get("sector_name") or ""),
            "table_name": str(row.get("table_name") or ""),
            "period": str(row.get("period") or ""),
            "date": str(row.get("date") or ""),
            "time_ms": int(row.get("time_ms") or 0),
            "request_start_time": str(row.get("request_start_time") or ""),
            "request_end_time": str(row.get("request_end_time") or ""),
        }

    @classmethod
    def _flatten_payload(cls, value: Any, prefix: str = "") -> list[tuple[str, str]]:
        if isinstance(value, Mapping):
            if not value:
                return [(prefix or "value", "{}")]
            fields: list[tuple[str, str]] = []
            for key, item in value.items():
                name = f"{prefix}.{key}" if prefix else str(key)
                fields.extend(cls._flatten_payload(item, name))
            return fields
        if isinstance(value, (list, tuple)):
            if not value:
                return [(prefix or "value", "[]")]
            fields = []
            for index, item in enumerate(value):
                name = f"{prefix}[{index}]" if prefix else f"value[{index}]"
                fields.extend(cls._flatten_payload(item, name))
            return fields
        return [(prefix or "value", cls._string_value(value))]

    @staticmethod
    def _string_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        return str(value)

    @staticmethod
    def _field_root(name: str) -> str:
        return str(name).split(".", 1)[0].split("[", 1)[0]

    @classmethod
    def _coerce_value(cls, value: Any, column_type: str) -> Any:
        if column_type == "Float64":
            try:
                return float(value or 0)
            except (TypeError, ValueError):
                return 0.0
        if column_type in {"Int64", "UInt32"}:
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0
        if column_type == "Array(Float64)":
            if not isinstance(value, (list, tuple)):
                return []
            result: list[float] = []
            for item in value:
                try:
                    result.append(float(item))
                except (TypeError, ValueError):
                    continue
            return result
        if column_type == "Map(String, String)":
            return {}
        return cls._string_value(value)

    @staticmethod
    def _default_value(column: str) -> Any:
        if column == "extra_fields":
            return {}
        return ""

    def _table_ref(self, table_name: str) -> str:
        return f"{self.database}.{table_name}"

    def _create_sync_task_log_ddl(self) -> str:
        table = self._table_ref(QMT_SYNC_TASK_LOG_TABLE)
        return f"""
        CREATE TABLE IF NOT EXISTS {table}
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
        ENGINE = ReplacingMergeTree(finished_at)
        PARTITION BY toYYYYMM(run_date)
        ORDER BY (task_name, scope_key, run_date, finished_at)
        """

    def _create_sync_checkpoint_ddl(self) -> str:
        table = self._table_ref(QMT_SYNC_CHECKPOINT_TABLE)
        return f"""
        CREATE TABLE IF NOT EXISTS {table}
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
        PARTITION BY toYYYYMM(run_date)
        ORDER BY (task_name, scope_key, run_date, finished_at)
        """

    def _create_task_table_ddl(
        self,
        spec: QmtTaskSpec,
        *,
        table_name: str | None = None,
    ) -> str:
        table = self._table_ref(table_name or spec.table_name)
        order_by = ", ".join(order_by_columns_for_spec(spec))
        columns = ",\n            ".join(
            f"{name} {column_type}"
            for name, column_type in self.table_column_definitions_for_spec(spec)
        )
        return f"""
        CREATE TABLE IF NOT EXISTS {table}
        (
            {columns}
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY ({order_by})
        """

    @staticmethod
    def _normalize_lookup_value(key: str, value: Any) -> str:
        if key in {"symbol", "stock_code"}:
            return normalize_qmt_code(value)
        return str(value or "").strip()


__all__ = [
    "QMT_SYNC_CHECKPOINT_TABLE",
    "QMT_SYNC_TASK_LOG_TABLE",
    "QmtRepository",
]
