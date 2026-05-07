#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BaoStock ClickHouse 落库层."""

from __future__ import annotations

import logging
from datetime import date
from typing import Any, Mapping, Sequence

from sync_data_system.sync_core.clickhouse import ClickHouseConnection
from sync_data_system.sync_core.sync_models import SyncCheckpointRow, SyncTaskLogRow, to_ch_date
from sync_data_system.sources.baostock.provider import normalize_baostock_code
from sync_data_system.sources.baostock.specs import (
    BAOSTOCK_TASK_SPECS,
    BaoStockTaskSpec,
    order_by_columns_for_spec,
    request_columns_for_spec,
    table_columns_for_spec,
)


logger = logging.getLogger(__name__)

BS_SYNC_TASK_LOG_TABLE = "bs_sync_task_log"
BS_SYNC_CHECKPOINT_TABLE = "bs_sync_checkpoint"


class BaoStockRepository:
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
        database: str = "baostock",
        insert_batch_size: int = 5000,
    ) -> None:
        self.client = client
        self.database = str(database).strip() or "baostock"
        self.insert_batch_size = max(1, int(insert_batch_size))

    def ensure_tables(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(self._create_sync_task_log_ddl())
        self.client.command(self._create_sync_checkpoint_ddl())
        for spec in BAOSTOCK_TASK_SPECS.values():
            self.client.command(self._create_task_table_ddl(spec))

    def save_task_frame(
        self,
        task: str,
        frame,
        *,
        request_meta: Mapping[str, Any],
    ) -> int:
        spec = BAOSTOCK_TASK_SPECS[task]
        columns = table_columns_for_spec(spec)
        rows: list[tuple[Any, ...]] = []
        request_values = self._build_request_values(spec, request_meta)

        for _, row in frame.iterrows():
            record: dict[str, Any] = dict(request_values)
            for field, column in zip(spec.fields, spec.field_columns):
                value = self._stringify(row.get(field, ""))
                if field == "code":
                    value = normalize_baostock_code(value)
                record[column] = value
            rows.append(tuple(record.get(column) for column in columns))

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
        self.client.insert_rows(self._table_ref(BS_SYNC_TASK_LOG_TABLE), self.SYNC_TASK_LOG_COLUMNS, rows)
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
        self.client.insert_rows(self._table_ref(BS_SYNC_CHECKPOINT_TABLE), self.SYNC_CHECKPOINT_COLUMNS, rows)

    def has_successful_sync_today(self, task_name: str, scope_key: str, run_date: date) -> bool:
        sql = f"""
        SELECT count()
        FROM {self._table_ref(BS_SYNC_TASK_LOG_TABLE)}
        WHERE task_name = {{task_name:String}}
          AND scope_key = {{scope_key:String}}
          AND run_date = {{run_date:Date}}
          AND status = 'success'
        """
        count = self.client.query_value(sql, {"task_name": task_name, "scope_key": scope_key, "run_date": run_date})
        return bool(count)

    def has_task_data_for_request(self, task: str, request_meta: Mapping[str, Any]) -> bool:
        spec = BAOSTOCK_TASK_SPECS[task]
        clauses: list[str] = []
        parameters: dict[str, Any] = {}

        if spec.has_code_field and request_meta.get("code"):
            clauses.append("code = {code:String}")
            parameters["code"] = normalize_baostock_code(request_meta.get("code"))

        for column in request_columns_for_spec(spec):
            raw_value = request_meta.get(self._request_meta_key(column))
            value = self._stringify(raw_value)
            if not value:
                continue
            clauses.append(f"{column} = {{{column}:String}}")
            parameters[column] = value

        if not clauses:
            return False

        sql = f"""
        SELECT count()
        FROM {self._table_ref(spec.table_name)}
        WHERE {' AND '.join(clauses)}
        """
        count = self.client.query_value(sql, parameters)
        return bool(count)

    def load_latest_cursor(self, task: str, *, code: str | None = None) -> str | None:
        spec = BAOSTOCK_TASK_SPECS[task]
        if not spec.cursor_columns:
            return None

        expr = self._cursor_select_expr(spec)
        clauses: list[str] = []
        parameters: dict[str, Any] = {}
        if spec.has_code_field and code:
            clauses.append("code = {code:String}")
            parameters["code"] = normalize_baostock_code(code)
        for column in spec.cursor_columns:
            clauses.append(f"{column} != ''")
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
        SELECT {expr}
        FROM {self._table_ref(spec.table_name)}
        {where_sql}
        """
        value = self.client.query_value(sql, parameters)
        text = self._stringify(value)
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

    def _table_ref(self, table_name: str) -> str:
        return f"{self.database}.{table_name}"

    @staticmethod
    def _request_meta_key(column: str) -> str:
        mapping = {
            "query_date": "day",
            "request_year_type": "year_type",
        }
        return mapping[column]

    @staticmethod
    def _cursor_select_expr(spec: BaoStockTaskSpec) -> str:
        columns = spec.cursor_columns
        if spec.cursor_granularity in {"day", "year"} and len(columns) == 1:
            return f"max({columns[0]})"
        if spec.cursor_granularity == "month" and len(columns) == 2:
            year_col, month_col = columns
            return (
                f"max(concat({year_col}, '-', "
                f"if(length({month_col}) = 1, concat('0', {month_col}), {month_col})))"
            )
        if len(columns) == 1:
            return f"max({columns[0]})"
        joined = ", ".join(columns)
        raise ValueError(f"暂不支持的 BaoStock cursor 配置 task={spec.task} columns={joined}")

    def _build_request_values(self, spec: BaoStockTaskSpec, request_meta: Mapping[str, Any]) -> dict[str, Any]:
        values: dict[str, Any] = {}
        if spec.uses_day:
            values["query_date"] = self._stringify(request_meta.get("day", ""))
        if spec.uses_year_type:
            values["request_year_type"] = self._stringify(request_meta.get("year_type", ""))
        return values

    def _create_sync_task_log_ddl(self) -> str:
        table = self._table_ref(BS_SYNC_TASK_LOG_TABLE)
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
        table = self._table_ref(BS_SYNC_CHECKPOINT_TABLE)
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

    def _create_task_table_ddl(self, spec: BaoStockTaskSpec) -> str:
        table = self._table_ref(spec.table_name)
        column_defs: list[str] = []
        for column in table_columns_for_spec(spec):
            column_defs.append(f"{column} String")
        order_by = ", ".join(order_by_columns_for_spec(spec))
        columns_sql = ",\n            ".join(column_defs)
        return f"""
        CREATE TABLE IF NOT EXISTS {table}
        (
            {columns_sql}
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY ({order_by})
        """

    @staticmethod
    def _stringify(value: Any) -> Any:
        if value is None:
            return ""
        if isinstance(value, float):
            try:
                if value != value:
                    return ""
            except Exception:
                pass
        return str(value)


def to_sync_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return to_ch_date(text)
    except Exception:
        return None


__all__ = [
    "BS_SYNC_CHECKPOINT_TABLE",
    "BS_SYNC_TASK_LOG_TABLE",
    "BaoStockRepository",
    "to_sync_date",
]
