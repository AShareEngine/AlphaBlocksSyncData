#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QMT ClickHouse persistence layer."""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any, Mapping, Sequence

from sync_data_system.providers.qmt.provider import iter_qmt_rows, normalize_qmt_code
from sync_data_system.providers.qmt.specs import QMT_TASK_SPECS, QmtTaskSpec, order_by_columns_for_spec
from sync_data_system.sync_core.clickhouse import ClickHouseConnection
from sync_data_system.sync_core.sync_models import SyncCheckpointRow, SyncTaskLogRow


logger = logging.getLogger(__name__)

QMT_SYNC_TASK_LOG_TABLE = "qmt_sync_task_log"
QMT_SYNC_CHECKPOINT_TABLE = "qmt_sync_checkpoint"


class QmtRepository:
    TASK_TABLE_COLUMNS = (
        "task",
        "symbol",
        "stock_code",
        "index_code",
        "market",
        "sector_name",
        "table_name",
        "period",
        "date",
        "time_ms",
        "request_start_time",
        "request_end_time",
        "payload_json",
    )
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
        for spec in QMT_TASK_SPECS.values():
            self.client.command(self._create_task_table_ddl(spec))

    def save_task_response(
        self,
        task: str,
        envelope: Mapping[str, Any],
        *,
        request_meta: Mapping[str, Any],
    ) -> int:
        spec = QMT_TASK_SPECS[task]
        rows = [
            self._row_tuple(row)
            for row in iter_qmt_rows(spec, envelope, request_meta)
        ]
        if not rows:
            return 0
        return self._insert_rows_in_batches(self._table_ref(spec.table_name), self.TASK_TABLE_COLUMNS, rows)

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

    def _row_tuple(self, row: Mapping[str, Any]) -> tuple[Any, ...]:
        return (
            str(row.get("task") or ""),
            normalize_qmt_code(row.get("symbol")),
            normalize_qmt_code(row.get("stock_code")),
            str(row.get("index_code") or ""),
            str(row.get("market") or ""),
            str(row.get("sector_name") or ""),
            str(row.get("table_name") or ""),
            str(row.get("period") or ""),
            str(row.get("date") or ""),
            int(row.get("time_ms") or 0),
            str(row.get("request_start_time") or ""),
            str(row.get("request_end_time") or ""),
            json.dumps(row.get("payload"), ensure_ascii=False, sort_keys=True, default=str),
        )

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

    def _create_task_table_ddl(self, spec: QmtTaskSpec) -> str:
        table = self._table_ref(spec.table_name)
        order_by = ", ".join(order_by_columns_for_spec(spec))
        return f"""
        CREATE TABLE IF NOT EXISTS {table}
        (
            task String,
            symbol String,
            stock_code String,
            index_code String,
            market String,
            sector_name String,
            table_name String,
            period String,
            date String,
            time_ms Int64,
            request_start_time String,
            request_end_time String,
            payload_json String,
            ingested_at DateTime64(3) DEFAULT now64(3)
        )
        ENGINE = ReplacingMergeTree(ingested_at)
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
