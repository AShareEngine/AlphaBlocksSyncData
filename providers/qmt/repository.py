#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""QMT ClickHouse persistence layer."""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any, Iterable, Mapping, Sequence

from sync_data_system.providers.qmt.provider import iter_qmt_rows, normalize_qmt_code
from sync_data_system.providers.qmt.specs import QMT_TASK_SPECS, QmtTaskSpec, order_by_columns_for_spec
from sync_data_system.sync_core.clickhouse import ClickHouseConnection
from sync_data_system.sync_core.sync_models import SyncCheckpointRow, SyncTaskLogRow


logger = logging.getLogger(__name__)

QMT_SYNC_TASK_LOG_TABLE = "qmt_sync_task_log"
QMT_SYNC_CHECKPOINT_TABLE = "qmt_sync_checkpoint"
DYNAMIC_ROW_KINDS = frozenset(
    {"financial", "instrument", "dynamic_fields", "type", "frame"}
)
LEGACY_GENERIC_COLUMNS = frozenset(
    {
        "source",
        "fetched_at",
        "ingested_at",
        "task",
        "request_start_time",
        "request_end_time",
        "record_index",
        "field_name",
        "field_value",
        "extra_fields",
    }
)

ROW_KIND_COLUMN_DEFINITIONS: dict[str, tuple[tuple[str, str], ...]] = {
    "bar": (
        ("symbol", "String"),
        ("time_ms", "Int64"),
        ("open", "Nullable(Float64)"),
        ("high", "Nullable(Float64)"),
        ("low", "Nullable(Float64)"),
        ("close", "Nullable(Float64)"),
        ("volume", "Nullable(Int64)"),
        ("amount", "Nullable(Float64)"),
        ("settle", "Nullable(Float64)"),
        ("open_interest", "Nullable(Int64)"),
        ("pre_close", "Nullable(Float64)"),
        ("suspend_flag", "Nullable(Int64)"),
    ),
    "tick": (
        ("symbol", "String"),
        ("time_ms", "Int64"),
        ("last_price", "Float64"),
        ("open", "Float64"),
        ("high", "Float64"),
        ("low", "Float64"),
        ("last_close", "Float64"),
        ("amount", "Float64"),
        ("volume", "Int64"),
        ("pvolume", "Int64"),
        ("open_int", "Int64"),
        ("stock_status", "Int64"),
        ("last_settlement_price", "Float64"),
        ("ask_price", "Array(Float64)"),
        ("bid_price", "Array(Float64)"),
        ("ask_vol", "Array(Int64)"),
        ("bid_vol", "Array(Int64)"),
        ("transaction_num", "Int64"),
    ),
    "quote": (
        ("symbol", "String"),
        ("time_ms", "Int64"),
        ("last_price", "Float64"),
        ("open", "Float64"),
        ("high", "Float64"),
        ("low", "Float64"),
        ("last_close", "Float64"),
        ("amount", "Float64"),
        ("volume", "Int64"),
        ("pvolume", "Int64"),
        ("open_int", "Int64"),
        ("stock_status", "Int64"),
        ("last_settlement_price", "Float64"),
        ("ask_price", "Array(Float64)"),
        ("bid_price", "Array(Float64)"),
        ("ask_vol", "Array(Int64)"),
        ("bid_vol", "Array(Int64)"),
        ("transaction_num", "Int64"),
    ),
    "order": (
        ("symbol", "String"),
        ("time_ms", "Int64"),
        ("price", "Float64"),
        ("volume", "Int64"),
        ("entrust_no", "Int64"),
        ("entrust_type", "Int64"),
        ("entrust_direction", "Int64"),
    ),
    "transaction": (
        ("symbol", "String"),
        ("time_ms", "Int64"),
        ("price", "Float64"),
        ("volume", "Int64"),
        ("amount", "Float64"),
        ("trade_index", "Int64"),
        ("buy_no", "Int64"),
        ("sell_no", "Int64"),
        ("trade_type", "Int64"),
        ("trade_flag", "Int64"),
    ),
    "component": (
        ("index_code", "String"),
        ("symbol", "String"),
        ("weight", "Float64"),
    ),
    "sector": (("sector_name", "String"), ("symbols", "Array(String)")),
    "financial": (
        ("symbol", "String"),
        ("table_name", "String"),
        ("index", "Int64"),
        ("m_timetag", "String"),
        ("m_anntime", "String"),
    ),
    "instrument": (
        ("symbol", "String"),
        ("ExchangeID", "Nullable(String)"),
        ("InstrumentID", "Nullable(String)"),
        ("InstrumentName", "Nullable(String)"),
        ("ProductID", "Nullable(String)"),
        ("ProductName", "Nullable(String)"),
        ("ProductType", "Nullable(String)"),
        ("ExchangeCode", "Nullable(String)"),
        ("UniCode", "Nullable(String)"),
        ("CreateDate", "Nullable(String)"),
        ("OpenDate", "Nullable(String)"),
        ("ExpireDate", "Nullable(String)"),
        ("PreClose", "Nullable(Float64)"),
        ("SettlementPrice", "Nullable(Float64)"),
        ("UpStopPrice", "Nullable(Float64)"),
        ("DownStopPrice", "Nullable(Float64)"),
        ("FloatVolume", "Nullable(Float64)"),
        ("TotalVolume", "Nullable(Float64)"),
        ("LongMarginRatio", "Nullable(Float64)"),
        ("ShortMarginRatio", "Nullable(Float64)"),
        ("PriceTick", "Nullable(Float64)"),
        ("VolumeMultiple", "Nullable(Int64)"),
        ("MainContract", "Nullable(Int64)"),
        ("LastVolume", "Nullable(Int64)"),
        ("InstrumentStatus", "Nullable(Int64)"),
        ("IsTrading", "Nullable(Bool)"),
        ("IsRecent", "Nullable(Bool)"),
        ("ProductTradeQuota", "Nullable(Float64)"),
        ("ContractTradeQuota", "Nullable(Float64)"),
        ("ProductOpenInterestQuota", "Nullable(Float64)"),
        ("ContractOpenInterestQuota", "Nullable(Float64)"),
    ),
    "dynamic_fields": (("symbol", "String"),),
    "type": (("symbol", "String"),),
    "trade_times": (("symbol", "String"), ("trade_times", "Array(Array(Int64))")),
    "main_contract": (("code_market", "String"), ("main_contract", "String")),
    "calendar_date": (("market", "String"), ("date", "String")),
    "frame": (
        ("symbol", "String"),
        ("index", "String"),
        ("time", "Int64"),
        ("open", "Nullable(Float64)"),
        ("high", "Nullable(Float64)"),
        ("low", "Nullable(Float64)"),
        ("close", "Nullable(Float64)"),
        ("volume", "Nullable(Int64)"),
        ("amount", "Nullable(Float64)"),
        ("openInterest", "Nullable(Int64)"),
        ("preClose", "Nullable(Float64)"),
        ("settelementPrice", "Nullable(Float64)"),
        ("suspendFlag", "Nullable(Int64)"),
    ),
    "holiday": (("date", "String"),),
    "period": (("period", "String"),),
    "data_dir": (("data_dir", "String"),),
    "factor": (
        ("stock_code", "String"),
        ("index", "String"),
        ("time", "Int64"),
        ("interest", "Nullable(Float64)"),
        ("stockBonus", "Nullable(Float64)"),
        ("stockGift", "Nullable(Float64)"),
        ("allotNum", "Nullable(Float64)"),
        ("allotPrice", "Nullable(Float64)"),
        ("gugai", "Nullable(Float64)"),
        ("dr", "Nullable(Float64)"),
    ),
    "ipo": (
        ("securityCode", "String"),
        ("codeName", "String"),
        ("market", "String"),
        ("actIssueQty", "Int64"),
        ("onlineIssueQty", "Int64"),
        ("onlineSubCode", "String"),
        ("onlineSubMaxQty", "Int64"),
        ("publishPrice", "Float64"),
        ("startDate", "String"),
        ("onlineSubMinQty", "Int64"),
        ("isProfit", "Int64"),
        ("industryPe", "Float64"),
        ("beforePE", "Float64"),
        ("afterPE", "Float64"),
        ("listedDate", "String"),
        ("declareDate", "String"),
        ("paymentDate", "String"),
        ("lwr", "Float64"),
    ),
    "download_result": (
        ("function", "String"),
        ("success", "Bool"),
        ("result", "String"),
    ),
}


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
        self._ensured_dynamic_columns: dict[str, set[str]] = {}

    def ensure_tables(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(self._create_sync_task_log_ddl())
        self.client.command(self._create_sync_checkpoint_ddl())
        self._recreate_outdated_state_table(
            QMT_SYNC_TASK_LOG_TABLE,
            expected_engine="MergeTree",
            expected_sorting_key=("task_name", "scope_key", "run_date", "started_at"),
            expected_partition_key="toYYYYMM(run_date)",
            expected_columns=self.SYNC_TASK_LOG_COLUMNS,
            ddl=self._create_sync_task_log_ddl(),
        )
        self._recreate_outdated_state_table(
            QMT_SYNC_CHECKPOINT_TABLE,
            expected_engine="ReplacingMergeTree",
            expected_sorting_key=("task_name", "scope_key"),
            expected_partition_key="",
            expected_columns=self.SYNC_CHECKPOINT_COLUMNS,
            ddl=self._create_sync_checkpoint_ddl(),
        )
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

    def _recreate_outdated_state_table(
        self,
        table_name: str,
        *,
        expected_engine: str,
        expected_sorting_key: tuple[str, ...],
        expected_partition_key: str,
        expected_columns: tuple[str, ...],
        ddl: str,
    ) -> bool:
        rows = self.client.query_rows(
            """
            SELECT engine, sorting_key, partition_key
            FROM system.tables
            WHERE database = {database:String}
              AND name = {table:String}
            """,
            {"database": self.database, "table": table_name},
        )
        if not rows:
            return False

        engine, sorting_key, partition_key = (str(value or "") for value in rows[0][:3])
        column_rows = self.client.query_rows(
            """
            SELECT name
            FROM system.columns
            WHERE database = {database:String}
              AND table = {table:String}
            """,
            {"database": self.database, "table": table_name},
        )
        actual_columns = {str(row[0]) for row in column_rows if row}
        expected_column_set = set(expected_columns)
        expected_key = ",".join(expected_sorting_key)
        if (
            engine == expected_engine
            and self._normalize_ch_expression(sorting_key) == expected_key
            and self._normalize_ch_expression(partition_key)
            == self._normalize_ch_expression(expected_partition_key)
            and actual_columns == expected_column_set
        ):
            return False

        table = self._table_ref(table_name)
        logger.warning(
            "Recreating QMT state table %s with business-correct engine and sorting key",
            table,
        )
        self.client.command(f"DROP TABLE IF EXISTS {table}")
        self.client.command(ddl)
        return True

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
        if not actual_columns or self.table_layout_is_current(
            spec,
            actual_columns,
        ):
            return False

        table = self._table_ref(spec.table_name)
        logger.warning(
            "Recreating QMT table %s because its columns are outdated; existing rows will be resynced",
            table,
        )
        self.client.command(f"DROP TABLE IF EXISTS {table}")
        self.client.command(self._create_task_table_ddl(spec))
        self._ensured_dynamic_columns.pop(spec.table_name, None)
        return True

    def save_task_response(
        self,
        task: str,
        envelope: Mapping[str, Any],
        *,
        request_meta: Mapping[str, Any],
    ) -> int:
        spec = QMT_TASK_SPECS[task]
        source_rows = iter_qmt_rows(spec, envelope, request_meta)
        definitions = list(self.table_column_definitions_for_spec(spec))
        if spec.row_kind in DYNAMIC_ROW_KINDS:
            base_columns = {name for name, _ in definitions}
            observed_columns = _dedupe_names(
                str(name)
                for row in source_rows
                for name in row
                if str(name) not in base_columns
            )
            self._ensure_dynamic_columns(spec, observed_columns)
            definitions.extend((name, "String") for name in observed_columns)
        columns = tuple(name for name, _ in definitions)
        rows = self._materialize_rows(
            source_rows,
            definitions,
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
        columns = set(self.table_columns_for_spec(spec))

        # A response row cannot identify request options that QMT does not
        # return. In those cases the sync log, not invented business columns,
        # is responsible for request-level deduplication.
        if (
            spec.uses_begin_end
            or spec.uses_period
            or spec.uses_fields
            or spec.uses_adjust_type
            or spec.uses_fill_data
            or spec.uses_count
            or spec.uses_incrementally
            or spec.uses_complete
            or spec.uses_table_names
        ):
            return False

        clauses: list[str] = []
        parameters: dict[str, Any] = {}
        for key, column in (
            ("symbol", "symbol"),
            ("stock_code", "stock_code"),
            ("index_code", "index_code"),
            ("market", "market"),
            ("sector_name", "sector_name"),
            ("code_market", "code_market"),
        ):
            if column not in columns:
                continue
            value = self._normalize_lookup_value(key, request_meta.get(key))
            if value == "":
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

    def load_latest_cursor(self, task: str, *, symbol: str | None = None) -> str | None:
        spec = QMT_TASK_SPECS[task]
        if not spec.cursor_path:
            return None
        column = spec.cursor_path[-1]
        columns = set(self.table_columns_for_spec(spec))
        if column not in columns:
            return None

        clauses: list[str] = []
        parameters: dict[str, Any] = {}
        if symbol and "symbol" in columns:
            clauses.append("symbol = {symbol:String}")
            parameters["symbol"] = normalize_qmt_code(symbol)
        quoted_column = self._quote_identifier(column)
        column_type = dict(self.table_column_definitions_for_spec(spec))[column]
        if column_type == "String":
            clauses.append(f"{quoted_column} != ''")
        else:
            clauses.append(f"{quoted_column} != 0")
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
        SELECT max({quoted_column})
        FROM {self._table_ref(spec.table_name)}
        {where_clause}
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
        return ROW_KIND_COLUMN_DEFINITIONS[spec.row_kind]

    @classmethod
    def table_columns_for_spec(cls, spec: QmtTaskSpec) -> tuple[str, ...]:
        return tuple(name for name, _ in cls.table_column_definitions_for_spec(spec))

    @classmethod
    def legacy_columns_for_spec(cls, spec: QmtTaskSpec) -> frozenset[str]:
        container_columns: set[str] = {"payload_json"}
        if spec.row_kind == "financial":
            container_columns.update({"columns", "rows"})
        elif spec.row_kind == "dynamic_fields":
            container_columns.add("fields")
        elif spec.row_kind == "type":
            container_columns.add("type")
        elif spec.row_kind == "frame":
            container_columns.update({"fields", "rows"})
        elif spec.row_kind == "instrument":
            container_columns.add("fields")
        elif spec.row_kind == "factor":
            container_columns.add("items")
        return frozenset(container_columns) | LEGACY_GENERIC_COLUMNS

    @classmethod
    def table_layout_is_current(
        cls,
        spec: QmtTaskSpec,
        actual_columns: Iterable[str],
    ) -> bool:
        actual = {str(name) for name in actual_columns}
        expected = set(cls.table_columns_for_spec(spec))
        if actual & cls.legacy_columns_for_spec(spec):
            return False
        if spec.row_kind in DYNAMIC_ROW_KINDS:
            return expected <= actual
        return actual == expected

    def _materialize_rows(
        self,
        source_rows: Sequence[Mapping[str, Any]],
        definitions: Sequence[tuple[str, str]],
    ) -> list[tuple[Any, ...]]:
        return [
            tuple(
                self._coerce_value(source.get(name), column_type)
                for name, column_type in definitions
            )
            for source in source_rows
        ]

    def _ensure_dynamic_columns(
        self,
        spec: QmtTaskSpec,
        columns: Sequence[str],
    ) -> None:
        table = self._table_ref(spec.table_name)
        ensured = self._ensured_dynamic_columns.setdefault(spec.table_name, set())
        for column in columns:
            if column in ensured:
                continue
            self.client.command(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "
                f"{self._quote_identifier(column)} String"
            )
            ensured.add(column)

    @staticmethod
    def _string_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        return str(value)

    @classmethod
    def _coerce_value(cls, value: Any, column_type: str) -> Any:
        if column_type.startswith("Nullable(") and column_type.endswith(")"):
            if value is None:
                return None
            return cls._coerce_value(value, column_type[9:-1])
        if column_type == "Float64":
            try:
                return float(value or 0)
            except (TypeError, ValueError):
                return 0.0
        if column_type == "Int64":
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0
        if column_type in {"Array(Float64)", "Array(Int64)"}:
            if not isinstance(value, (list, tuple)):
                return []
            result: list[float | int] = []
            for item in value:
                try:
                    result.append(float(item) if column_type == "Array(Float64)" else int(float(item)))
                except (TypeError, ValueError):
                    continue
            return result
        if column_type == "Array(String)":
            return [cls._string_value(item) for item in value] if isinstance(value, (list, tuple)) else []
        if column_type == "Array(Array(Int64))":
            if not isinstance(value, (list, tuple)):
                return []
            result: list[list[int]] = []
            for row in value:
                if not isinstance(row, (list, tuple)):
                    continue
                normalized_row: list[int] = []
                for item in row:
                    try:
                        normalized_row.append(int(float(item)))
                    except (TypeError, ValueError):
                        continue
                result.append(normalized_row)
            return result
        if column_type == "Map(String, String)":
            if not isinstance(value, Mapping):
                return {}
            return {str(key): cls._string_value(item) for key, item in value.items()}
        if column_type == "Array(Map(String, String))":
            if not isinstance(value, (list, tuple)):
                return []
            return [
                {str(key): cls._string_value(item) for key, item in row.items()}
                for row in value
                if isinstance(row, Mapping)
            ]
        if column_type == "Bool":
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "on"}
            return bool(value)
        return cls._string_value(value)

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
            started_at DateTime64(3)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMM(run_date)
        ORDER BY (task_name, scope_key, run_date, started_at)
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
            message Nullable(String)
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (task_name, scope_key)
        """

    def _create_task_table_ddl(
        self,
        spec: QmtTaskSpec,
        *,
        table_name: str | None = None,
    ) -> str:
        table = self._table_ref(table_name or spec.table_name)
        order_by = ", ".join(
            self._quote_identifier(name) for name in order_by_columns_for_spec(spec)
        )
        columns = ",\n            ".join(
            f"{self._quote_identifier(name)} {column_type}"
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

    @staticmethod
    def _normalize_ch_expression(value: Any) -> str:
        return "".join(str(value or "").replace("`", "").split())

    @staticmethod
    def _quote_identifier(value: Any) -> str:
        return f"`{str(value).replace('`', '``')}`"


def _dedupe_names(values: Iterable[Any]) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        name = str(value)
        if name and name not in seen:
            seen.add(name)
            result.append(name)
    return tuple(result)


__all__ = [
    "QMT_SYNC_CHECKPOINT_TABLE",
    "QMT_SYNC_TASK_LOG_TABLE",
    "QmtRepository",
]
