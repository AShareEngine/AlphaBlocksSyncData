#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ClickHouse persistence for the free US market provider."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Sequence

import pandas as pd

from sync_data_system.providers.yfinance.specs import YFINANCE_TASK_SPECS
from sync_data_system.sync_core.clickhouse import ClickHouseConnection
from sync_data_system.sync_core.sync_models import SyncCheckpointRow, SyncTaskLogRow


logger = logging.getLogger(__name__)

YFINANCE_SYNC_TASK_LOG_TABLE = "yf_sync_task_log"
YFINANCE_SYNC_CHECKPOINT_TABLE = "yf_sync_checkpoint"
YFINANCE_SYMBOL_CURSOR_TABLE = "yf_symbol_cursor"

TASK_COLUMNS: dict[str, tuple[str, ...]] = {
    "symbol_master": (
        "snapshot_date",
        "symbol",
        "name",
        "currency",
        "sector",
        "industry_group",
        "industry",
        "exchange",
        "market",
        "country",
        "state",
        "city",
        "zipcode",
        "website",
        "market_cap",
        "summary",
        "isin",
        "cusip",
        "figi",
        "composite_figi",
        "shareclass_figi",
        "source",
    ),
    "daily_kline": (
        "symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "dividends",
        "stock_splits",
        "capital_gains",
        "source",
        "fetched_at",
    ),
    "corporate_actions": (
        "symbol",
        "event_date",
        "dividend",
        "stock_split",
        "capital_gain",
        "source",
        "fetched_at",
    ),
    "industry_membership": (
        "snapshot_date",
        "symbol",
        "sector",
        "industry_group",
        "industry",
        "exchange",
        "source",
    ),
    "sector_daily": (
        "group_code",
        "group_name",
        "benchmark_symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "source",
        "fetched_at",
    ),
    "concept_daily": (
        "group_code",
        "group_name",
        "benchmark_symbol",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
        "source",
        "fetched_at",
    ),
    "concept_membership": (
        "snapshot_date",
        "concept_code",
        "concept_name",
        "etf_symbol",
        "symbol",
        "holding_name",
        "weight",
        "membership_scope",
        "source",
        "fetched_at",
    ),
}

STRING_COLUMNS = frozenset(
    {
        "symbol",
        "name",
        "currency",
        "sector",
        "industry_group",
        "industry",
        "exchange",
        "market",
        "country",
        "state",
        "city",
        "zipcode",
        "website",
        "market_cap",
        "summary",
        "isin",
        "cusip",
        "figi",
        "composite_figi",
        "shareclass_figi",
        "source",
        "group_code",
        "group_name",
        "benchmark_symbol",
        "concept_code",
        "concept_name",
        "etf_symbol",
        "holding_name",
        "membership_scope",
    }
)


class YFinanceRepository:
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
        database: str = "yfinance",
        insert_batch_size: int = 5000,
    ) -> None:
        self.client = client
        self.database = str(database).strip() or "yfinance"
        self.insert_batch_size = max(1, int(insert_batch_size))

    def ensure_tables(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(self._create_sync_task_log_ddl())
        self.client.command(self._create_sync_checkpoint_ddl())
        self.client.command(self._create_symbol_cursor_ddl())
        for task in YFINANCE_TASK_SPECS:
            self.client.command(self._create_task_table_ddl(task))

    def save_frame(self, task: str, frame: pd.DataFrame) -> int:
        if task not in TASK_COLUMNS:
            raise KeyError(task)
        if frame is None or frame.empty:
            return 0
        columns = TASK_COLUMNS[task]
        missing = [column for column in columns if column not in frame.columns]
        if missing:
            raise ValueError(f"{task} 数据缺少落库字段: {missing}")
        rows = [
            tuple(_normalize_insert_value(column, value) for column, value in zip(columns, values))
            for values in frame.loc[:, list(columns)].itertuples(index=False, name=None)
        ]
        return self._insert_rows_in_batches(
            self._table_ref(YFINANCE_TASK_SPECS[task].table_name),
            columns,
            rows,
        )

    def load_symbols(self, *, limit: int = 0) -> list[str]:
        limit_sql = f"LIMIT {int(limit)}" if limit > 0 else ""
        sql = f"""
        SELECT symbol
        FROM {self._table_ref(YFINANCE_TASK_SPECS['symbol_master'].table_name)}
        WHERE snapshot_date = (
            SELECT max(snapshot_date)
            FROM {self._table_ref(YFINANCE_TASK_SPECS['symbol_master'].table_name)}
        )
        ORDER BY symbol
        {limit_sql}
        """
        return [str(row[0]).strip() for row in self.client.query_rows(sql) if row and str(row[0]).strip()]

    def load_latest_cursor(self, task: str, *, symbol: str | None = None) -> str | None:
        spec = YFINANCE_TASK_SPECS[task]
        if not spec.cursor_field:
            return None
        if symbol:
            value = self.client.query_value(
                f"""
                SELECT max(cursor_date)
                FROM {self._table_ref(YFINANCE_SYMBOL_CURSOR_TABLE)}
                WHERE task_name = {{task_name:String}}
                  AND symbol = {{symbol:String}}
                """,
                {
                    "task_name": task,
                    "symbol": str(symbol).strip().upper(),
                },
            )
            normalized = _normalize_cursor_value(value)
            if normalized:
                return normalized
        clauses: list[str] = []
        parameters: dict[str, Any] = {}
        symbol_column = "symbol"
        if task in {"sector_daily", "concept_daily"}:
            symbol_column = "benchmark_symbol"
        if symbol:
            clauses.append(f"{symbol_column} = {{symbol:String}}")
            parameters["symbol"] = str(symbol).strip().upper()
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
        SELECT max({spec.cursor_field})
        FROM {self._table_ref(spec.table_name)}
        {where_sql}
        """
        value = self.client.query_value(sql, parameters)
        return _normalize_cursor_value(value)

    def upsert_task_cursor(self, task: str, symbol: str, cursor_date: str | date) -> None:
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            return
        normalized_date = _to_date(cursor_date)
        self.client.insert_rows(
            self._table_ref(YFINANCE_SYMBOL_CURSOR_TABLE),
            ("task_name", "symbol", "cursor_date", "finished_at"),
            [(task, normalized_symbol, normalized_date, datetime.now(timezone.utc).replace(tzinfo=None))],
        )

    def insert_sync_log(self, row: SyncTaskLogRow) -> None:
        self.client.insert_rows(
            self._table_ref(YFINANCE_SYNC_TASK_LOG_TABLE),
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
            self._table_ref(YFINANCE_SYNC_CHECKPOINT_TABLE),
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
        sql = f"""
        SELECT count()
        FROM {self._table_ref(YFINANCE_SYNC_TASK_LOG_TABLE)}
        WHERE task_name = {{task_name:String}}
          AND scope_key = {{scope_key:String}}
          AND run_date = {{run_date:Date}}
          AND status = 'success'
        """
        count = self.client.query_value(
            sql,
            {"task_name": task_name, "scope_key": scope_key, "run_date": run_date},
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
        return f"{self.database}.{table_name}"

    def _create_sync_task_log_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_ref(YFINANCE_SYNC_TASK_LOG_TABLE)}
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
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_ref(YFINANCE_SYNC_CHECKPOINT_TABLE)}
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

    def _create_symbol_cursor_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_ref(YFINANCE_SYMBOL_CURSOR_TABLE)}
        (
            task_name String,
            symbol String,
            cursor_date Date,
            finished_at DateTime64(3)
        )
        ENGINE = ReplacingMergeTree(finished_at)
        ORDER BY (task_name, symbol)
        """

    def _create_task_table_ddl(self, task: str) -> str:
        table = self._table_ref(YFINANCE_TASK_SPECS[task].table_name)
        if task == "symbol_master":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                symbol String,
                name String,
                currency String,
                sector String,
                industry_group String,
                industry String,
                exchange String,
                market String,
                country String,
                state String,
                city String,
                zipcode String,
                website String,
                market_cap String,
                summary String,
                isin String,
                cusip String,
                figi String,
                composite_figi String,
                shareclass_figi String,
                source String,
                ingested_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(ingested_at)
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, symbol)
            """
        if task == "daily_kline":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                symbol String,
                trade_date Date,
                open Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                close Nullable(Float64),
                adj_close Nullable(Float64),
                volume Nullable(Float64),
                dividends Nullable(Float64),
                stock_splits Nullable(Float64),
                capital_gains Nullable(Float64),
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(trade_date)
            ORDER BY (symbol, trade_date)
            """
        if task == "corporate_actions":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                symbol String,
                event_date Date,
                dividend Nullable(Float64),
                stock_split Nullable(Float64),
                capital_gain Nullable(Float64),
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(event_date)
            ORDER BY (symbol, event_date)
            """
        if task == "industry_membership":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                symbol String,
                sector String,
                industry_group String,
                industry String,
                exchange String,
                source String,
                ingested_at DateTime64(3) DEFAULT now64(3)
            )
            ENGINE = ReplacingMergeTree(ingested_at)
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, sector, industry, symbol)
            """
        if task in {"sector_daily", "concept_daily"}:
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                group_code String,
                group_name String,
                benchmark_symbol String,
                trade_date Date,
                open Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                close Nullable(Float64),
                adj_close Nullable(Float64),
                volume Nullable(Float64),
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(trade_date)
            ORDER BY (group_code, benchmark_symbol, trade_date)
            """
        if task == "concept_membership":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                concept_code String,
                concept_name String,
                etf_symbol String,
                symbol String,
                holding_name String,
                weight Nullable(Float64),
                membership_scope String,
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, concept_code, etf_symbol, symbol)
            """
        raise KeyError(task)


def _normalize_insert_value(column: str, value: Any) -> Any:
    if value is None:
        return "" if column in STRING_COLUMNS else None
    try:
        if pd.isna(value):
            return "" if column in STRING_COLUMNS else None
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    item = getattr(value, "item", None)
    if callable(item) and not isinstance(value, (str, bytes, date, datetime)):
        try:
            value = item()
        except Exception:
            pass
    if column in STRING_COLUMNS:
        return str(value)
    return value


def _normalize_cursor_value(value: Any) -> str | None:
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = "".join(character for character in str(value or "") if character.isdigit())
    return text[:8] or None


def _to_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = "".join(character for character in str(value or "") if character.isdigit())
    if len(text) < 8:
        raise ValueError(f"日期必须是 YYYYMMDD / YYYY-MM-DD，当前值: {value!r}")
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


__all__ = [
    "TASK_COLUMNS",
    "YFINANCE_SYNC_CHECKPOINT_TABLE",
    "YFINANCE_SYNC_TASK_LOG_TABLE",
    "YFINANCE_SYMBOL_CURSOR_TABLE",
    "YFinanceRepository",
]
