#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ClickHouse persistence for AKShare market data."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Sequence

import pandas as pd

from sync_data_system.providers.akshare.provider import (
    DAILY_COLUMNS,
    EM_CONCEPT_CONS_COLUMNS,
    EM_CONCEPT_HIST_COLUMNS,
    EM_CONCEPT_NAME_COLUMNS,
    FINANCIAL_INDICATOR_COLUMNS,
    FINANCIAL_STATEMENT_COLUMNS,
    INDEX_COLUMNS,
    MINUTE_COLUMNS,
    PROFILE_COLUMNS,
    SPOT_COLUMNS,
    THS_CONCEPT_INDEX_COLUMNS,
    THS_CONCEPT_INFO_COLUMNS,
    THS_CONCEPT_NAME_COLUMNS,
    VALUATION_COLUMNS,
)
from sync_data_system.providers.akshare.specs import AKSHARE_TASK_SPECS
from sync_data_system.sync_core.clickhouse import ClickHouseConnection
from sync_data_system.sync_core.sync_models import SyncCheckpointRow, SyncTaskLogRow


AKSHARE_SYNC_TASK_LOG_TABLE = "ak_sync_task_log"
AKSHARE_SYNC_CHECKPOINT_TABLE = "ak_sync_checkpoint"
AKSHARE_SYMBOL_CURSOR_TABLE = "ak_symbol_cursor"

TASK_COLUMNS: dict[str, tuple[str, ...]] = {
    "us_spot": SPOT_COLUMNS,
    "us_daily_kline": DAILY_COLUMNS,
    "us_minute_kline": MINUTE_COLUMNS,
    "us_company_profile": PROFILE_COLUMNS,
    "us_financial_statement": FINANCIAL_STATEMENT_COLUMNS,
    "us_financial_indicator": FINANCIAL_INDICATOR_COLUMNS,
    "us_valuation": VALUATION_COLUMNS,
    "us_index_daily": INDEX_COLUMNS,
    "stock_board_concept_name_ths": THS_CONCEPT_NAME_COLUMNS,
    "stock_board_concept_index_ths": THS_CONCEPT_INDEX_COLUMNS,
    "stock_board_concept_info_ths": THS_CONCEPT_INFO_COLUMNS,
    "stock_board_concept_name_em": EM_CONCEPT_NAME_COLUMNS,
    "stock_board_concept_cons_em": EM_CONCEPT_CONS_COLUMNS,
    "stock_board_concept_hist_em": EM_CONCEPT_HIST_COLUMNS,
}

STRING_COLUMNS = frozenset(
    {
        "em_code",
        "market_id",
        "symbol",
        "name",
        "instrument_type",
        "source",
        "adjust",
        "item",
        "value",
        "statement_type",
        "period_type",
        "report_type",
        "secu_code",
        "security_name",
        "item_code",
        "item_name",
        "raw_json",
        "currency",
        "indicator",
        "period",
        "index_code",
        "index_name",
        "concept_code",
        "concept_name",
    }
)


class AkshareUSRepository:
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
        database: str = "akshare",
        insert_batch_size: int = 5000,
    ) -> None:
        self.client = client
        self.database = str(database).strip() or "akshare"
        self.insert_batch_size = max(1, int(insert_batch_size))

    def ensure_tables(self) -> None:
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(self._create_sync_task_log_ddl())
        self.client.command(self._create_sync_checkpoint_ddl())
        self.client.command(self._create_symbol_cursor_ddl())
        for task in AKSHARE_TASK_SPECS:
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
            self._table_ref(AKSHARE_TASK_SPECS[task].table_name),
            columns,
            rows,
        )

    def load_symbols(self, *, limit: int = 0) -> list[str]:
        limit_sql = f"LIMIT {int(limit)}" if limit > 0 else ""
        sql = f"""
        SELECT symbol
        FROM {self._table_ref(AKSHARE_TASK_SPECS['us_spot'].table_name)}
        WHERE snapshot_date = (
            SELECT max(snapshot_date)
            FROM {self._table_ref(AKSHARE_TASK_SPECS['us_spot'].table_name)}
        )
          AND instrument_type = 'common_stock'
        ORDER BY symbol
        {limit_sql}
        """
        return [
            str(row[0]).strip()
            for row in self.client.query_rows(sql)
            if row and str(row[0]).strip()
        ]

    def load_ths_concepts(
        self,
        *,
        snapshot_date: date | None = None,
        limit: int = 0,
    ) -> list[dict[str, str]]:
        limit_sql = f"LIMIT {int(limit)}" if limit > 0 else ""
        table = self._table_ref(
            AKSHARE_TASK_SPECS["stock_board_concept_name_ths"].table_name
        )
        sql = f"""
        SELECT concept_code, argMax(concept_name, fetched_at) AS concept_name
        FROM {table}
        WHERE snapshot_date = {{snapshot_date:Date}}
        GROUP BY concept_code
        ORDER BY concept_name, concept_code
        {limit_sql}
        """
        return [
            {
                "concept_code": str(row[0]).strip(),
                "concept_name": str(row[1]).strip(),
            }
            for row in self.client.query_rows(
                sql,
                {"snapshot_date": snapshot_date or date.today()},
            )
            if len(row) >= 2 and str(row[0]).strip() and str(row[1]).strip()
        ]

    def load_em_concepts(
        self,
        *,
        snapshot_date: date | None = None,
        limit: int = 0,
    ) -> list[dict[str, str]]:
        limit_sql = f"LIMIT {int(limit)}" if limit > 0 else ""
        table = self._table_ref(
            AKSHARE_TASK_SPECS["stock_board_concept_name_em"].table_name
        )
        sql = f"""
        SELECT concept_code, argMax(concept_name, fetched_at) AS concept_name
        FROM {table}
        WHERE snapshot_date = {{snapshot_date:Date}}
        GROUP BY concept_code
        ORDER BY concept_name, concept_code
        {limit_sql}
        """
        return [
            {
                "concept_code": str(row[0]).strip(),
                "concept_name": str(row[1]).strip(),
            }
            for row in self.client.query_rows(
                sql,
                {"snapshot_date": snapshot_date or date.today()},
            )
            if len(row) >= 2 and str(row[0]).strip() and str(row[1]).strip()
        ]

    def load_latest_cursor(self, task: str, *, symbol: str) -> str | None:
        spec = AKSHARE_TASK_SPECS[task]
        if not spec.cursor_field:
            return None
        value = self.client.query_value(
            f"""
            SELECT max(cursor_date)
            FROM {self._table_ref(AKSHARE_SYMBOL_CURSOR_TABLE)}
            WHERE task_name = {{task_name:String}}
              AND symbol = {{symbol:String}}
            """,
            {
                "task_name": task,
                "symbol": str(symbol).strip().upper(),
            },
        )
        return _normalize_cursor_value(value)

    def upsert_task_cursor(self, task: str, symbol: str, cursor_date: str | date) -> None:
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            return
        self.client.insert_rows(
            self._table_ref(AKSHARE_SYMBOL_CURSOR_TABLE),
            ("task_name", "symbol", "cursor_date", "finished_at"),
            [
                (
                    task,
                    normalized_symbol,
                    _to_date(cursor_date),
                    datetime.now(timezone.utc).replace(tzinfo=None),
                )
            ],
        )

    def insert_sync_log(self, row: SyncTaskLogRow) -> None:
        self.client.insert_rows(
            self._table_ref(AKSHARE_SYNC_TASK_LOG_TABLE),
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
            self._table_ref(AKSHARE_SYNC_CHECKPOINT_TABLE),
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
            FROM {self._table_ref(AKSHARE_SYNC_TASK_LOG_TABLE)}
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
        return total

    def _table_ref(self, table_name: str) -> str:
        return f"{self.database}.{table_name}"

    def _create_sync_task_log_ddl(self) -> str:
        return f"""
        CREATE TABLE IF NOT EXISTS {self._table_ref(AKSHARE_SYNC_TASK_LOG_TABLE)}
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
        CREATE TABLE IF NOT EXISTS {self._table_ref(AKSHARE_SYNC_CHECKPOINT_TABLE)}
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
        CREATE TABLE IF NOT EXISTS {self._table_ref(AKSHARE_SYMBOL_CURSOR_TABLE)}
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
        table = self._table_ref(AKSHARE_TASK_SPECS[task].table_name)
        if task == "us_spot":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                snapshot_at DateTime64(3),
                em_code String,
                market_id String,
                symbol String,
                name String,
                instrument_type String,
                last Nullable(Float64),
                change_amount Nullable(Float64),
                change_percent Nullable(Float64),
                open Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                previous_close Nullable(Float64),
                market_cap Nullable(Float64),
                pe Nullable(Float64),
                volume Nullable(Float64),
                turnover Nullable(Float64),
                amplitude Nullable(Float64),
                turnover_rate Nullable(Float64),
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, symbol, em_code)
            """
        if task == "us_daily_kline":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                em_code String,
                symbol String,
                trade_date Date,
                open Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                close Nullable(Float64),
                volume Nullable(Float64),
                turnover Nullable(Float64),
                amplitude Nullable(Float64),
                change_percent Nullable(Float64),
                change_amount Nullable(Float64),
                turnover_rate Nullable(Float64),
                adjust String,
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(trade_date)
            ORDER BY (symbol, trade_date, adjust)
            """
        if task == "us_minute_kline":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                em_code String,
                symbol String,
                trade_time DateTime,
                open Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                close Nullable(Float64),
                volume Nullable(Float64),
                turnover Nullable(Float64),
                latest Nullable(Float64),
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(trade_time)
            ORDER BY (symbol, trade_time)
            """
        if task == "us_company_profile":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                symbol String,
                item String,
                value String,
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, symbol, item)
            """
        if task == "us_financial_statement":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                symbol String,
                statement_type String,
                period_type String,
                report_date Date,
                report_type String,
                secu_code String,
                security_name String,
                item_code String,
                item_name String,
                amount Nullable(Float64),
                raw_json String,
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(report_date)
            ORDER BY (symbol, statement_type, period_type, report_date, item_code, item_name)
            """
        if task == "us_financial_indicator":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                symbol String,
                period_type String,
                report_date Date,
                notice_date Nullable(Date),
                currency String,
                operate_income Nullable(Float64),
                operate_income_yoy Nullable(Float64),
                gross_profit Nullable(Float64),
                gross_profit_yoy Nullable(Float64),
                net_profit Nullable(Float64),
                net_profit_yoy Nullable(Float64),
                basic_eps Nullable(Float64),
                diluted_eps Nullable(Float64),
                gross_profit_ratio Nullable(Float64),
                net_profit_ratio Nullable(Float64),
                roe Nullable(Float64),
                roa Nullable(Float64),
                current_ratio Nullable(Float64),
                quick_ratio Nullable(Float64),
                debt_asset_ratio Nullable(Float64),
                raw_json String,
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(report_date)
            ORDER BY (symbol, period_type, report_date)
            """
        if task == "us_valuation":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                symbol String,
                indicator String,
                period String,
                trade_date Date,
                value Nullable(Float64),
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(trade_date)
            ORDER BY (symbol, indicator, trade_date)
            """
        if task == "us_index_daily":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                index_code String,
                index_name String,
                trade_date Date,
                open Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                close Nullable(Float64),
                volume Nullable(Float64),
                amount Nullable(Float64),
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(trade_date)
            ORDER BY (index_code, trade_date)
            """
        if task == "stock_board_concept_name_ths":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                concept_code String,
                concept_name String,
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, concept_code)
            """
        if task == "stock_board_concept_index_ths":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                concept_code String,
                concept_name String,
                trade_date Date,
                open Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                close Nullable(Float64),
                volume Nullable(Float64),
                amount Nullable(Float64),
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(trade_date)
            ORDER BY (concept_code, trade_date)
            """
        if task == "stock_board_concept_info_ths":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                concept_code String,
                concept_name String,
                item String,
                value String,
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, concept_code, item)
            """
        if task == "stock_board_concept_name_em":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                concept_code String,
                concept_name String,
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, concept_code)
            """
        if task == "stock_board_concept_cons_em":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                concept_code String,
                concept_name String,
                rank Nullable(Int64),
                symbol String,
                name String,
                last Nullable(Float64),
                change_percent Nullable(Float64),
                change_amount Nullable(Float64),
                volume Nullable(Float64),
                amount Nullable(Float64),
                amplitude Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                open Nullable(Float64),
                previous_close Nullable(Float64),
                turnover_rate Nullable(Float64),
                pe_dynamic Nullable(Float64),
                pb Nullable(Float64),
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, concept_code, symbol)
            """
        if task == "stock_board_concept_hist_em":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                concept_code String,
                concept_name String,
                period String,
                adjust String,
                trade_date Date,
                open Nullable(Float64),
                high Nullable(Float64),
                low Nullable(Float64),
                close Nullable(Float64),
                change_percent Nullable(Float64),
                change_amount Nullable(Float64),
                volume Nullable(Float64),
                amount Nullable(Float64),
                amplitude Nullable(Float64),
                turnover_rate Nullable(Float64),
                source String,
                fetched_at DateTime64(3)
            )
            ENGINE = ReplacingMergeTree(fetched_at)
            PARTITION BY toYYYYMM(trade_date)
            ORDER BY (concept_code, period, adjust, trade_date)
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
    return str(value) if column in STRING_COLUMNS else value


def _normalize_cursor_value(value: Any) -> str | None:
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value or "").strip().replace("-", "")
    return text[:8] if len(text) >= 8 and text[:8].isdigit() else None


def _to_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip().replace("-", "")
    return datetime.strptime(text[:8], "%Y%m%d").date()


__all__ = [
    "AKSHARE_SYMBOL_CURSOR_TABLE",
    "AKSHARE_SYNC_CHECKPOINT_TABLE",
    "AKSHARE_SYNC_TASK_LOG_TABLE",
    "AkshareUSRepository",
    "TASK_COLUMNS",
]
