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

COMMON_SECURITY_NAME_SQL = """
positionCaseInsensitiveUTF8(name, 'preference') = 0
AND positionCaseInsensitiveUTF8(name, 'preferred') = 0
AND positionCaseInsensitiveUTF8(name, 'warrant') = 0
AND positionCaseInsensitiveUTF8(name, ' rights') = 0
AND positionCaseInsensitiveUTF8(name, ' units') = 0
AND positionCaseInsensitiveUTF8(name, 'debenture') = 0
AND positionCaseInsensitiveUTF8(name, 'when-issued') = 0
AND positionCaseInsensitiveUTF8(name, ' dep shs') = 0
AND positionCaseInsensitiveUTF8(name, '% series') = 0
AND (
    positionCaseInsensitiveUTF8(name, 'depositary shares') = 0
    OR positionCaseInsensitiveUTF8(name, 'american depositary shares') > 0
)
"""

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
    ),
    "corporate_actions": (
        "symbol",
        "event_date",
        "dividend",
        "stock_split",
        "capital_gain",
    ),
    "industry_membership": (
        "snapshot_date",
        "symbol",
        "sector",
        "industry_group",
        "industry",
        "exchange",
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
    ),
    "income_statement": (
        "symbol",
        "report_date",
        "period_type",
        "metric",
        "value",
    ),
    "balance_sheet": (
        "symbol",
        "report_date",
        "period_type",
        "metric",
        "value",
    ),
    "cash_flow": (
        "symbol",
        "report_date",
        "period_type",
        "metric",
        "value",
    ),
    "financial_metrics": (
        "snapshot_date",
        "symbol",
        "currency",
        "financial_currency",
        "quote_type",
        "market_cap",
        "enterprise_value",
        "trailing_pe",
        "forward_pe",
        "price_to_book",
        "enterprise_to_revenue",
        "enterprise_to_ebitda",
        "dividend_yield",
        "payout_ratio",
        "beta",
        "shares_outstanding",
        "float_shares",
        "held_percent_insiders",
        "held_percent_institutions",
        "profit_margins",
        "operating_margins",
        "gross_margins",
        "return_on_assets",
        "return_on_equity",
        "revenue_growth",
        "earnings_growth",
        "total_revenue",
        "net_income_to_common",
        "total_cash",
        "total_debt",
        "free_cashflow",
        "operating_cashflow",
    ),
    "earnings_calendar": (
        "symbol",
        "event_time",
        "eps_estimate",
        "reported_eps",
        "surprise_percent",
    ),
    "analyst_estimates": (
        "snapshot_date",
        "symbol",
        "dataset",
        "horizon",
        "metric",
        "value",
    ),
    "institutional_holders": (
        "snapshot_date",
        "symbol",
        "holder_type",
        "holder",
        "report_date",
        "shares",
        "value",
        "percent_held",
        "percent_change",
    ),
    "insider_transactions": (
        "symbol",
        "start_date",
        "insider",
        "position",
        "transaction",
        "shares",
        "value",
        "ownership",
        "transaction_text",
        "url",
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
        "group_code",
        "group_name",
        "benchmark_symbol",
        "concept_code",
        "concept_name",
        "etf_symbol",
        "holding_name",
        "membership_scope",
        "period_type",
        "metric",
        "financial_currency",
        "quote_type",
        "dataset",
        "horizon",
        "holder_type",
        "holder",
        "insider",
        "position",
        "transaction",
        "ownership",
        "transaction_text",
        "url",
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
            self._migrate_removed_metadata_columns(task)

    def _migrate_removed_metadata_columns(self, task: str) -> None:
        table_name = YFINANCE_TASK_SPECS[task].table_name
        rows = self.client.query_rows(
            """
            SELECT name
            FROM system.columns
            WHERE database = {database:String}
              AND table = {table:String}
              AND name IN ('source', 'fetched_at')
            """,
            {"database": self.database, "table": table_name},
        )
        existing = {str(row[0]) for row in rows if row}
        if not existing:
            return

        table = self._table_ref(table_name)
        if "fetched_at" not in existing:
            self.client.command(f"ALTER TABLE {table} DROP COLUMN IF EXISTS source")
            return

        migration_name = f"{table_name}__without_metadata_v1"
        migration_table = self._table_ref(migration_name)
        columns = TASK_COLUMNS[task]
        column_sql = ", ".join(columns)

        self.client.command(f"DROP TABLE IF EXISTS {migration_table}")
        self.client.command(self._create_task_table_ddl(task, table_name=migration_name))
        self.client.command(
            f"INSERT INTO {migration_table} ({column_sql}) "
            f"SELECT {column_sql} FROM {table}"
        )
        self.client.command(
            f"EXCHANGE TABLES {table} AND {migration_table}"
        )
        self.client.command(f"DROP TABLE IF EXISTS {migration_table}")

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
          AND {COMMON_SECURITY_NAME_SQL}
        ORDER BY symbol
        {limit_sql}
        """
        return [str(row[0]).strip() for row in self.client.query_rows(sql) if row and str(row[0]).strip()]

    def load_symbol_master(
        self,
        *,
        limit: int = 0,
        require_industry: bool = False,
    ) -> pd.DataFrame:
        columns = TASK_COLUMNS["symbol_master"]
        limit_sql = f"LIMIT {int(limit)}" if limit > 0 else ""
        industry_filter = (
            "sector != '' OR industry_group != '' OR industry != ''"
        )
        latest_filter = f"WHERE {industry_filter}" if require_industry else ""
        row_filter = f"AND ({industry_filter})" if require_industry else ""
        sql = f"""
        SELECT {", ".join(columns)}
        FROM {self._table_ref(YFINANCE_TASK_SPECS['symbol_master'].table_name)} FINAL
        WHERE snapshot_date = (
            SELECT max(snapshot_date)
            FROM {self._table_ref(YFINANCE_TASK_SPECS['symbol_master'].table_name)}
            {latest_filter}
        )
        {row_filter}
        AND {COMMON_SECURITY_NAME_SQL}
        ORDER BY symbol
        {limit_sql}
        """
        rows = self.client.query_rows(sql)
        return pd.DataFrame(rows, columns=list(columns))

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

    def _create_task_table_ddl(
        self,
        task: str,
        *,
        table_name: str | None = None,
    ) -> str:
        table = self._table_ref(table_name or YFINANCE_TASK_SPECS[task].table_name)
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
                capital_gains Nullable(Float64)
            )
            ENGINE = ReplacingMergeTree
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
                capital_gain Nullable(Float64)
            )
            ENGINE = ReplacingMergeTree
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
                volume Nullable(Float64)
            )
            ENGINE = ReplacingMergeTree
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
                membership_scope String
            )
            ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, concept_code, etf_symbol, symbol)
            """
        if task in {"income_statement", "balance_sheet", "cash_flow"}:
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                symbol String,
                report_date Date,
                period_type String,
                metric String,
                value Nullable(Float64)
            )
            ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(report_date)
            ORDER BY (symbol, report_date, period_type, metric)
            """
        if task == "financial_metrics":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                symbol String,
                currency String,
                financial_currency String,
                quote_type String,
                market_cap Nullable(Float64),
                enterprise_value Nullable(Float64),
                trailing_pe Nullable(Float64),
                forward_pe Nullable(Float64),
                price_to_book Nullable(Float64),
                enterprise_to_revenue Nullable(Float64),
                enterprise_to_ebitda Nullable(Float64),
                dividend_yield Nullable(Float64),
                payout_ratio Nullable(Float64),
                beta Nullable(Float64),
                shares_outstanding Nullable(Float64),
                float_shares Nullable(Float64),
                held_percent_insiders Nullable(Float64),
                held_percent_institutions Nullable(Float64),
                profit_margins Nullable(Float64),
                operating_margins Nullable(Float64),
                gross_margins Nullable(Float64),
                return_on_assets Nullable(Float64),
                return_on_equity Nullable(Float64),
                revenue_growth Nullable(Float64),
                earnings_growth Nullable(Float64),
                total_revenue Nullable(Float64),
                net_income_to_common Nullable(Float64),
                total_cash Nullable(Float64),
                total_debt Nullable(Float64),
                free_cashflow Nullable(Float64),
                operating_cashflow Nullable(Float64)
            )
            ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, symbol)
            """
        if task == "earnings_calendar":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                symbol String,
                event_time DateTime64(3),
                eps_estimate Nullable(Float64),
                reported_eps Nullable(Float64),
                surprise_percent Nullable(Float64)
            )
            ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(event_time)
            ORDER BY (symbol, event_time)
            """
        if task == "analyst_estimates":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                symbol String,
                dataset String,
                horizon String,
                metric String,
                value Nullable(Float64)
            )
            ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, symbol, dataset, horizon, metric)
            """
        if task == "institutional_holders":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                snapshot_date Date,
                symbol String,
                holder_type String,
                holder String,
                report_date Nullable(Date),
                shares Nullable(Float64),
                value Nullable(Float64),
                percent_held Nullable(Float64),
                percent_change Nullable(Float64)
            )
            ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(snapshot_date)
            ORDER BY (snapshot_date, symbol, holder_type, holder)
            """
        if task == "insider_transactions":
            return f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                symbol String,
                start_date Date,
                insider String,
                position String,
                transaction String,
                shares Nullable(Float64),
                value Nullable(Float64),
                ownership String,
                transaction_text String,
                url String
            )
            ENGINE = ReplacingMergeTree
            PARTITION BY toYYYYMM(start_date)
            ORDER BY (symbol, start_date, insider, transaction, transaction_text, url)
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
