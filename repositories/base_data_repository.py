#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BaseData 对应的 ClickHouse 读写层."""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import date
from typing import Iterable, Sequence

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from sync_data_system.amazingdata_constants import SyncStatus
from sync_data_system.clickhouse_client import ClickHouseConnection
from sync_data_system.clickhouse_tables import (
    AD_ADJ_FACTOR_TABLE,
    AD_BACKWARD_FACTOR_TABLE,
    AD_CODE_INFO_TABLE,
    AD_ETF_PCF_TABLE,
    AD_ETF_PCF_CONSTITUENT_TABLE,
    AD_HIST_CODE_DAILY_TABLE,
    AD_SYNC_CHECKPOINT_TABLE,
    AD_SYNC_TASK_LOG_TABLE,
    AD_TRADE_CALENDAR_TABLE,
    iter_base_data_table_ddls,
)
from sync_data_system.data_models import (
    CalendarQuery,
    CodeInfoQuery,
    CodeInfoRow,
    EtfPcfRow,
    EtfPcfConstituentRow,
    HistCodeDailyRow,
    HistCodeQuery,
    PriceFactorQuery,
    PriceFactorRow,
    SyncCheckpointRow,
    SyncTaskLogRow,
    TradeCalendarRow,
    should_keep_security_code,
    to_ch_date,
)


logger = logging.getLogger(__name__)


class BaseDataRepository:
    """BaseData 的 ClickHouse repository.

    设计原则：
    - repository 只负责 SQL 与数据落库
    - 不负责参数校验和自动补数策略
    - 返回给 SDK 层时，尽量已经接近最终可消费结构
    """

    TRADE_CALENDAR_COLUMNS = (
        "trade_date",
    )
    CODE_INFO_COLUMNS = (
        "security_type",
        "code",
        "symbol",
        "security_status_raw",
        "pre_close",
        "high_limited",
        "low_limited",
        "price_tick",
    )
    HIST_CODE_DAILY_COLUMNS = (
        "trade_date",
        "security_type",
        "code",
    )
    PRICE_FACTOR_COLUMNS = (
        "trade_date",
        "code",
        "factor_value",
    )
    ETF_PCF_COLUMNS = (
        "etf_code",
        "creation_redemption_unit",
        "max_cash_ratio",
        "publish",
        "creation",
        "redemption",
        "creation_redemption_switch",
        "record_num",
        "total_record_num",
        "estimate_cash_component",
        "trading_day",
        "pre_trading_day",
        "cash_component",
        "nav_per_cu",
        "nav",
        "symbol",
        "fund_management_company",
        "underlying_security_id",
        "underlying_security_id_source",
        "dividend_per_cu",
        "creation_limit",
        "redemption_limit",
        "creation_limit_per_user",
        "redemption_limit_per_user",
        "net_creation_limit",
        "net_redemption_limit",
        "net_creation_limit_per_user",
        "net_redemption_limit_per_user",
    )
    ETF_PCF_CONSTITUENT_COLUMNS = (
        "etf_code",
        "trading_day",
        "underlying_symbol",
        "component_share",
        "substitute_flag",
        "premium_ratio",
        "discount_ratio",
        "creation_cash_substitute",
        "redemption_cash_substitute",
        "substitution_cash_amount",
        "underlying_security_id",
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

    def __init__(self, client: ClickHouseConnection, insert_batch_size: int = 5000) -> None:
        self.client = client
        self.insert_batch_size = max(1, int(insert_batch_size))

    def ensure_tables(self) -> None:
        for ddl in iter_base_data_table_ddls():
            self.client.command(ddl)

    def _load_table_column_names(self, table: str) -> set[str]:
        sql = """
        SELECT name
        FROM system.columns
        WHERE database = currentDatabase()
          AND table = {table:String}
        """
        rows = self.client.query_rows(sql, {"table": table})
        return {str(row[0]) for row in rows if row and row[0] is not None}

    def _ensure_table_columns(self, table: str, column_defs: Sequence[tuple[str, str]]) -> None:
        existing_columns = self._load_table_column_names(table)
        for column_name, column_type in column_defs:
            if column_name in existing_columns:
                continue
            self.client.command(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")

    def save_calendar_rows(self, rows: Iterable[TradeCalendarRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_TRADE_CALENDAR_TABLE,
            columns=self.TRADE_CALENDAR_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_code_info_rows(self, rows: Iterable[CodeInfoRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_CODE_INFO_TABLE,
            columns=self.CODE_INFO_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_hist_code_daily_rows(self, rows: Iterable[HistCodeDailyRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_HIST_CODE_DAILY_TABLE,
            columns=self.HIST_CODE_DAILY_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_price_factor_rows(self, rows: Iterable[PriceFactorRow]) -> int:
        raise NotImplementedError("请使用 save_adj_factor_rows 或 save_backward_factor_rows。")

    def save_adj_factor_rows(self, rows: Iterable[PriceFactorRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_ADJ_FACTOR_TABLE,
            columns=self.PRICE_FACTOR_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_backward_factor_rows(self, rows: Iterable[PriceFactorRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_BACKWARD_FACTOR_TABLE,
            columns=self.PRICE_FACTOR_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_etf_pcf_rows(self, rows: Iterable[EtfPcfRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_ETF_PCF_TABLE,
            columns=self.ETF_PCF_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_etf_pcf_constituent_rows(self, rows: Iterable[EtfPcfConstituentRow]) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_ETF_PCF_CONSTITUENT_TABLE,
            columns=self.ETF_PCF_CONSTITUENT_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def insert_sync_log(self, row: SyncTaskLogRow) -> None:
        self._insert_dataclass_rows_in_batches(
            table=AD_SYNC_TASK_LOG_TABLE,
            columns=self.SYNC_TASK_LOG_COLUMNS,
            rows=[row],
            partition_field="run_date",
        )
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
        self._insert_dataclass_rows_in_batches(
            table=AD_SYNC_CHECKPOINT_TABLE,
            columns=self.SYNC_CHECKPOINT_COLUMNS,
            rows=[row],
            partition_field="run_date",
        )

    def has_successful_task_log_today(self, task_name: str, scope_key: str, run_date: date) -> bool:
        sql = f"""
        SELECT count()
        FROM {AD_SYNC_TASK_LOG_TABLE}
        WHERE task_name = {{task_name:String}}
          AND scope_key = {{scope_key:String}}
          AND run_date = {{run_date:Date}}
          AND status = {{status:String}}
        """
        value = self.client.query_value(
            sql,
            {
                "task_name": task_name,
                "scope_key": scope_key,
                "run_date": run_date,
                "status": SyncStatus.SUCCESS,
            },
        )
        return bool(value and int(value) > 0)

    def load_sync_checkpoint_date(self, task_name: str, scope_key: str):
        sql = f"""
        SELECT argMax(checkpoint_date, finished_at)
        FROM {AD_SYNC_CHECKPOINT_TABLE}
        WHERE task_name = {{task_name:String}}
          AND scope_key = {{scope_key:String}}
          AND status = {{status:String}}
        """
        return self.client.query_value(
            sql,
            {
                "task_name": task_name,
                "scope_key": scope_key,
                "status": SyncStatus.SUCCESS,
            },
        )

    def has_successful_sync_today(self, task_name: str, scope_key: str, run_date: date) -> bool:
        sql = f"""
        SELECT count()
        FROM {AD_SYNC_CHECKPOINT_TABLE}
        WHERE task_name = {{task_name:String}}
          AND scope_key = {{scope_key:String}}
          AND run_date = {{run_date:Date}}
          AND status = {{status:String}}
        """
        value = self.client.query_value(
            sql,
            {
                "task_name": task_name,
                "scope_key": scope_key,
                "run_date": run_date,
                "status": SyncStatus.SUCCESS,
            },
        )
        return bool(value and int(value) > 0)

    def load_successful_scope_keys(self, task_name: str, scope_keys: Sequence[str]) -> set[str]:
        if not scope_keys:
            return set()
        sql = f"""
        SELECT scope_key
        FROM {AD_SYNC_CHECKPOINT_TABLE}
        WHERE task_name = {{task_name:String}}
          AND status = {{status:String}}
          AND scope_key IN {{scope_keys:Array(String)}}
        GROUP BY scope_key
        """
        rows = self.client.query_rows(
            sql,
            {
                "task_name": task_name,
                "status": SyncStatus.SUCCESS,
                "scope_keys": list(scope_keys),
            },
        )
        return {str(row[0]) for row in rows if row and row[0] is not None}

    def load_calendar_dates(self, query: CalendarQuery) -> list[date]:
        sql = f"""
        SELECT trade_date
        FROM {AD_TRADE_CALENDAR_TABLE}
        GROUP BY trade_date
        ORDER BY trade_date
        """
        rows = self.client.query_rows(sql)
        return [row[0] for row in rows if row and row[0] is not None]

    def load_latest_calendar_date(self):
        sql = f"""
        SELECT max(trade_date)
        FROM {AD_TRADE_CALENDAR_TABLE}
        """
        return self.client.query_value(sql)

    def load_latest_hist_code_trade_date(self, security_type: str):
        sql = f"""
        SELECT max(trade_date)
        FROM {AD_HIST_CODE_DAILY_TABLE}
        WHERE security_type = {{security_type:String}}
        """
        return self.client.query_value(sql, {"security_type": security_type})

    def load_latest_price_factor_trade_date(self, table_name: str, code_list: Sequence[str]):
        if not code_list:
            return None

        sql = f"""
        SELECT code, max(trade_date) AS latest_trade_date
        FROM {table_name}
        WHERE code IN {{code_list:Array(String)}}
        GROUP BY code
        ORDER BY code
        """
        rows = self.client.query_rows(
            sql,
            {
                "code_list": list(code_list),
            },
        )
        if len(rows) != len(code_list):
            # 只要有任一 code 完全缺失，就从头同步这批 code。
            return None
        latest_dates = [row[1] for row in rows if len(row) > 1 and row[1] is not None]
        if not latest_dates:
            return None
        return min(latest_dates)

    def load_code_info_frame(self, query: CodeInfoQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")

        sql = f"""
        SELECT
            code,
            any(symbol) AS symbol,
            any(security_status_raw) AS security_status,
            any(pre_close) AS pre_close,
            any(high_limited) AS high_limited,
            any(low_limited) AS low_limited,
            any(price_tick) AS price_tick
        FROM {AD_CODE_INFO_TABLE}
        WHERE security_type = {{security_type:String}}
        GROUP BY code
        ORDER BY code
        """
        frame = self.client.query_df(
            sql,
            {"security_type": query.security_type},
        )
        if frame.empty:
            return frame
        filtered_codes = [
            code
            for code in frame["code"].tolist()
            if should_keep_security_code(code, security_type=query.security_type)
        ]
        frame = frame[frame["code"].isin(filtered_codes)].copy()
        if frame.empty:
            return frame
        frame = frame.set_index("code")
        frame.index.name = None
        return frame

    def load_hist_code_list(self, query: HistCodeQuery) -> list[str]:
        sql = f"""
        SELECT code
        FROM {AD_HIST_CODE_DAILY_TABLE}
        WHERE security_type = {{security_type:String}}
          AND trade_date >= {{start_date:Date}}
          AND trade_date <= {{end_date:Date}}
        GROUP BY code
        ORDER BY code
        """
        rows = self.client.query_rows(
            sql,
            {
                "security_type": query.security_type,
                "start_date": query.start_date,
                "end_date": query.end_date,
            },
        )
        return [str(row[0]) for row in rows if row and row[0] is not None]

    def load_price_factor_frame(self, table_name: str, query: PriceFactorQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")

        if not query.code_list:
            return pd.DataFrame(columns=["trade_date", "code", "factor_value"])

        sql = f"""
        SELECT
            trade_date,
            code,
            factor_value
        FROM {table_name} FINAL
        WHERE code IN {{code_list:Array(String)}}
        ORDER BY trade_date, code
        """
        return self.client.query_df(
            sql,
            {
                "code_list": list(query.code_list),
            },
        )

    def load_existing_codes(self, table_name: str, code_list: Sequence[str]) -> set[str]:
        if not code_list:
            return set()
        sql = f"""
        SELECT code
        FROM {table_name}
        WHERE code IN {{code_list:Array(String)}}
        GROUP BY code
        """
        rows = self.client.query_rows(sql, {"code_list": list(code_list)})
        return {str(row[0]) for row in rows if row and row[0] is not None}

    def _insert_dataclass_rows_in_batches(
        self,
        table: str,
        columns: Sequence[str],
        rows: Iterable[object],
        partition_field: str | None = None,
        partition_group_size: int | None = None,
        single_insert: bool = False,
    ) -> int:
        total = 0
        if single_insert:
            payload: list[tuple] = []
            for row in rows:
                record = asdict(row)
                payload.append(tuple(record[column] for column in columns))
            if not payload:
                return 0
            self.client.insert_rows(table, columns, payload)
            total += len(payload)
            logger.info("Inserted %s rows into %s single_insert=true", len(payload), table)
            return total

        if partition_field is None:
            batch: list[tuple] = []

            for row in rows:
                record = asdict(row)
                batch.append(tuple(record[column] for column in columns))
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

        partition_batches: dict[str, list[tuple]] = {}
        for row in rows:
            record = asdict(row)
            partition_key = self._get_partition_key(record.get(partition_field))
            batch = partition_batches.setdefault(partition_key, [])
            batch.append(tuple(record[column] for column in columns))
            if partition_group_size is None and len(batch) >= self.insert_batch_size:
                self.client.insert_rows(table, columns, batch)
                total += len(batch)
                logger.info("Inserted %s rows into %s partition=%s", len(batch), table, partition_key)
                partition_batches[partition_key] = []

        if partition_group_size is not None and partition_group_size > 0:
            sorted_partition_keys = sorted(partition_batches.keys())
            for index in range(0, len(sorted_partition_keys), partition_group_size):
                partition_group = sorted_partition_keys[index : index + partition_group_size]
                merged_batch: list[tuple] = []
                for partition_key in partition_group:
                    merged_batch.extend(partition_batches.get(partition_key, []))
                if not merged_batch:
                    continue
                self.client.insert_rows(table, columns, merged_batch)
                total += len(merged_batch)
                logger.info(
                    "Inserted %s rows into %s partitions=%s..%s partition_count=%s",
                    len(merged_batch),
                    table,
                    partition_group[0],
                    partition_group[-1],
                    len(partition_group),
                )
            return total

        for partition_key, batch in partition_batches.items():
            if not batch:
                continue
            self.client.insert_rows(table, columns, batch)
            total += len(batch)
            logger.info("Inserted %s rows into %s partition=%s", len(batch), table, partition_key)

        return total

    @staticmethod
    def _get_partition_key(value) -> str:
        if value is None:
            return "unknown"
        dt = to_ch_date(value)
        return dt.strftime("%Y%m")


__all__ = ["BaseDataRepository"]
