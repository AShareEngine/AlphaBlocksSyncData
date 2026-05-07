#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData 风格的 MarketData 实现.

当前已实现两个接口：
- `query_kline`
- `query_snapshot`
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, timedelta
from typing import Callable, Iterable, Optional, Protocol, Sequence

from sync_data_system.amazingdata_constants import PeriodName, SyncStatus
from sync_data_system.base_data import BaseDataCacheMissError, BaseDataParameterError
from sync_data_system.clickhouse_client import ClickHouseConfig, create_clickhouse_client
from sync_data_system.clickhouse_tables import AD_MARKET_KLINE_DAILY_TABLE, AD_MARKET_KLINE_MINUTE_TABLE, AD_MARKET_SNAPSHOT_TABLE
from sync_data_system.data_models import (
    MarketKlineQuery,
    MarketKlineRow,
    MarketSnapshotQuery,
    MarketSnapshotRow,
    SyncTaskLogRow,
    normalize_code_list,
    to_ch_date,
    utcnow,
)
from sync_data_system.repositories.market_data_repository import MarketDataRepository


logger = logging.getLogger(__name__)


class MarketDataSyncProvider(Protocol):
    """MarketData 远端同步协议."""

    def fetch_kline(
        self,
        code_list: Sequence[str],
        begin_date: date,
        end_date: date,
        period: str,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Iterable[MarketKlineRow]:
        ...

    def fetch_snapshot(
        self,
        code_list: Sequence[str],
        begin_date: date,
        end_date: date,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Iterable[MarketSnapshotRow]:
        ...


class MarketData:
    """MarketData SDK 兼容层."""

    def __init__(
        self,
        repository: MarketDataRepository,
        sync_provider: Optional[MarketDataSyncProvider] = None,
    ) -> None:
        self.repository = repository
        self.sync_provider = sync_provider
        self._period_cache: dict[str, str] = {}

    @classmethod
    def from_clickhouse_config(
        cls,
        config: ClickHouseConfig,
        sync_provider: Optional[MarketDataSyncProvider] = None,
        ensure_tables: bool = True,
        insert_batch_size: int = 5000,
    ) -> "MarketData":
        connection = create_clickhouse_client(config)
        repository = MarketDataRepository(connection, insert_batch_size=insert_batch_size)
        instance = cls(repository=repository, sync_provider=sync_provider)
        if ensure_tables:
            instance.ensure_tables()
        return instance

    def ensure_tables(self) -> None:
        self.repository.ensure_tables()

    def close(self) -> None:
        self.repository.client.close()

    def query_kline(
        self,
        code_list: Sequence[str],
        begin_date: int,
        end_date: int,
        period: str | int = PeriodName.DAY,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        begin = to_ch_date(begin_date)
        end = to_ch_date(end_date)
        period_token = self._resolve_period_token(period)
        self._validate_date_range(begin, end)

        self.sync_kline(
            code_list=normalized_codes,
            begin_date=begin,
            end_date=end,
            period=period_token,
            begin_time=begin_time,
            end_time=end_time,
            force=False,
        )

        result = self.repository.load_kline_dict(
            self._build_kline_query(
                code_list=normalized_codes,
                begin_date=begin,
                end_date=end,
                begin_time=begin_time,
                end_time=end_time,
            )
        )
        if not result:
            self.sync_kline(
                code_list=normalized_codes,
                begin_date=begin,
                end_date=end,
                period=period_token,
                begin_time=begin_time,
                end_time=end_time,
                force=True,
            )
            result = self.repository.load_kline_dict(
                self._build_kline_query(
                    code_list=normalized_codes,
                    begin_date=begin,
                    end_date=end,
                    begin_time=begin_time,
                    end_time=end_time,
                )
            )
        if not result:
            raise BaseDataCacheMissError(
                f"未找到 code_count={len(normalized_codes)} 的日线 kline 数据。"
            )
        return result

    def query_kline_minute(
        self,
        code_list: Sequence[str],
        begin_date: int,
        end_date: int,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        begin = to_ch_date(begin_date)
        end = to_ch_date(end_date)
        self._validate_date_range(begin, end)

        self.sync_kline_minute(
            code_list=normalized_codes,
            begin_date=begin,
            end_date=end,
            begin_time=begin_time,
            end_time=end_time,
            force=False,
        )

        result = self.repository.load_kline_minute_dict(
            self._build_kline_query(
                code_list=normalized_codes,
                begin_date=begin,
                end_date=end,
                begin_time=begin_time,
                end_time=end_time,
            )
        )
        if not result:
            self.sync_kline_minute(
                code_list=normalized_codes,
                begin_date=begin,
                end_date=end,
                begin_time=begin_time,
                end_time=end_time,
                force=True,
            )
            result = self.repository.load_kline_minute_dict(
                self._build_kline_query(
                    code_list=normalized_codes,
                    begin_date=begin,
                    end_date=end,
                    begin_time=begin_time,
                    end_time=end_time,
                )
            )
        if not result:
            raise BaseDataCacheMissError(
                f"未找到 code_count={len(normalized_codes)} 的 1 分钟 kline 数据。"
            )
        return result

    def query_snapshot(
        self,
        code_list: Sequence[str],
        begin_date: int,
        end_date: int,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        begin = to_ch_date(begin_date)
        end = to_ch_date(end_date)
        self._validate_date_range(begin, end)

        self.sync_snapshot(
            code_list=normalized_codes,
            begin_date=begin,
            end_date=end,
            begin_time=begin_time,
            end_time=end_time,
            force=False,
        )

        result = self.repository.load_snapshot_dict(
            MarketSnapshotQuery(
                code_list=tuple(normalized_codes),
                begin_date=begin,
                end_date=end,
                begin_time=begin_time,
                end_time=end_time,
            )
        )
        if not result:
            self.sync_snapshot(
                code_list=normalized_codes,
                begin_date=begin,
                end_date=end,
                begin_time=begin_time,
                end_time=end_time,
                force=True,
            )
            result = self.repository.load_snapshot_dict(
                MarketSnapshotQuery(
                    code_list=tuple(normalized_codes),
                    begin_date=begin,
                    end_date=end,
                    begin_time=begin_time,
                    end_time=end_time,
                )
            )
        if not result:
            raise BaseDataCacheMissError(
                f"未找到 code_count={len(normalized_codes)} 的 snapshot 数据。"
            )
        return result

    def sync_kline(
        self,
        code_list: Sequence[str],
        begin_date: date | int | str,
        end_date: date | int | str,
        period: str | int = PeriodName.DAY,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        begin = to_ch_date(begin_date)
        end = to_ch_date(end_date)
        period_text = str(period).strip()
        if period_text in {PeriodName.DAY, "10008"}:
            period_token = "10008"
        else:
            raise BaseDataParameterError("ad_market_kline_daily 只支持日线周期。")
        self._validate_date_range(begin, end)
        logger.info(
            "sync_kline prepared code_count=%s begin_date=%s end_date=%s raw_period=%s resolved_period=%s",
            len(normalized_codes),
            begin,
            end,
            period,
            period_token,
        )
        latest_date_map = self.repository.load_latest_kline_trade_date_map(normalized_codes)

        total_inserted = 0
        for code_index, code in enumerate(normalized_codes, start=1):
            latest_date = latest_date_map.get(code)
            if latest_date is not None:
                latest_date = to_ch_date(latest_date)
            sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
            if sync_start is not None and sync_start > end:
                logger.info(
                    "sync_kline code=%s progress=%s/%s latest_date=%s sync_start=%s end_date=%s no_new_window_skip",
                    code,
                    code_index,
                    len(normalized_codes),
                    latest_date,
                    sync_start,
                    end,
                )
                continue
            logger.info(
                "sync_kline code=%s progress=%s/%s latest_date=%s sync_start=%s",
                code,
                code_index,
                len(normalized_codes),
                latest_date,
                sync_start,
            )
            scope_key = self._build_market_scope_key(
                task_name="query_kline",
                code_list=[code],
                begin_date=sync_start,
                end_date=end,
                begin_time=begin_time,
                end_time=end_time,
            )
            inserted = self._run_sync_job(
                task_name="query_kline",
                scope_key=scope_key,
                target_table=AD_MARKET_KLINE_DAILY_TABLE,
                latest_date=latest_date,
                fetch_rows=lambda _latest_date, code=code, sync_start=sync_start: self._provider_fetch_kline(
                    [code],
                    begin_date=sync_start,
                    end_date=end,
                    period=PeriodName.DAY,
                    begin_time=begin_time,
                    end_time=end_time,
                ),
                save_rows=self.repository.save_market_kline_rows,
                row_date_getter=lambda row: row.trade_time.date(),
                force=force,
            )
            total_inserted += int(inserted)

        return total_inserted

    def sync_kline_minute(
        self,
        code_list: Sequence[str],
        begin_date: date | int | str,
        end_date: date | int | str,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        begin = to_ch_date(begin_date)
        end = to_ch_date(end_date)
        period_token = "10000"
        self._validate_date_range(begin, end)
        logger.info(
            "sync_kline_minute prepared code_count=%s begin_date=%s end_date=%s resolved_period=%s",
            len(normalized_codes),
            begin,
            end,
            period_token,
        )
        latest_date_map = self.repository.load_latest_kline_minute_trade_date_map(normalized_codes)

        total_inserted = 0
        for code_index, code in enumerate(normalized_codes, start=1):
            latest_date = latest_date_map.get(code)
            if latest_date is not None:
                latest_date = to_ch_date(latest_date)
            sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
            if sync_start is not None and sync_start > end:
                logger.info(
                    "sync_kline_minute code=%s progress=%s/%s latest_date=%s sync_start=%s end_date=%s no_new_window_skip",
                    code,
                    code_index,
                    len(normalized_codes),
                    latest_date,
                    sync_start,
                    end,
                )
                continue
            logger.info(
                "sync_kline_minute code=%s progress=%s/%s latest_date=%s sync_start=%s",
                code,
                code_index,
                len(normalized_codes),
                latest_date,
                sync_start,
            )
            scope_key = self._build_market_scope_key(
                task_name="query_kline_minute",
                code_list=[code],
                begin_date=sync_start,
                end_date=end,
                period=period_token,
                begin_time=begin_time,
                end_time=end_time,
            )
            inserted = self._run_sync_job(
                task_name="query_kline_minute",
                scope_key=scope_key,
                target_table=AD_MARKET_KLINE_MINUTE_TABLE,
                latest_date=latest_date,
                fetch_rows=lambda _latest_date, code=code, sync_start=sync_start: self._provider_fetch_kline(
                    [code],
                    begin_date=sync_start,
                    end_date=end,
                    period=period_token,
                    begin_time=begin_time,
                    end_time=end_time,
                ),
                save_rows=self.repository.save_market_kline_minute_rows,
                row_date_getter=lambda row: row.trade_time.date(),
                force=force,
            )
            total_inserted += int(inserted)

        return total_inserted

    def sync_snapshot(
        self,
        code_list: Sequence[str],
        begin_date: date | int | str,
        end_date: date | int | str,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")

        begin = to_ch_date(begin_date)
        end = to_ch_date(end_date)
        self._validate_date_range(begin, end)
        logger.info(
            "sync_snapshot prepared code_count=%s begin_date=%s end_date=%s",
            len(normalized_codes),
            begin,
            end,
        )

        scope_key = self._build_market_scope_key(
            task_name="query_snapshot",
            code_list=normalized_codes,
            begin_date=begin,
            end_date=end,
            begin_time=begin_time,
            end_time=end_time,
        )
        latest_date = self.repository.load_latest_snapshot_trade_date(list(normalized_codes))
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        if sync_start is not None and sync_start > end:
            logger.info(
                "sync_snapshot latest_date=%s sync_start=%s end_date=%s no_new_window_skip",
                latest_date,
                sync_start,
                end,
            )
            return 0
        logger.info(
            "sync_snapshot latest_date=%s sync_start=%s code_count=%s",
            latest_date,
            sync_start,
            len(normalized_codes),
        )
        return self._run_sync_job(
            task_name="query_snapshot",
            scope_key=scope_key,
            target_table=AD_MARKET_SNAPSHOT_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _latest_date: self._provider_fetch_snapshot(
                normalized_codes,
                begin_date=sync_start,
                end_date=end,
                begin_time=begin_time,
                end_time=end_time,
            ),
            save_rows=self.repository.save_market_snapshot_rows,
            row_date_getter=lambda row: row.trade_time.date(),
            force=force,
        )

    def _run_sync_job(
        self,
        task_name: str,
        scope_key: str,
        target_table: str,
        latest_date: Optional[date],
        fetch_rows: Callable[[Optional[date]], Iterable[object]],
        save_rows: Callable[[Iterable[object]], int],
        row_date_getter: Callable[[object], Optional[date]],
        force: bool,
    ) -> int:
        run_date = datetime.now().date()
        started_at = utcnow()

        if self.sync_provider is None:
            message = f"{task_name} 未配置 sync_provider，无法执行同步。"
            logger.warning(message)
            self._write_sync_log(
                task_name=task_name,
                scope_key=scope_key,
                run_date=run_date,
                status=SyncStatus.FAILED,
                target_table=target_table,
                start_date=latest_date,
                end_date=latest_date,
                row_count=0,
                message=message,
                started_at=started_at,
                finished_at=utcnow(),
            )
            return 0

        stats = {"row_count": 0, "min_date": None, "max_date": latest_date}

        def tracked_rows() -> Iterable[object]:
            for row in fetch_rows(latest_date):
                stats["row_count"] += 1
                row_date = row_date_getter(row)
                if row_date is not None:
                    if stats["min_date"] is None or row_date < stats["min_date"]:
                        stats["min_date"] = row_date
                    if stats["max_date"] is None or row_date > stats["max_date"]:
                        stats["max_date"] = row_date
                yield row

        logger.info(
            "Start sync task=%s scope=%s target_table=%s latest_date=%s",
            task_name,
            scope_key,
            target_table,
            latest_date,
        )

        try:
            inserted_count = save_rows(tracked_rows())
        except Exception as exc:
            message = f"{task_name} 同步失败: {exc}"
            logger.exception(message)
            self._write_sync_log(
                task_name=task_name,
                scope_key=scope_key,
                run_date=run_date,
                status=SyncStatus.FAILED,
                target_table=target_table,
                start_date=latest_date or stats["min_date"],
                end_date=stats["max_date"],
                row_count=stats["row_count"],
                message=message,
                started_at=started_at,
                finished_at=utcnow(),
            )
            raise

        message = (
            f"{task_name} 同步完成，"
            f"latest_date={latest_date}, inserted_rows={inserted_count}, observed_rows={stats['row_count']}"
        )
        logger.info(message)
        self._write_sync_log(
            task_name=task_name,
            scope_key=scope_key,
            run_date=run_date,
            status=SyncStatus.SUCCESS,
            target_table=target_table,
            start_date=latest_date or stats["min_date"],
            end_date=stats["max_date"],
            row_count=inserted_count,
            message=message,
            started_at=started_at,
            finished_at=utcnow(),
        )
        return inserted_count

    def _provider_fetch_kline(
        self,
        code_list: Sequence[str],
        begin_date: date,
        end_date: date,
        period: str,
        begin_time: Optional[int],
        end_time: Optional[int],
    ):
        return self.sync_provider.fetch_kline(  # type: ignore[union-attr]
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            period=period,
            begin_time=begin_time,
            end_time=end_time,
        )

    @staticmethod
    def _build_kline_query(
        code_list: Sequence[str],
        begin_date: date,
        end_date: date,
        begin_time: Optional[int],
        end_time: Optional[int],
    ) -> MarketKlineQuery:
        return MarketKlineQuery(
            code_list=tuple(code_list),
            begin_date=begin_date,
            end_date=end_date,
            begin_time=begin_time,
            end_time=end_time,
        )

    def _provider_fetch_snapshot(
        self,
        code_list: Sequence[str],
        begin_date: date,
        end_date: date,
        begin_time: Optional[int],
        end_time: Optional[int],
    ):
        return self.sync_provider.fetch_snapshot(  # type: ignore[union-attr]
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            begin_time=begin_time,
            end_time=end_time,
        )

    def _write_sync_log(
        self,
        task_name: str,
        scope_key: str,
        run_date: date,
        status: str,
        target_table: str,
        start_date: Optional[date],
        end_date: Optional[date],
        row_count: int,
        message: str,
        started_at: datetime,
        finished_at: datetime,
    ) -> None:
        try:
            self.repository.insert_sync_log(
                SyncTaskLogRow(
                    task_name=task_name,
                    scope_key=scope_key,
                    run_date=run_date,
                    status=status,
                    target_table=target_table,
                    start_date=start_date,
                    end_date=end_date,
                    row_count=max(0, int(row_count)),
                    message=message,
                    started_at=started_at,
                    finished_at=finished_at,
                )
            )
        except Exception:
            logger.exception("写入同步日志失败 task=%s scope=%s status=%s", task_name, scope_key, status)

    @staticmethod
    def _resolve_period_token(self, period: str | int) -> str:
        text = str(period).strip()
        if not text:
            raise BaseDataParameterError("period 不能为空。")
        if text == PeriodName.DAY:
            return "10008"
        if text == PeriodName.MIN1:
            if self.sync_provider is None or not hasattr(self.sync_provider, "session"):
                raise BaseDataParameterError("无法解析 1 分钟周期枚举。")
            return str(self.sync_provider.session.resolve_period_value(text))  # type: ignore[attr-defined]
        if text == "10008":
            return text
        raise BaseDataParameterError("当前只支持日线和 1 分钟周期。")

    @staticmethod
    def _resolve_incremental_start_date(
        latest_date: Optional[date],
        requested_begin_date: Optional[date],
    ) -> Optional[date]:
        if latest_date is None:
            return requested_begin_date
        next_date = latest_date + timedelta(days=1)
        if requested_begin_date is None:
            return next_date
        return max(next_date, requested_begin_date)

    @staticmethod
    def _validate_date_range(begin_date: date, end_date: date) -> None:
        if begin_date > end_date:
            raise BaseDataParameterError("begin_date 不能大于 end_date。")

    @staticmethod
    def _build_market_scope_key(
        task_name: str,
        code_list: Sequence[str],
        begin_date: Optional[date],
        end_date: Optional[date],
        period: Optional[str] = None,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> str:
        digest = hashlib.sha1(",".join(sorted(code_list)).encode("utf-8")).hexdigest()[:12]
        return (
            f"task={task_name}|code_count={len(code_list)}|codes_sha1={digest}"
            f"|begin_date={begin_date.isoformat() if begin_date else ''}"
            f"|end_date={end_date.isoformat() if end_date else ''}"
            f"|period={period or ''}|begin_time={begin_time or ''}|end_time={end_time or ''}"
        )


__all__ = [
    "MarketData",
    "MarketDataSyncProvider",
]
