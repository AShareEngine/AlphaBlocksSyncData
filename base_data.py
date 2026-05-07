#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData 风格的 BaseData 实现.

当前版本重点解决 4 件事：
1. 对外方法签名尽量贴近手册
2. 数据统一走 ClickHouse repository
3. 日期 + code 类表按增量方式同步
4. 同步日志持久化，且当天成功后再次执行自动跳过
"""

from __future__ import annotations

import logging
from datetime import date, datetime, time
from typing import Callable, Iterable, Optional, Protocol, Sequence

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from sync_data_system.amazingdata_constants import FactorType, Market, SecurityType, SyncStatus
from sync_data_system.clickhouse_client import ClickHouseConfig, create_clickhouse_client
from sync_data_system.clickhouse_tables import (
    AD_ADJ_FACTOR_TABLE,
    AD_BACKWARD_FACTOR_TABLE,
    AD_CODE_INFO_TABLE,
    AD_ETF_PCF_TABLE,
    AD_ETF_PCF_CONSTITUENT_TABLE,
    AD_HIST_CODE_DAILY_TABLE,
    AD_TRADE_CALENDAR_TABLE,
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
    TradeCalendarRow,
    normalize_code_list,
    to_ch_date,
    utcnow,
)
from sync_data_system.repositories.base_data_repository import BaseDataRepository
from sync_data_system.sync_core.amazingdata import build_hashed_scope_key, resolve_incremental_start_date, safe_write_sync_log


logger = logging.getLogger(__name__)
FACTOR_SYNC_CODE_BATCH_SIZE = 25


class BaseDataError(Exception):
    """BaseData 基础异常."""


class BaseDataParameterError(BaseDataError):
    """BaseData 参数异常."""


class BaseDataCacheMissError(BaseDataError):
    """ClickHouse 中没有命中可用数据，且当前无法自动补数."""


class BaseDataSyncProvider(Protocol):
    """远端同步协议.

    BaseData 不直接依赖具体数据源。
    未来无论你接 AmazingData 官方 SDK，还是接自己的采集服务，
    只要实现这组方法，就能复用当前 BaseData + ClickHouse 的结构。
    """

    def fetch_calendar(
        self,
        market: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[TradeCalendarRow]:
        ...

    def fetch_code_info(
        self,
        security_type: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[CodeInfoRow]:
        ...

    def fetch_hist_code_daily(
        self,
        security_type: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[HistCodeDailyRow]:
        ...

    def fetch_price_factor(
        self,
        factor_type: str,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[PriceFactorRow]:
        ...

    def fetch_etf_pcf(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[EtfPcfRow]:
        ...

    def fetch_etf_pcf_constituent(
        self,
        code_list: Sequence[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Iterable[EtfPcfConstituentRow]:
        ...


class BaseData:
    """BaseData SDK 兼容层."""

    def __init__(
        self,
        repository: BaseDataRepository,
        sync_provider: Optional[BaseDataSyncProvider] = None,
    ) -> None:
        self.repository = repository
        self.sync_provider = sync_provider

    @classmethod
    def from_clickhouse_config(
        cls,
        config: ClickHouseConfig,
        sync_provider: Optional[BaseDataSyncProvider] = None,
        ensure_tables: bool = True,
        insert_batch_size: int = 5000,
    ) -> "BaseData":
        connection = create_clickhouse_client(config)
        repository = BaseDataRepository(connection, insert_batch_size=insert_batch_size)
        instance = cls(repository=repository, sync_provider=sync_provider)
        if ensure_tables:
            instance.ensure_tables()
        return instance

    def ensure_tables(self) -> None:
        self.repository.ensure_tables()

    def close(self) -> None:
        """关闭底层 ClickHouse 连接."""

        self.repository.client.close()

    def get_calendar(self, data_type: str = "str", market: str = Market.SH):
        """获取交易日历.

        手册写法保留为 `data_type='str'` / `data_type='datetime'`。
        当前实现约定：
        - `str` 返回 `['20240327', ...]`
        - `datetime` 返回 `[datetime(...), ...]`
        """

        self._validate_data_type(data_type)

        self.sync_calendar(market=market, force=False)
        dates = self.repository.load_calendar_dates(CalendarQuery(data_type=data_type))
        if not dates:
            raise BaseDataCacheMissError("未找到交易日历数据。")

        if data_type == "datetime":
            return [datetime.combine(item, time.min) for item in dates]
        return [item.strftime("%Y%m%d") for item in dates]

    def get_code_info(self, security_type: str = SecurityType.EXTRA_STOCK_A):
        """获取每日最新证券信息."""

        security_type = self._validate_security_type(security_type)

        self.sync_code_info(security_type=security_type, force=False)
        frame = self.repository.load_code_info_frame(CodeInfoQuery(security_type=security_type))
        if frame.empty:
            raise BaseDataCacheMissError(f"未找到 security_type={security_type} 的证券基础信息数据。")
        return frame

    def get_code_list(self, security_type: str = SecurityType.EXTRA_STOCK_A) -> list[str]:
        """获取每日最新代码表.

        对外保留 SDK 方法名，但内部统一走代码池封装：
        - 先查今天是否已同步过代码池
        - 已同步则直接从 ClickHouse 读取
        - 未同步则先确保 `code_info` 已同步，再从 ClickHouse 投影出代码池
        """

        return self.get_security_universe(security_type=security_type, force=False)

    def get_security_universe(
        self,
        security_type: str = SecurityType.EXTRA_STOCK_A,
        force: bool = False,
    ) -> list[str]:
        """统一的证券 universe 入口.

        这是后续所有“取代码池”场景的公共方法。
        不论是股票、ETF、可转债还是指数，都统一复用同一套：
        - 今日成功则直接读库
        - 否则先补 `code_info`
        - 最终从 ClickHouse 返回标准化代码列表
        """

        return self.ensure_code_list(security_type=security_type, force=force)

    def get_security_universe_from_db(self, security_type: str = SecurityType.EXTRA_STOCK_A) -> list[str]:
        """只从 ClickHouse 读取证券 universe，不触发同步."""

        return self.get_code_list_from_db(security_type=security_type)

    def get_stock_universe(
        self,
        security_type: str = SecurityType.EXTRA_STOCK_A_SH_SZ,
        force: bool = False,
    ) -> list[str]:
        """获取股票 universe.

        默认使用 `EXTRA_STOCK_A_SH_SZ`，即沪深 A 股。
        如果需要包含北交所，可显式传 `SecurityType.EXTRA_STOCK_A`。
        """

        return self.get_security_universe(security_type=security_type, force=force)

    def get_etf_universe(
        self,
        security_type: str = SecurityType.EXTRA_ETF,
        force: bool = False,
    ) -> list[str]:
        """获取 ETF universe."""

        return self.get_security_universe(security_type=security_type, force=force)

    def get_kzz_universe(
        self,
        security_type: str = SecurityType.EXTRA_KZZ,
        force: bool = False,
    ) -> list[str]:
        """获取可转债 universe."""

        return self.get_security_universe(security_type=security_type, force=force)

    def get_index_universe(
        self,
        security_type: str = SecurityType.EXTRA_INDEX_A_SH_SZ,
        force: bool = False,
    ) -> list[str]:
        """获取指数 universe.

        默认使用 `EXTRA_INDEX_A_SH_SZ`，即沪深指数。
        如果需要包含北交所，可显式传 `SecurityType.EXTRA_INDEX_A`。
        """

        return self.get_security_universe(security_type=security_type, force=force)

    def get_option_code_list(
        self,
        security_type: str = SecurityType.EXTRA_ETF_OP,
        force: bool = False,
    ) -> list[str]:
        """获取期权代码池.

        默认使用 `EXTRA_ETF_OP`，即沪深 ETF 期权。
        """

        return self.get_security_universe(security_type=security_type, force=force)

    def ensure_code_list(self, security_type: str = SecurityType.EXTRA_STOCK_A, force: bool = False) -> list[str]:
        """确保代码池可用，并统一返回 ClickHouse 中的最新代码列表.

        统一流程：
        1. 先看今天是否已经同步过 `get_code_list`
        2. 如果同步过，直接从数据库拿代码池
        3. 如果没同步过，先保证 `get_code_info` 已同步
        4. 再从数据库投影出代码池
        5. 记录 `get_code_list` 的同步日志，供后续任务复用
        """

        security_type = self._validate_security_type(security_type)
        run_date = datetime.now().date()
        scope_key = f"security_type={security_type}"
        started_at = utcnow()

        if not force and self.repository.has_successful_sync_today("get_code_list", scope_key, run_date):
            code_list = self.get_code_list_from_db(security_type=security_type)
            if code_list:
                logger.info(
                    "get_code_list 已在 %s 同步成功，直接从数据库读取 security_type=%s code_count=%s",
                    run_date,
                    security_type,
                    len(code_list),
                )
                return code_list
            logger.warning(
                "get_code_list 今日已有成功日志，但数据库代码池为空，改为重新构建 security_type=%s",
                security_type,
            )

        if self.sync_provider is not None:
            if force:
                self.sync_code_info(security_type=security_type, force=True)
            elif self.repository.has_successful_sync_today("get_code_info", scope_key, run_date):
                logger.info(
                    "get_code_info 已在 %s 同步成功，get_code_list 直接复用 code_info 数据 security_type=%s",
                    run_date,
                    security_type,
                )
            else:
                self.sync_code_info(security_type=security_type, force=False)

        code_list = self.get_code_list_from_db(security_type=security_type)
        if not code_list and self.sync_provider is not None:
            logger.warning(
                "第一次构建代码池后仍为空，改为强制刷新 code_info security_type=%s",
                security_type,
            )
            self.sync_code_info(security_type=security_type, force=True)
            code_list = self.get_code_list_from_db(security_type=security_type)

        latest_checkpoint_date = self.repository.load_sync_checkpoint_date("get_code_info", scope_key)
        finished_at = utcnow()

        if not code_list:
            message = f"未找到 security_type={security_type} 的代码池数据。"
            self._write_sync_log(
                task_name="get_code_list",
                scope_key=scope_key,
                run_date=run_date,
                status=SyncStatus.FAILED,
                target_table=AD_CODE_INFO_TABLE,
                start_date=latest_checkpoint_date,
                end_date=latest_checkpoint_date,
                row_count=0,
                message=message,
                started_at=started_at,
                finished_at=finished_at,
            )
            raise BaseDataCacheMissError(message)

        self._write_sync_log(
            task_name="get_code_list",
            scope_key=scope_key,
            run_date=run_date,
            status=SyncStatus.SUCCESS,
            target_table=AD_CODE_INFO_TABLE,
            start_date=latest_checkpoint_date,
            end_date=latest_checkpoint_date,
            row_count=len(code_list),
            message=(
                f"get_code_list 构建完成 security_type={security_type} "
                f"checkpoint_date={latest_checkpoint_date} code_count={len(code_list)}"
            ),
            started_at=started_at,
            finished_at=finished_at,
        )
        logger.info(
            "get_code_list build success security_type=%s checkpoint_date=%s code_count=%s",
            security_type,
            latest_checkpoint_date,
            len(code_list),
        )
        return code_list

    def get_code_list_from_db(self, security_type: str = SecurityType.EXTRA_STOCK_A) -> list[str]:
        """只从 ClickHouse 获取代码池，不触发同步."""

        security_type = self._validate_security_type(security_type)
        frame = self.repository.load_code_info_frame(CodeInfoQuery(security_type=security_type))
        if frame.empty:
            return []
        return [str(code) for code in frame.index.tolist()]

    def get_hist_code_list(
        self,
        security_type: str,
        start_date: int,
        end_date: int,
        local_path: str,
    ) -> list[str]:
        """获取历史代码表.

        `local_path` 参数保留，是为了兼容手册签名。
        当前 ClickHouse 方案不直接读本地文件，实际命中逻辑以数据库缓存为准。
        """

        security_type = self._validate_security_type(security_type)
        start = to_ch_date(start_date)
        end = to_ch_date(end_date)
        self._validate_date_range(start, end)
        self._validate_local_path(local_path)

        self.sync_hist_code_list(
            security_type=security_type,
            begin_date=start,
            end_date=end,
            force=False,
        )

        query = HistCodeQuery(
            security_type=security_type,
            start_date=start,
            end_date=end,
            local_path=local_path,
        )
        code_list = self.repository.load_hist_code_list(query)
        if not code_list:
            raise BaseDataCacheMissError(
                f"未找到 security_type={security_type}、"
                f"date_range=[{start}, {end}] 的历史代码表数据。"
            )
        return code_list

    def get_adj_factor(self, code_list: Sequence[str], local_path: str, is_local: bool = True):
        """获取单次复权因子."""

        return self._get_factor(
            factor_type=FactorType.ADJ,
            code_list=code_list,
            local_path=local_path,
            is_local=is_local,
        )

    def get_backward_factor(self, code_list: Sequence[str], local_path: str, is_local: bool = True):
        """获取后复权因子."""

        return self._get_factor(
            factor_type=FactorType.BACKWARD,
            code_list=code_list,
            local_path=local_path,
            is_local=is_local,
        )

    def sync_calendar(self, market: str = Market.SH, force: bool = False) -> int:
        """同步交易日历表."""

        latest_date = self.repository.load_latest_calendar_date()
        return self._run_sync_job(
            task_name="get_calendar",
            scope_key="calendar",
            target_table=AD_TRADE_CALENDAR_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda start_date: self._provider_fetch_calendar(market, start_date),
            save_rows=self.repository.save_calendar_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def sync_code_info(self, security_type: str = SecurityType.EXTRA_STOCK_A, force: bool = False) -> int:
        """同步证券基础信息表."""

        security_type = self._validate_security_type(security_type)
        return self._run_sync_job(
            task_name="get_code_info",
            scope_key=f"security_type={security_type}",
            target_table=AD_CODE_INFO_TABLE,
            latest_date=self.repository.load_sync_checkpoint_date("get_code_info", f"security_type={security_type}"),
            fetch_rows=lambda start_date: self._provider_fetch_code_info(security_type, start_date),
            save_rows=self.repository.save_code_info_rows,
            row_date_getter=lambda _row: datetime.now().date(),
            force=force,
        )

    def sync_hist_code_list(
        self,
        security_type: str,
        begin_date: Optional[date | int | str] = None,
        end_date: Optional[date | int | str] = None,
        force: bool = False,
    ) -> int:
        """同步历史代码表."""

        security_type = self._validate_security_type(security_type)
        begin = to_ch_date(begin_date) if begin_date is not None else None
        end = to_ch_date(end_date) if end_date is not None else None
        self._validate_optional_date_range(begin, end)
        scope_key = f"security_type={security_type}|begin_date={begin or ''}|end_date={end or ''}"
        latest_date = self.repository.load_latest_hist_code_trade_date(security_type)
        sync_start = self._resolve_incremental_start_date(latest_date=latest_date, requested_begin_date=begin)
        return self._run_sync_job(
            task_name="get_hist_code_list",
            scope_key=scope_key,
            target_table=AD_HIST_CODE_DAILY_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _start_date: self._provider_fetch_hist_code_daily(security_type, sync_start, end),
            save_rows=self.repository.save_hist_code_daily_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
        )

    def sync_adj_factor(
        self,
        code_list: Sequence[str],
        local_path: str,
        force: bool = False,
    ) -> int:
        """同步单次复权因子表."""

        return self._sync_factor_buffered(
            factor_type=FactorType.ADJ,
            code_list=code_list,
            local_path=local_path,
            force=force,
        )

    def sync_backward_factor(
        self,
        code_list: Sequence[str],
        local_path: str,
        force: bool = False,
    ) -> int:
        """同步后复权因子表."""

        return self._sync_factor_buffered(
            factor_type=FactorType.BACKWARD,
            code_list=code_list,
            local_path=local_path,
            force=force,
        )

    def sync_etf_pcf(
        self,
        code_list: Sequence[str],
        force: bool = False,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        scope_key = self._build_code_scope_key("get_etf_pcf", normalized_codes)
        latest_date = self.repository.load_sync_checkpoint_date("get_etf_pcf", scope_key)
        total_inserted = self._run_sync_job(
            task_name="get_etf_pcf",
            scope_key=scope_key,
            target_table=AD_ETF_PCF_TABLE,
            latest_date=latest_date,
            fetch_rows=lambda _start_date: self._provider_fetch_etf_pcf(normalized_codes, None, None),
            save_rows=self.repository.save_etf_pcf_rows,
            row_date_getter=lambda row: row.trading_day,
            force=force,
            skip_if_success_today=True,
        )
        constituent_scope_key = self._build_code_scope_key("get_etf_pcf_constituent", normalized_codes)
        constituent_latest_date = self.repository.load_sync_checkpoint_date(
            "get_etf_pcf_constituent",
            constituent_scope_key,
        )
        total_inserted += self._run_sync_job(
            task_name="get_etf_pcf_constituent",
            scope_key=constituent_scope_key,
            target_table=AD_ETF_PCF_CONSTITUENT_TABLE,
            latest_date=constituent_latest_date,
            fetch_rows=lambda _start_date: self._provider_fetch_etf_pcf_constituent(normalized_codes, None, None),
            save_rows=self.repository.save_etf_pcf_constituent_rows,
            row_date_getter=lambda row: row.trading_day,
            force=force,
            skip_if_success_today=True,
        )
        return total_inserted

    def _get_factor(self, factor_type: str, code_list: Sequence[str], local_path: str, is_local: bool):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")

        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        self._validate_local_path(local_path)

        if not is_local:
            if factor_type == FactorType.ADJ:
                self.sync_adj_factor(code_list=normalized_codes, local_path=local_path, force=False)
            else:
                self.sync_backward_factor(code_list=normalized_codes, local_path=local_path, force=False)

        query = PriceFactorQuery(
            code_list=tuple(normalized_codes),
            local_path=local_path,
            is_local=is_local,
        )
        frame = self.repository.load_price_factor_frame(
            table_name=self._resolve_factor_table_name(factor_type),
            query=query,
        )
        if frame.empty:
            if factor_type == FactorType.ADJ:
                self.sync_adj_factor(code_list=normalized_codes, local_path=local_path, force=False)
            else:
                self.sync_backward_factor(code_list=normalized_codes, local_path=local_path, force=False)
            frame = self.repository.load_price_factor_frame(
                table_name=self._resolve_factor_table_name(factor_type),
                query=query,
            )

        if frame.empty:
            raise BaseDataCacheMissError(
                f"未找到 factor_type={factor_type}、code_count={len(normalized_codes)} 的复权因子数据。"
            )

        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        wide = frame.pivot_table(
            index="trade_date",
            columns="code",
            values="factor_value",
            aggfunc="last",
        ).sort_index()
        wide = wide.reindex(columns=[code for code in normalized_codes if code in wide.columns])
        wide.columns.name = None
        return wide

    def _sync_factor(
        self,
        factor_type: str,
        code_list: Sequence[str],
        local_path: str,
        force: bool,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        self._validate_local_path(local_path)

        scope_key = self._build_factor_scope_key(factor_type, normalized_codes)
        task_name = f"get_{factor_type}_factor"
        latest_date = self.repository.load_sync_checkpoint_date(task_name, scope_key)
        target_table = self._resolve_factor_table_name(factor_type)
        save_rows = (
            self.repository.save_adj_factor_rows
            if factor_type == FactorType.ADJ
            else self.repository.save_backward_factor_rows
        )
        def validate_factor_batch() -> tuple[bool, str]:
            existing_codes = self.repository.load_existing_codes(target_table, normalized_codes)
            missing_codes = [code for code in normalized_codes if code not in existing_codes]
            if missing_codes:
                preview = ",".join(missing_codes[:10])
                return False, (
                    f"{task_name} 写后校验失败，仍有 code 未落库 missing_count={len(missing_codes)} "
                    f"missing_preview={preview}"
                )
            return True, ""
        return self._run_sync_job(
            task_name=task_name,
            scope_key=scope_key,
            target_table=target_table,
            latest_date=latest_date,
            fetch_rows=lambda start_date: self._provider_fetch_price_factor(
                factor_type=factor_type,
                code_list=normalized_codes,
                start_date=start_date,
            ),
            save_rows=save_rows,
            row_date_getter=lambda row: row.trade_date,
            force=force,
            skip_if_success_today=True,
            success_validator=validate_factor_batch,
        )

    def _sync_factor_buffered(
        self,
        factor_type: str,
        code_list: Sequence[str],
        local_path: str,
        force: bool,
    ) -> int:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            raise BaseDataParameterError("code_list 不能为空。")
        self._validate_local_path(local_path)

        task_name = f"get_{factor_type}_factor"
        target_table = self._resolve_factor_table_name(factor_type)
        save_rows = (
            self.repository.save_adj_factor_rows
            if factor_type == FactorType.ADJ
            else self.repository.save_backward_factor_rows
        )

        total_inserted = 0
        buffered_rows: list[PriceFactorRow] = []
        buffered_metas: list[dict[str, object]] = []
        total_codes = len(normalized_codes)

        for index, code in enumerate(normalized_codes, start=1):
            logger.info("%s progress=%s/%s code=%s", task_name, index, total_codes, code)
            meta, code_rows = self._collect_factor_rows_for_code(
                task_name=task_name,
                target_table=target_table,
                factor_type=factor_type,
                code=code,
                force=force,
            )
            if meta is None or not code_rows:
                continue

            buffered_rows.extend(code_rows)
            buffered_metas.append(meta)

            if len(buffered_metas) >= FACTOR_SYNC_CODE_BATCH_SIZE:
                total_inserted += self._flush_factor_sync_buffer(
                    task_name=task_name,
                    target_table=target_table,
                    save_rows=save_rows,
                    buffered_rows=buffered_rows,
                    buffered_metas=buffered_metas,
                )
                buffered_rows = []
                buffered_metas = []

        if buffered_metas:
            total_inserted += self._flush_factor_sync_buffer(
                task_name=task_name,
                target_table=target_table,
                save_rows=save_rows,
                buffered_rows=buffered_rows,
                buffered_metas=buffered_metas,
            )

        return total_inserted

    def _collect_factor_rows_for_code(
        self,
        task_name: str,
        target_table: str,
        factor_type: str,
        code: str,
        force: bool,
    ) -> tuple[Optional[dict[str, object]], list[PriceFactorRow]]:
        run_date = datetime.now().date()
        started_at = utcnow()
        scope_key = self._build_factor_scope_key(factor_type, [code])
        latest_date = self.repository.load_sync_checkpoint_date(task_name, scope_key)

        if (
            not force
            and latest_date is not None
            and self.repository.has_successful_sync_today(task_name, scope_key, run_date)
        ):
            message = f"{task_name} 已在 {run_date} 同步成功，本次跳过。"
            logger.info(message)
            self._write_sync_log(
                task_name=task_name,
                scope_key=scope_key,
                run_date=run_date,
                status=SyncStatus.SKIPPED,
                target_table=target_table,
                start_date=latest_date,
                end_date=latest_date,
                row_count=0,
                message=message,
                started_at=started_at,
                finished_at=utcnow(),
            )
            return None, []

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
            return None, []

        stats = {
            "row_count": 0,
            "min_date": None,
            "max_date": latest_date,
        }
        code_rows: list[PriceFactorRow] = []

        try:
            for row in self._provider_fetch_price_factor(
                factor_type=factor_type,
                code_list=[code],
                start_date=latest_date,
            ):
                code_rows.append(row)
                stats["row_count"] += 1
                row_date = row.trade_date
                if stats["min_date"] is None or row_date < stats["min_date"]:
                    stats["min_date"] = row_date
                if stats["max_date"] is None or row_date > stats["max_date"]:
                    stats["max_date"] = row_date
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
            return None, []

        if stats["row_count"] == 0:
            message = f"{task_name} 未获取到任何远端数据，本次记为失败，避免错误写入成功 checkpoint。"
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
            return None, []

        meta = {
            "code": code,
            "scope_key": scope_key,
            "run_date": run_date,
            "target_table": target_table,
            "latest_date": latest_date,
            "row_count": stats["row_count"],
            "min_date": stats["min_date"],
            "max_date": stats["max_date"],
            "started_at": started_at,
        }
        return meta, code_rows

    def _flush_factor_sync_buffer(
        self,
        task_name: str,
        target_table: str,
        save_rows: Callable[[Iterable[PriceFactorRow]], int],
        buffered_rows: list[PriceFactorRow],
        buffered_metas: list[dict[str, object]],
    ) -> int:
        if not buffered_metas:
            return 0

        finished_at = utcnow()
        try:
            inserted_count = save_rows(buffered_rows)
        except Exception as exc:
            message = f"{task_name} 批量写入失败: {exc}"
            logger.exception(message)
            failed_finished_at = utcnow()
            for meta in buffered_metas:
                self._write_sync_log(
                    task_name=task_name,
                    scope_key=str(meta["scope_key"]),
                    run_date=meta["run_date"],
                    status=SyncStatus.FAILED,
                    target_table=target_table,
                    start_date=meta["latest_date"] or meta["min_date"],
                    end_date=meta["max_date"],
                    row_count=int(meta["row_count"]),
                    message=message,
                    started_at=meta["started_at"],
                    finished_at=failed_finished_at,
                )
            raise

        existing_codes = self.repository.load_existing_codes(
            target_table,
            [str(meta["code"]) for meta in buffered_metas],
        )
        for meta in buffered_metas:
            code = str(meta["code"])
            if code not in existing_codes:
                message = f"{task_name} 写后校验失败，仍有 code 未落库 code={code}"
                logger.warning(message)
                self._write_sync_log(
                    task_name=task_name,
                    scope_key=str(meta["scope_key"]),
                    run_date=meta["run_date"],
                    status=SyncStatus.FAILED,
                    target_table=target_table,
                    start_date=meta["latest_date"] or meta["min_date"],
                    end_date=meta["max_date"],
                    row_count=int(meta["row_count"]),
                    message=message,
                    started_at=meta["started_at"],
                    finished_at=finished_at,
                )
                continue

            message = (
                f"{task_name} 同步完成，"
                f"latest_date={meta['latest_date']}, inserted_rows={meta['row_count']}, "
                f"observed_rows={meta['row_count']}"
            )
            logger.info(message)
            self._write_sync_log(
                task_name=task_name,
                scope_key=str(meta["scope_key"]),
                run_date=meta["run_date"],
                status=SyncStatus.SUCCESS,
                target_table=target_table,
                start_date=meta["latest_date"] or meta["min_date"],
                end_date=meta["max_date"],
                row_count=int(meta["row_count"]),
                message=message,
                started_at=meta["started_at"],
                finished_at=finished_at,
            )

        return inserted_count

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
        skip_if_success_today: bool = False,
        success_validator: Optional[Callable[[], tuple[bool, str]]] = None,
    ) -> int:
        run_date = datetime.now().date()
        started_at = utcnow()

        if (
            not force
            and skip_if_success_today
            and latest_date is not None
            and self.repository.has_successful_sync_today(task_name, scope_key, run_date)
        ):
            message = f"{task_name} 已在 {run_date} 同步成功，本次跳过。"
            logger.info(message)
            self._write_sync_log(
                task_name=task_name,
                scope_key=scope_key,
                run_date=run_date,
                status=SyncStatus.SKIPPED,
                target_table=target_table,
                start_date=latest_date,
                end_date=latest_date,
                row_count=0,
                message=message,
                started_at=started_at,
                finished_at=utcnow(),
            )
            return 0

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

        stats = {
            "row_count": 0,
            "min_date": None,
            "max_date": latest_date,
        }

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

        if stats["row_count"] == 0:
            message = f"{task_name} 未获取到任何远端数据，本次记为失败，避免错误写入成功 checkpoint。"
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

        if success_validator is not None:
            ok, validation_message = success_validator()
            if not ok:
                message = validation_message or f"{task_name} 写后校验失败。"
                logger.warning(message)
                self._write_sync_log(
                    task_name=task_name,
                    scope_key=scope_key,
                    run_date=run_date,
                    status=SyncStatus.FAILED,
                    target_table=target_table,
                    start_date=latest_date or stats["min_date"],
                    end_date=stats["max_date"],
                    row_count=inserted_count,
                    message=message,
                    started_at=started_at,
                    finished_at=utcnow(),
                )
                return inserted_count

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

    def _provider_fetch_calendar(self, market: str, start_date: Optional[date]):
        return self.sync_provider.fetch_calendar(market=market, start_date=start_date)  # type: ignore[union-attr]

    def _provider_fetch_code_info(self, security_type: str, start_date: Optional[date]):
        return self.sync_provider.fetch_code_info(security_type=security_type, start_date=start_date)  # type: ignore[union-attr]

    def _provider_fetch_hist_code_daily(
        self,
        security_type: str,
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_hist_code_daily(  # type: ignore[union-attr]
            security_type=security_type,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_price_factor(
        self,
        factor_type: str,
        code_list: Sequence[str],
        start_date: Optional[date],
    ):
        return self.sync_provider.fetch_price_factor(  # type: ignore[union-attr]
            factor_type=factor_type,
            code_list=code_list,
            start_date=start_date,
        )

    def _provider_fetch_etf_pcf(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_etf_pcf(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
        )

    def _provider_fetch_etf_pcf_constituent(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        return self.sync_provider.fetch_etf_pcf_constituent(  # type: ignore[union-attr]
            code_list=code_list,
            start_date=start_date,
            end_date=end_date,
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
        safe_write_sync_log(
            repository=self.repository,
            task_name=task_name,
            scope_key=scope_key,
            run_date=run_date,
            status=status,
            target_table=target_table,
            start_date=start_date,
            end_date=end_date,
            row_count=row_count,
            message=message,
            started_at=started_at,
            finished_at=finished_at,
        )

    @staticmethod
    def _build_factor_scope_key(factor_type: str, code_list: Sequence[str]) -> str:
        return build_hashed_scope_key(factor_type, code_list, prefix="factor_type")

    @staticmethod
    def _resolve_factor_table_name(factor_type: str) -> str:
        if factor_type == FactorType.ADJ:
            return AD_ADJ_FACTOR_TABLE
        if factor_type == FactorType.BACKWARD:
            return AD_BACKWARD_FACTOR_TABLE
        raise ValueError(f"不支持的 factor_type: {factor_type}")

    @staticmethod
    def _build_code_scope_key(task_name: str, code_list: Sequence[str]) -> str:
        return build_hashed_scope_key(task_name, code_list, prefix="task")

    @staticmethod
    def _validate_security_type(security_type: str) -> str:
        text = str(security_type).strip()
        if not text:
            raise BaseDataParameterError("security_type 不能为空。")
        return text

    @staticmethod
    def _validate_market(market: str) -> str:
        text = str(market).strip()
        if not text:
            raise BaseDataParameterError("market 不能为空。")
        return text

    @staticmethod
    def _validate_data_type(data_type: str) -> None:
        if data_type not in {"str", "datetime"}:
            raise BaseDataParameterError("data_type 仅支持 'str' 或 'datetime'。")

    @staticmethod
    def _validate_date_range(start_date: date, end_date: date) -> None:
        if start_date > end_date:
            raise BaseDataParameterError("start_date 不能大于 end_date。")

    @staticmethod
    def _validate_optional_date_range(begin_date: Optional[date], end_date: Optional[date]) -> None:
        if begin_date is not None and end_date is not None and begin_date > end_date:
            raise BaseDataParameterError("begin_date 不能大于 end_date。")

    @staticmethod
    def _resolve_incremental_start_date(
        latest_date: Optional[date],
        requested_begin_date: Optional[date],
    ) -> Optional[date]:
        return resolve_incremental_start_date(latest_date, requested_begin_date)

    @staticmethod
    def _validate_local_path(local_path: str) -> None:
        if not isinstance(local_path, str):
            raise BaseDataParameterError("local_path 必须是字符串。")


__all__ = [
    "BaseData",
    "BaseDataCacheMissError",
    "BaseDataError",
    "BaseDataParameterError",
    "BaseDataSyncProvider",
]
