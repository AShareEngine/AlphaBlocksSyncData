#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData 官方 SDK 到本地增量同步框架的适配层."""

from __future__ import annotations

import logging
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence
from zoneinfo import ZoneInfo

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from aiquantbase.runtime_config import load_runtime_config
from sync_data_system.amazingdata_constants import (
    FactorType,
    Market,
)
from sync_data_system.base_data import BaseDataSyncProvider
from sync_data_system.config_paths import resolve_runtime_config_path
from sync_data_system.data_models import (
    BALANCE_SHEET_DATE_FIELDS,
    BALANCE_SHEET_FIELD_NAMES,
    BALANCE_SHEET_INT_FIELDS,
    BALANCE_SHEET_STRING_FIELDS,
    BalanceSheetRow,
    BjCodeMappingRow,
    BlockTradingRow,
    CASH_FLOW_DATE_FIELDS,
    CASH_FLOW_FIELD_NAMES,
    CASH_FLOW_INT_FIELDS,
    CASH_FLOW_STRING_FIELDS,
    CashFlowRow,
    INCOME_DATE_FIELDS,
    INCOME_FIELD_NAMES,
    INCOME_INT_FIELDS,
    INCOME_STRING_FIELDS,
    IncomeRow,
    KzzConvChangeRow,
    PROFIT_EXPRESS_DATE_FIELDS,
    PROFIT_EXPRESS_FIELD_NAMES,
    PROFIT_EXPRESS_INT_FIELDS,
    PROFIT_EXPRESS_STRING_FIELDS,
    ProfitExpressRow,
    PROFIT_NOTICE_DATE_FIELDS,
    PROFIT_NOTICE_FIELD_NAMES,
    PROFIT_NOTICE_INT_FIELDS,
    PROFIT_NOTICE_STRING_FIELDS,
    ProfitNoticeRow,
    EquityStructureRow,
    EquityPledgeFreezeRow,
    EquityRestrictedRow,
    CodeInfoRow,
    DividendRow,
    EtfPcfRow,
    EtfPcfConstituentRow,
    FundIopvRow,
    FundShareRow,
    HistCodeDailyRow,
    HistoryStockStatusRow,
    InfoPayloadRow,
    HolderNumRow,
    IndustryBaseInfoRow,
    IndustryConstituentRow,
    IndustryDailyRow,
    IndustryWeightRow,
    IndexConstituentRow,
    IndexWeightRow,
    KzzCallExplanationRow,
    KzzCallRow,
    KzzConvRow,
    KzzPutExplanationRow,
    KzzPutCallItemRow,
    KzzPutRow,
    MarketKlineRow,
    MarketSnapshotRow,
    LongHuBangRow,
    MarginDetailRow,
    MarginSummaryRow,
    PriceFactorRow,
    RightIssueRow,
    ShareHolderRow,
    StockBasicRow,
    TradeCalendarRow,
    KZZ_ISSUANCE_FIELD_NAMES,
    KZZ_ISSUANCE_INT_FIELDS,
    KZZ_ISSUANCE_STRING_FIELDS,
    KzzIssuanceRow,
    KzzShareRow,
    KzzSuspendRow,
    OptionBasicInfoRow,
    OptionMonCtrSpecsRow,
    OptionStdCtrSpecsRow,
    TreasuryYieldRow,
    normalize_code_list,
    should_keep_security_code,
    to_ch_date,
    to_yyyymmdd,
    KzzCorrRow,
)
from sync_data_system.info_data import InfoDataSyncProvider
from sync_data_system.market_data import MarketDataSyncProvider


logger = logging.getLogger(__name__)
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class AmazingDataSDKConfig:
    """AmazingData SDK 登录配置."""

    username: str
    password: str
    host: str
    port: int
    local_path: str

    @classmethod
    def from_env(
        cls,
        local_path: Optional[str] = None,
        runtime_path: Optional[str | Path] = None,
    ) -> "AmazingDataSDKConfig":
        resolved_runtime_path = resolve_runtime_config_path(runtime_path)
        runtime = load_runtime_config(resolved_runtime_path)
        sync_config = runtime.sync.amazingdata

        username = str(sync_config.username or "").strip()
        password = str(sync_config.password or "").strip()
        host = str(sync_config.host or "").strip()
        port = int(sync_config.port or 0)
        missing_fields: list[str] = []
        if not username:
            missing_fields.append("sync.amazingdata.username")
        if not password:
            missing_fields.append("sync.amazingdata.password")
        if not host:
            missing_fields.append("sync.amazingdata.host")
        if not port:
            missing_fields.append("sync.amazingdata.port")
        if missing_fields:
            raise ValueError(_format_missing_runtime_error(missing_fields, resolved_runtime_path))

        resolved_local_path = _normalize_local_path(
            local_path or str(sync_config.local_path or "").strip() or str(Path.cwd() / "amazing_data_cache")
        )
        Path(resolved_local_path.replace("//", "/")).mkdir(parents=True, exist_ok=True)

        return cls(
            username=username,
            password=password,
            host=host,
            port=port,
            local_path=resolved_local_path,
        )


class AmazingDataSDKSession:
    """AmazingData SDK 登录会话.

    这里把登录、对象创建和登出统一包起来，避免 provider 每个方法都重复做连接管理。
    """

    def __init__(self, config: AmazingDataSDKConfig) -> None:
        self.config = config
        self._ad = None
        self._base = None
        self._info = None
        self._market = None
        self._connected = False
        self._calendar_cache: dict[str, list] = {}
        self._raw_calendar_cache = None

    def ensure_connected(self) -> None:
        if self._connected:
            return

        try:
            import AmazingData as ad
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "未安装 AmazingData 官方 SDK，无法执行真实同步测试。"
            ) from exc

        ok = ad.login(
            username=self.config.username,
            password=self.config.password,
            host=self.config.host,
            port=self.config.port,
        )
        if ok is False:
            raise RuntimeError("AmazingData 登录失败，请检查账号、密码、IP、端口和权限。")

        self._ad = ad
        self._base = ad.BaseData()
        self._info = ad.InfoData()
        self._connected = True
        logger.info("AmazingData SDK login success host=%s port=%s", self.config.host, self.config.port)

    @property
    def base(self):
        self.ensure_connected()
        return self._base

    @property
    def info(self):
        self.ensure_connected()
        return self._info

    @property
    def market(self):
        self.ensure_connected()
        if self._market is None:
            self._market = self._build_market_client()
        return self._market

    def get_calendar_dates(self, market: str = Market.SH) -> list:
        self.ensure_connected()
        # 当前 AmazingData SDK 版本直接 `get_calendar()` 即可，`market` 参数在这里不再向下透传。
        cache_key = "default"
        if cache_key not in self._calendar_cache:
            dates = self._load_calendar_dates()
            self._calendar_cache[cache_key] = dates
        return self._calendar_cache[cache_key]

    def get_latest_trade_date(self, market: str = Market.SH):
        dates = self.get_calendar_dates(market=market)
        if not dates:
            fallback = date.today()
            logger.warning(
                "AmazingData get_calendar 不可用，latest_trade_date 临时回退为 today=%s",
                fallback,
            )
            return fallback
        return to_ch_date(dates[-1])

    def get_snapshot_date(self) -> date:
        """日级快照接口统一使用当天日期作为落库快照日.

        `get_code_info` / `get_stock_basic` 都属于“每日最新快照”类型，
        不需要为了拿一个快照日期再去强依赖 `get_calendar()`。
        """

        return date.today()

    def _load_calendar_dates(self) -> list:
        try:
            result = self.base.get_calendar()
        except Exception as exc:
            logger.warning("AmazingData get_calendar() 调用失败: %s", exc)
            return []

        normalized = _normalize_calendar_result(result)
        if normalized:
            logger.info("AmazingData get_calendar() success count=%s", len(normalized))
            return normalized

        logger.warning("AmazingData get_calendar() 返回空结果")
        return []

    def get_raw_calendar(self):
        self.ensure_connected()
        if self._raw_calendar_cache is None:
            try:
                self._raw_calendar_cache = self.base.get_calendar()
            except Exception as exc:
                logger.warning("AmazingData get_calendar() 原始结果获取失败: %s", exc)
                self._raw_calendar_cache = []
        return self._raw_calendar_cache

    def resolve_period_value(self, period: str | int) -> int:
        self.ensure_connected()
        text = str(period).strip()
        if not text:
            raise ValueError("period 不能为空。")
        if text.isdigit():
            return int(text)

        period_obj = getattr(getattr(self._ad, "constant", object()), "Period", None)
        if period_obj is not None and hasattr(period_obj, text):
            attr = getattr(period_obj, text)
            value = getattr(attr, "value", attr)
            return int(value)
        raise ValueError(f"无法解析官方 Period 枚举: {period!r}")

    def _build_market_client(self):
        errors: list[str] = []
        constructors = []
        raw_calendar = self.get_raw_calendar()
        if raw_calendar:
            constructors.append(((raw_calendar,), "MarketData(calendar)"))
        constructors.append((([],), "MarketData([])"))
        constructors.append((tuple(), "MarketData()"))

        for args, label in constructors:
            try:
                market = self._ad.MarketData(*args)
                logger.info("AmazingData %s 初始化成功", label)
                return market
            except Exception as exc:
                errors.append(f"{label}: {type(exc).__name__}: {exc}")
                continue

        raise RuntimeError("AmazingData MarketData 初始化失败: " + " | ".join(errors[-3:]))

    def close(self) -> None:
        if not self._connected or self._ad is None:
            return
        try:
            logout = getattr(self._ad, "logout", None)
            if callable(logout):
                logout(username=self.config.username)
        except Exception:
            logger.exception("AmazingData logout failed")
        finally:
            self._connected = False
            self._ad = None
            self._base = None
            self._info = None
            self._market = None
            self._calendar_cache.clear()
            self._raw_calendar_cache = None


class AmazingDataSDKProvider(BaseDataSyncProvider, InfoDataSyncProvider, MarketDataSyncProvider):
    """把 AmazingData 官方 SDK 返回值转换成我们的本地行模型."""

    def __init__(self, config: AmazingDataSDKConfig) -> None:
        self.config = config
        self.session = AmazingDataSDKSession(config)
        self._etf_pcf_result_cache: dict[tuple[str, ...], Any] = {}
        self._cleared_cache_files: set[str] = set()

    def close(self) -> None:
        self._etf_pcf_result_cache.clear()
        self._cleared_cache_files.clear()
        self.session.close()

    def fetch_calendar(
        self,
        market: str,
        start_date=None,
        end_date=None,
    ) -> Iterable[TradeCalendarRow]:
        for raw_date in self.session.get_calendar_dates(market=market):
            trade_date = to_ch_date(raw_date)
            if start_date is not None and trade_date < start_date:
                continue
            if end_date is not None and trade_date > end_date:
                continue
            yield TradeCalendarRow(trade_date=trade_date)

    def fetch_bj_code_mapping(self) -> Iterable[BjCodeMappingRow]:
        kwargs: dict[str, Any] = {
            "local_path": self.config.local_path,
            "is_local": False,
        }
        frame = _ensure_dataframe(self.session.info.get_bj_code_mapping(**kwargs), "get_bj_code_mapping")
        logger.info("AmazingData fetch_bj_code_mapping loaded rows=%s cols=%s", len(frame), len(frame.columns))
        for record in _frame_to_records(frame):
            old_code = _as_str(_record_get(record, "OLD_CODE", "old_code"))
            new_code = _as_str(_record_get(record, "NEW_CODE", "new_code"))
            if not old_code or not new_code:
                continue
            yield BjCodeMappingRow(
                old_code=old_code,
                new_code=new_code,
                security_name=_as_str(_record_get(record, "SECURITY_NAME", "security_name")),
                listing_date=_as_int(_record_get(record, "LISTING_DATE", "listing_date")),
            )

    def fetch_code_info(
        self,
        security_type: str,
        start_date=None,
        end_date=None,
    ) -> Iterable[CodeInfoRow]:
        logger.info("AmazingData fetch_code_info start security_type=%s", security_type)
        frame = _ensure_dataframe(self.session.base.get_code_info(security_type=security_type), "get_code_info")
        logger.info("AmazingData fetch_code_info loaded rows=%s cols=%s", len(frame), len(frame.columns))

        for code, row in frame.iterrows():
            market_code = str(code).strip()
            if not should_keep_security_code(market_code, security_type=security_type):
                continue
            yield CodeInfoRow(
                security_type=security_type,
                code=market_code,
                symbol=_as_str(_series_get(row, "symbol", "SYMBOL")),
                security_status_raw=_stringify(_series_get(row, "security_status", "SECURITY_STATUS")),
                pre_close=_as_float(_series_get(row, "pre_close", "PRECLOSE", "PRE_CLOSE")),
                high_limited=_as_float(_series_get(row, "high_limited", "HIGH_LIMITED")),
                low_limited=_as_float(_series_get(row, "low_limited", "LOW_LIMITED")),
                price_tick=_as_float(_series_get(row, "price_tick", "PRICE_TICK")),
            )

    def fetch_hist_code_daily(
        self,
        security_type: str,
        start_date=None,
        end_date=None,
    ) -> Iterable[HistCodeDailyRow]:
        logger.info(
            "AmazingData fetch_hist_code_daily start security_type=%s start_date=%s end_date=%s",
            security_type,
            start_date,
            end_date,
        )
        latest_trade_date = self.session.get_latest_trade_date(Market.SH)
        if latest_trade_date is None:
            return

        actual_end = end_date or latest_trade_date
        actual_start = start_date or actual_end
        if actual_start > actual_end:
            return

        calendar_dates = [to_ch_date(item) for item in self.session.get_calendar_dates(Market.SH)]
        iter_dates = [
            current_date
            for current_date in calendar_dates
            if actual_start <= current_date <= actual_end
        ]
        if not iter_dates:
            logger.warning(
                "未获取到可用交易日历，fetch_hist_code_daily 改为按自然日遍历: start=%s end=%s",
                actual_start,
                actual_end,
            )
            iter_dates = list(_iter_natural_dates(actual_start, actual_end))

        for trade_date in iter_dates:
            try:
                code_list = self.session.base.get_hist_code_list(
                    security_type=security_type,
                    start_date=to_yyyymmdd(trade_date),
                    end_date=to_yyyymmdd(trade_date),
                    local_path=self.config.local_path,
                )
            except Exception as exc:
                logger.warning(
                    "fetch_hist_code_daily 跳过 trade_date=%s security_type=%s: %s",
                    trade_date,
                    security_type,
                    exc,
                )
                continue
            for code in normalize_code_list(code_list or []):
                yield HistCodeDailyRow(
                    trade_date=trade_date,
                    security_type=security_type,
                    code=code,
                )

    def fetch_price_factor(
        self,
        factor_type: str,
        code_list: Sequence[str],
        start_date=None,
        end_date=None,
    ) -> Iterable[PriceFactorRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        logger.info(
            "AmazingData fetch_price_factor start factor_type=%s code_count=%s",
            factor_type,
            len(normalized_codes),
        )

        if factor_type == FactorType.ADJ and len(normalized_codes) == 1:
            yield from self._fetch_adj_factor_rows_by_code(
                code=normalized_codes[0],
                start_date=start_date,
                end_date=end_date,
            )
            return

        if factor_type == FactorType.BACKWARD and len(normalized_codes) == 1:
            yield from self._fetch_backward_factor_rows_by_code(
                code=normalized_codes[0],
                start_date=start_date,
                end_date=end_date,
            )
            return

        if factor_type == FactorType.ADJ:
            frame = self.session.base.get_adj_factor(
                code_list=normalized_codes,
                local_path=self.config.local_path,
                is_local=False,
            )
        elif factor_type == FactorType.BACKWARD:
            frame = self.session.base.get_backward_factor(
                code_list=normalized_codes,
                local_path=self.config.local_path,
                is_local=False,
            )
        else:
            raise ValueError(f"不支持的 factor_type: {factor_type}")

        frame = _ensure_dataframe(frame, f"fetch_price_factor({factor_type})")
        frame = _normalize_price_factor_frame(frame, normalized_codes)
        logger.info("AmazingData fetch_price_factor loaded rows=%s cols=%s", len(frame), len(frame.columns))
        for trade_date, row in frame.iterrows():
            current_date = to_ch_date(trade_date)
            if start_date is not None and current_date < start_date:
                continue
            if end_date is not None and current_date > end_date:
                continue
            for code, factor_value in row.items():
                numeric_value = _as_float(factor_value)
                if numeric_value is None:
                    continue
                yield PriceFactorRow(
                    trade_date=current_date,
                    code=str(code).strip(),
                    factor_value=numeric_value,
                )

    def _fetch_adj_factor_rows_by_code(
        self,
        code: str,
        start_date=None,
        end_date=None,
    ) -> Iterable[PriceFactorRow]:
        requested_code = normalize_code_list([code])[0]
        frame = self.session.base.get_adj_factor(
            code_list=[requested_code],
            local_path=self.config.local_path,
            is_local=False,
        )
        frame = _ensure_dataframe(frame, f"fetch_adj_factor({requested_code})")
        frame = _normalize_single_price_factor_frame(frame, requested_code)
        if frame.empty:
            logger.warning("AmazingData fetch_adj_factor empty code=%s", requested_code)
            return

        if len(frame.columns) != 1:
            raise ValueError(
                f"get_adj_factor 单 code 返回列数异常 code={requested_code} column_count={len(frame.columns)}"
            )

        returned_code = str(frame.columns[0]).strip()
        if returned_code != requested_code:
            raise ValueError(
                "get_adj_factor 返回列名与请求 code 不一致 "
                f"requested_code={requested_code} returned_code={returned_code}"
            )

        logger.info(
            "AmazingData fetch_adj_factor(single) loaded code=%s rows=%s",
            requested_code,
            len(frame),
        )
        series = frame.iloc[:, 0]
        for trade_date, factor_value in series.items():
            current_date = to_ch_date(trade_date)
            if start_date is not None and current_date < start_date:
                continue
            if end_date is not None and current_date > end_date:
                continue
            numeric_value = _as_float(factor_value)
            if numeric_value is None:
                continue
            yield PriceFactorRow(
                trade_date=current_date,
                code=requested_code,
                factor_value=numeric_value,
            )

    def _fetch_backward_factor_rows_by_code(
        self,
        code: str,
        start_date=None,
        end_date=None,
    ) -> Iterable[PriceFactorRow]:
        requested_code = normalize_code_list([code])[0]
        frame = self.session.base.get_backward_factor(
            code_list=[requested_code],
            local_path=self.config.local_path,
            is_local=False,
        )
        frame = _ensure_dataframe(frame, f"fetch_backward_factor({requested_code})")
        frame = _normalize_single_price_factor_frame(frame, requested_code)
        if frame.empty:
            logger.warning("AmazingData fetch_backward_factor empty code=%s", requested_code)
            return

        if len(frame.columns) != 1:
            raise ValueError(
                f"get_backward_factor 单 code 返回列数异常 code={requested_code} column_count={len(frame.columns)}"
            )

        returned_code = str(frame.columns[0]).strip()
        if returned_code != requested_code:
            raise ValueError(
                "get_backward_factor 返回列名与请求 code 不一致 "
                f"requested_code={requested_code} returned_code={returned_code}"
            )

        logger.info(
            "AmazingData fetch_backward_factor(single) loaded code=%s rows=%s",
            requested_code,
            len(frame),
        )
        series = frame.iloc[:, 0]
        for trade_date, factor_value in series.items():
            current_date = to_ch_date(trade_date)
            if start_date is not None and current_date < start_date:
                continue
            if end_date is not None and current_date > end_date:
                continue
            numeric_value = _as_float(factor_value)
            if numeric_value is None:
                continue
            yield PriceFactorRow(
                trade_date=current_date,
                code=requested_code,
                factor_value=numeric_value,
            )

    def fetch_etf_pcf(
        self,
        code_list: Sequence[str],
        start_date=None,
        end_date=None,
    ) -> Iterable[EtfPcfRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        result = self._get_etf_pcf_raw_result(normalized_codes)
        info_result = result[0] if isinstance(result, (tuple, list)) and result else result
        frame = _ensure_dataframe(info_result, "get_etf_pcf")
        logger.info(
            "AmazingData get_etf_pcf loaded result_type=%s info_rows=%s",
            type(result).__name__,
            len(frame),
        )

        for etf_code, row in frame.iterrows():
            code = str(etf_code).strip()
            if not code:
                continue
            yield EtfPcfRow(
                etf_code=code,
                creation_redemption_unit=_as_int(
                    _series_get(row, "CREATION_REDEMPTION_UNIT", "creation_redemption_unit")
                ),
                max_cash_ratio=_as_str(_series_get(row, "MAX_CASH_RATIO", "max_cash_ratio")),
                publish=_as_str(_series_get(row, "PUBLISH", "publish")),
                creation=_as_str(_series_get(row, "CREATION", "creation")),
                redemption=_as_str(_series_get(row, "REDEMPTION", "redemption")),
                creation_redemption_switch=_as_str(
                    _series_get(row, "CREATION_REDEMPTION_SWITCH", "creation_redemption_switch")
                ),
                record_num=_as_int(_series_get(row, "RECORD_NUM", "record_num")),
                total_record_num=_as_int(_series_get(row, "TOTAL_RECORD_NUM", "total_record_num")),
                estimate_cash_component=_as_float(
                    _series_get(row, "ESTIMATE_CASH_COMPONENT", "estimate_cash_component")
                ),
                trading_day=_as_date(_series_get(row, "TRADING_DAY", "trading_day")),
                pre_trading_day=_as_date(_series_get(row, "PRE_TRADING_DAY", "pre_trading_day")),
                cash_component=_as_float(_series_get(row, "CASH_COMPONENT", "cash_component")),
                nav_per_cu=_as_float(_series_get(row, "NAV_PER_CU", "nav_per_cu")),
                nav=_as_float(_series_get(row, "NAV", "nav")),
                symbol=_as_str(_series_get(row, "SYMBOL", "symbol")),
                fund_management_company=_as_str(
                    _series_get(row, "FUND_MANAGEMENT_COMPANY", "fund_management_company")
                ),
                underlying_security_id=_as_str(
                    _series_get(row, "UNDERLYING_SECURITY_ID", "underlying_security_id")
                ),
                underlying_security_id_source=_as_str(
                    _series_get(row, "UNDERLYING_SECURITY_ID_SOURCE", "underlying_security_id_source")
                ),
                dividend_per_cu=_as_float(_series_get(row, "DIVIDEND_PER_CU", "dividend_per_cu")),
                creation_limit=_as_float(_series_get(row, "CREATION_LIMIT", "creation_limit")),
                redemption_limit=_as_float(_series_get(row, "REDEMPTION_LIMIT", "redemption_limit")),
                creation_limit_per_user=_as_float(
                    _series_get(row, "CREATION_LIMIT_PER_USER", "creation_limit_per_user")
                ),
                redemption_limit_per_user=_as_float(
                    _series_get(row, "REDEMPTION_LIMIT_PER_USER", "redemption_limit_per_user")
                ),
                net_creation_limit=_as_float(_series_get(row, "NET_CREATION_LIMIT", "net_creation_limit")),
                net_redemption_limit=_as_float(
                    _series_get(row, "NET_REDEMPTION_LIMIT", "net_redemption_limit")
                ),
                net_creation_limit_per_user=_as_float(
                    _series_get(row, "NET_CREATION_LIMIT_PER_USER", "net_creation_limit_per_user")
                ),
                net_redemption_limit_per_user=_as_float(
                    _series_get(row, "NET_REDEMPTION_LIMIT_PER_USER", "net_redemption_limit_per_user")
                ),
            )

    def fetch_etf_pcf_constituent(
        self,
        code_list: Sequence[str],
        start_date=None,
        end_date=None,
    ) -> Iterable[EtfPcfConstituentRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        result = self._get_etf_pcf_raw_result(normalized_codes)
        if not isinstance(result, (tuple, list)) or len(result) < 2:
            logger.info("AmazingData get_etf_pcf constituent result missing code_count=%s", len(normalized_codes))
            return

        info_result = result[0]
        constituent_result = result[1]
        info_frame = _ensure_dataframe(info_result, "get_etf_pcf(info)")
        trading_day_map: dict[str, Optional[date]] = {}
        for etf_code, row in info_frame.iterrows():
            code = str(etf_code).strip()
            if not code:
                continue
            trading_day_map[code] = _as_date(_series_get(row, "TRADING_DAY", "trading_day"))

        for etf_code, frame in _iter_code_frames_from_result(constituent_result, action="get_etf_pcf_constituent"):
            code = _as_str(etf_code)
            if not code:
                continue
            for record in _frame_to_records(frame):
                yield EtfPcfConstituentRow(
                    etf_code=code,
                    trading_day=trading_day_map.get(code),
                    underlying_symbol=_as_str(_record_get(record, "UNDERLYING_SYMBOL", "underlying_symbol")),
                    component_share=_as_float(_record_get(record, "COMPONENT_SHARE", "component_share")),
                    substitute_flag=_as_str(_record_get(record, "SUBSTITUTE_FLAG", "substitute_flag")),
                    premium_ratio=_as_float(_record_get(record, "PREMIUM_RATIO", "premium_ratio")),
                    discount_ratio=_as_float(_record_get(record, "DISCOUNT_RATIO", "discount_ratio")),
                    creation_cash_substitute=_as_float(
                        _record_get(record, "CREATION_CASH_SUBSTITUTE", "creation_cash_substitute")
                    ),
                    redemption_cash_substitute=_as_float(
                        _record_get(record, "REDEMPTION_CASH_SUBSTITUTE", "redemption_cash_substitute")
                    ),
                    substitution_cash_amount=_as_float(
                        _record_get(record, "SUBSTITUTION_CASH_AMOUNT", "substitution_cash_amount")
                    ),
                    underlying_security_id=_as_str(
                        _record_get(record, "UNDERLYING_SECURITY_ID", "underlying_security_id")
                    ),
                )

    def _get_etf_pcf_raw_result(self, normalized_codes: Sequence[str]):
        cache_key = tuple(normalized_codes)
        if cache_key not in self._etf_pcf_result_cache:
            self._etf_pcf_result_cache[cache_key] = self.session.base.get_etf_pcf(list(normalized_codes))
        return self._etf_pcf_result_cache[cache_key]

    def fetch_stock_basic(
        self,
        code_list: Sequence[str],
        start_date=None,
        end_date=None,
    ) -> Iterable[StockBasicRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        logger.info("AmazingData fetch_stock_basic start code_count=%s", len(normalized_codes))

        snapshot_date = self.session.get_snapshot_date()
        if start_date is not None and snapshot_date < start_date:
            return
        if end_date is not None and snapshot_date > end_date:
            return

        frame = _ensure_dataframe(self.session.info.get_stock_basic(normalized_codes), "get_stock_basic")
        logger.info("AmazingData fetch_stock_basic loaded rows=%s cols=%s", len(frame), len(frame.columns))
        for record in _frame_to_records(frame):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield StockBasicRow(
                snapshot_date=snapshot_date,
                market_code=market_code,
                security_name=_as_str(_record_get(record, "SECURITY_NAME", "security_name")),
                comp_name=_as_str(_record_get(record, "COMP_NAME", "comp_name")),
                pinyin=_as_str(_record_get(record, "PINYIN", "pinyin")),
                comp_name_eng=_as_str(_record_get(record, "COMP_NAME_ENG", "comp_name_eng")),
                list_date=_as_int(_record_get(record, "LISTDATE", "list_date")),
                delist_date=_as_int(_record_get(record, "DELISTDATE", "delist_date")),
                listplate_name=_as_str(_record_get(record, "LISTPLATE_NAME", "listplate_name")),
                comp_sname_eng=_as_str(_record_get(record, "COMP_SNAME_ENG", "comp_sname_eng")),
                is_listed=_as_int(_record_get(record, "IS_LISTED", "is_listed")),
            )

    def fetch_kline(
        self,
        code_list: Sequence[str],
        begin_date: date,
        end_date: date,
        period: str,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Iterable[MarketKlineRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        period_value = self.session.resolve_period_value(period)
        logger.info(
            "AmazingData fetch_kline start code_count=%s begin_date=%s end_date=%s period=%s",
            len(normalized_codes),
            begin_date,
            end_date,
            period,
        )
        base_kwargs: dict[str, Any] = {
            "begin_date": to_yyyymmdd(begin_date),
            "end_date": to_yyyymmdd(end_date),
        }
        if begin_time is not None:
            base_kwargs["begin_time"] = begin_time
        if end_time is not None:
            base_kwargs["end_time"] = end_time

        result = self._query_kline_with_variants(
            code_list=normalized_codes,
            period=period,
            period_value=period_value,
            base_kwargs=base_kwargs,
        )
        logger.info(
            "AmazingData fetch_kline loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for code, frame in _iter_code_frames_from_result(result, action="query_kline"):
            try:
                normalized_frame = _prepare_market_time_frame(frame, action="query_kline", time_field="trade_time")
            except Exception:
                logger.exception(
                    "query_kline 原始结构无法识别 code=%s columns=%s index_type=%s index_name=%s index_preview=%s head=%s",
                    code,
                    list(frame.columns),
                    type(frame.index).__name__,
                    getattr(frame.index, "name", None),
                    list(frame.index[:3]) if len(frame.index) >= 3 else list(frame.index),
                    frame.head(3).to_dict("records") if pd is not None else "<pandas unavailable>",
                )
                raise
            normalized_frame["code"] = code
            for record in _frame_to_records(normalized_frame):
                trade_time = _to_datetime(_record_get(record, "trade_time", "TRADE_TIME"))
                if trade_time is None:
                    continue
                yield MarketKlineRow(
                    trade_time=trade_time,
                    code=code,
                    open=_as_float(_record_get(record, "open", "OPEN")),
                    high=_as_float(_record_get(record, "high", "HIGH")),
                    low=_as_float(_record_get(record, "low", "LOW")),
                    close=_as_float(_record_get(record, "close", "CLOSE")),
                    volume=_as_float(_record_get(record, "volume", "VOLUME")),
                    amount=_as_float(_record_get(record, "amount", "AMOUNT")),
                )

    def _query_kline_with_variants(
        self,
        code_list: Sequence[str],
        period: str,
        period_value: int,
        base_kwargs: dict[str, Any],
    ):
        variants: list[dict[str, Any]] = []
        variant_with_resolved = dict(base_kwargs)
        variant_with_resolved["period"] = int(period_value)
        variants.append(variant_with_resolved)
        errors: list[str] = []
        for kwargs in variants:
            try:
                logger.info("AmazingData query_kline try kwargs=%s", kwargs)
                result = self.session.market.query_kline(code_list, **kwargs)
                if not _is_sdk_result_empty(result):
                    return result
                errors.append(f"kwargs={kwargs} -> empty_result")
            except Exception as exc:
                errors.append(f"kwargs={kwargs} -> {type(exc).__name__}: {exc}")
                continue

        logger.warning("AmazingData query_kline 所有尝试均未取到数据: %s", " | ".join(errors[-3:]))
        return {}

    def fetch_snapshot(
        self,
        code_list: Sequence[str],
        begin_date: date,
        end_date: date,
        begin_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> Iterable[MarketSnapshotRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        logger.info(
            "AmazingData fetch_snapshot start code_count=%s begin_date=%s end_date=%s",
            len(normalized_codes),
            begin_date,
            end_date,
        )
        kwargs: dict[str, Any] = {
            "begin_date": to_yyyymmdd(begin_date),
            "end_date": to_yyyymmdd(end_date),
        }
        if begin_time is not None:
            kwargs["begin_time"] = begin_time
        if end_time is not None:
            kwargs["end_time"] = end_time

        result = self.session.market.query_snapshot(normalized_codes, **kwargs)
        logger.info(
            "AmazingData fetch_snapshot loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for code, frame in _iter_code_frames_from_result(result, action="query_snapshot"):
            normalized_frame = _prepare_market_time_frame(frame, action="query_snapshot", time_field="trade_time")
            normalized_frame["code"] = code
            for record in _frame_to_records(normalized_frame):
                trade_time = _to_datetime(_record_get(record, "trade_time", "TRADE_TIME"))
                if trade_time is None:
                    continue
                yield MarketSnapshotRow(
                    trade_time=trade_time,
                    code=code,
                    pre_close=_as_float(_record_get(record, "pre_close", "PRECLOSE", "PRE_CLOSE")),
                    last=_as_float(_record_get(record, "last", "LAST")),
                    open=_as_float(_record_get(record, "open", "OPEN")),
                    high=_as_float(_record_get(record, "high", "HIGH")),
                    low=_as_float(_record_get(record, "low", "LOW")),
                    close=_as_float(_record_get(record, "close", "CLOSE")),
                    volume=_as_float(_record_get(record, "volume", "VOLUME")),
                    amount=_as_float(_record_get(record, "amount", "AMOUNT")),
                    num_trades=_as_float(_record_get(record, "num_trades", "NUM_TRADES")),
                    high_limited=_as_float(_record_get(record, "high_limited", "HIGH_LIMITED")),
                    low_limited=_as_float(_record_get(record, "low_limited", "LOW_LIMITED")),
                    ask_price1=_as_float(_record_get(record, "ask_price1", "ASK_PRICE1")),
                    ask_price2=_as_float(_record_get(record, "ask_price2", "ASK_PRICE2")),
                    ask_price3=_as_float(_record_get(record, "ask_price3", "ASK_PRICE3")),
                    ask_price4=_as_float(_record_get(record, "ask_price4", "ASK_PRICE4")),
                    ask_price5=_as_float(_record_get(record, "ask_price5", "ASK_PRICE5")),
                    ask_volume1=_as_int(_record_get(record, "ask_volume1", "ASK_VOLUME1")),
                    ask_volume2=_as_int(_record_get(record, "ask_volume2", "ASK_VOLUME2")),
                    ask_volume3=_as_int(_record_get(record, "ask_volume3", "ASK_VOLUME3")),
                    ask_volume4=_as_int(_record_get(record, "ask_volume4", "ASK_VOLUME4")),
                    ask_volume5=_as_int(_record_get(record, "ask_volume5", "ASK_VOLUME5")),
                    bid_price1=_as_float(_record_get(record, "bid_price1", "BID_PRICE1")),
                    bid_price2=_as_float(_record_get(record, "bid_price2", "BID_PRICE2")),
                    bid_price3=_as_float(_record_get(record, "bid_price3", "BID_PRICE3")),
                    bid_price4=_as_float(_record_get(record, "bid_price4", "BID_PRICE4")),
                    bid_price5=_as_float(_record_get(record, "bid_price5", "BID_PRICE5")),
                    bid_volume1=_as_int(_record_get(record, "bid_volume1", "BID_VOLUME1")),
                    bid_volume2=_as_int(_record_get(record, "bid_volume2", "BID_VOLUME2")),
                    bid_volume3=_as_int(_record_get(record, "bid_volume3", "BID_VOLUME3")),
                    bid_volume4=_as_int(_record_get(record, "bid_volume4", "BID_VOLUME4")),
                    bid_volume5=_as_int(_record_get(record, "bid_volume5", "BID_VOLUME5")),
                    iopv=_as_float(_record_get(record, "iopv", "IOPV")),
                    trading_phase_code=_as_str(_record_get(record, "trading_phase_code", "TRADING_PHASE_CODE")),
                    total_long_position=_as_int(_record_get(record, "total_long_position", "TOTAL_LONG_POSITION")),
                    pre_settle=_as_float(_record_get(record, "pre_settle", "PRE_SETTLE")),
                    auction_price=_as_float(_record_get(record, "auction_price", "AUCTION_PRICE")),
                    auction_volume=_as_int(_record_get(record, "auction_volume", "AUCTION_VOLUME")),
                    settle=_as_float(_record_get(record, "settle", "SETTLE")),
                    contract_type=_as_str(_record_get(record, "contract_type", "CONTRACT_TYPE")),
                    expire_date=_as_int(_record_get(record, "expire_date", "EXPIRE_DATE")),
                    underlying_security_code=_as_str(
                        _record_get(record, "underlying_security_code", "UNDERLYING_SECURITY_CODE")
                    ),
                    exercise_price=_as_float(_record_get(record, "exercise_price", "EXERCISE_PRICE")),
                    action_day=_as_str(_record_get(record, "action_day", "ACTION_DAY")),
                    trading_day=_as_str(_record_get(record, "trading_day", "TRADING_DAY")),
                    pre_open_interest=_as_int(_record_get(record, "pre_open_interest", "PRE_OPEN_INTEREST")),
                    open_interest=_as_int(_record_get(record, "open_interest", "OPEN_INTEREST")),
                    average_price=_as_float(_record_get(record, "average_price", "AVERAGE_PRICE")),
                    nominal_price=_as_float(_record_get(record, "nominal_price", "NOMINAL_PRICE")),
                    ref_price=_as_float(_record_get(record, "ref_price", "REF_PRICE")),
                    bid_price_limit_up=_as_float(_record_get(record, "bid_price_limit_up", "BID_PRICE_LIMIT_UP")),
                    bid_price_limit_down=_as_float(
                        _record_get(record, "bid_price_limit_down", "BID_PRICE_LIMIT_DOWN")
                    ),
                    offer_price_limit_up=_as_float(
                        _record_get(record, "offer_price_limit_up", "OFFER_PRICE_LIMIT_UP")
                    ),
                    offer_price_limit_down=_as_float(
                        _record_get(record, "offer_price_limit_down", "OFFER_PRICE_LIMIT_DOWN")
                    ),
                )

    def fetch_history_stock_status(
        self,
        code_list: Sequence[str],
        start_date=None,
        end_date=None,
    ) -> Iterable[HistoryStockStatusRow]:
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        logger.info(
            "AmazingData fetch_history_stock_status start code_count=%s start_date=%s end_date=%s",
            len(normalized_codes),
            start_date,
            end_date,
        )

        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        if start_date is not None:
            kwargs["begin_date"] = to_yyyymmdd(start_date)
        if end_date is not None:
            kwargs["end_date"] = to_yyyymmdd(end_date)

        result = self.session.info.get_history_stock_status(**kwargs)
        logger.info(
            "AmazingData fetch_history_stock_status loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_history_stock_status",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            trade_date_value = _record_get(record, "TRADE_DATE", "trade_date")
            if not market_code or trade_date_value is None:
                continue
            trade_date = to_ch_date(trade_date_value)
            if start_date is not None and trade_date < start_date:
                continue
            if end_date is not None and trade_date > end_date:
                continue
            yield HistoryStockStatusRow(
                trade_date=trade_date,
                market_code=market_code,
                preclose=_as_float(_record_get(record, "PRECLOSE", "preclose", "PRE_CLOSE")),
                high_limited=_as_float(_record_get(record, "HIGH_LIMITED", "high_limited")),
                low_limited=_as_float(_record_get(record, "LOW_LIMITED", "low_limited")),
                price_high_lmt_rate=_as_float(
                    _record_get(record, "PRICE_HIGH_LMT_RATE", "price_high_lmt_rate")
                ),
                price_low_lmt_rate=_as_float(
                    _record_get(record, "PRICE_LOW_LMT_RATE", "price_low_lmt_rate")
                ),
                is_st_sec=_as_str(_record_get(record, "IS_ST_SEC", "is_st_sec")),
                is_susp_sec=_as_str(_record_get(record, "IS_SUSP_SEC", "is_susp_sec")),
                is_wd_sec=_as_str(_record_get(record, "IS_WD_SEC", "is_wd_sec")),
                is_xr_sec=_as_str(_record_get(record, "IS_XR_SEC", "is_xr_sec")),
            )

    def fetch_balance_sheet(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_balance_sheet(**kwargs)
        logger.info(
            "AmazingData get_balance_sheet loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_balance_sheet",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            values: dict[str, Any] = {}
            for field_name in BALANCE_SHEET_FIELD_NAMES:
                raw_value = _record_get(record, field_name.upper(), field_name)
                if field_name in BALANCE_SHEET_DATE_FIELDS:
                    values[field_name] = _as_date(raw_value)
                elif field_name in BALANCE_SHEET_INT_FIELDS:
                    values[field_name] = _as_int(raw_value)
                elif field_name in BALANCE_SHEET_STRING_FIELDS:
                    values[field_name] = _as_str(raw_value)
                else:
                    values[field_name] = _as_float(raw_value)
            if not values.get("market_code"):
                continue
            yield BalanceSheetRow(**values)

    def fetch_cash_flow(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_cash_flow(**kwargs)
        logger.info(
            "AmazingData get_cash_flow loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_cash_flow",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            values: dict[str, Any] = {}
            for field_name in CASH_FLOW_FIELD_NAMES:
                raw_value = _record_get(record, field_name.upper(), field_name)
                if field_name in CASH_FLOW_DATE_FIELDS:
                    values[field_name] = _as_date(raw_value)
                elif field_name in CASH_FLOW_INT_FIELDS:
                    values[field_name] = _as_int(raw_value)
                elif field_name in CASH_FLOW_STRING_FIELDS:
                    values[field_name] = _as_str(raw_value)
                else:
                    values[field_name] = _as_float(raw_value)
            if not values.get("market_code"):
                continue
            yield CashFlowRow(**values)

    def fetch_income(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_income(**kwargs)
        logger.info(
            "AmazingData get_income loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_income",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            values: dict[str, Any] = {}
            for field_name in INCOME_FIELD_NAMES:
                raw_value = _record_get(record, field_name.upper(), field_name)
                if field_name in INCOME_DATE_FIELDS:
                    values[field_name] = _as_date(raw_value)
                elif field_name in INCOME_INT_FIELDS:
                    values[field_name] = _as_int(raw_value)
                elif field_name in INCOME_STRING_FIELDS:
                    values[field_name] = _as_str(raw_value)
                else:
                    values[field_name] = _as_float(raw_value)
            if not values.get("market_code"):
                continue
            yield IncomeRow(**values)

    def fetch_profit_express(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_profit_express(**kwargs)
        logger.info(
            "AmazingData get_profit_express loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_profit_express",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            values: dict[str, Any] = {}
            for field_name in PROFIT_EXPRESS_FIELD_NAMES:
                raw_value = _record_get(record, field_name.upper(), field_name)
                if field_name in PROFIT_EXPRESS_DATE_FIELDS:
                    values[field_name] = _as_date(raw_value)
                elif field_name in PROFIT_EXPRESS_INT_FIELDS:
                    values[field_name] = _as_int(raw_value)
                elif field_name in PROFIT_EXPRESS_STRING_FIELDS:
                    values[field_name] = _as_str(raw_value)
                else:
                    values[field_name] = _as_float(raw_value)
            if not values.get("market_code"):
                continue
            yield ProfitExpressRow(**values)

    def fetch_profit_notice(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_profit_notice(**kwargs)
        logger.info(
            "AmazingData get_profit_notice loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_profit_notice",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            values: dict[str, Any] = {}
            for field_name in PROFIT_NOTICE_FIELD_NAMES:
                raw_value = _record_get(record, field_name.upper(), field_name)
                if field_name in PROFIT_NOTICE_DATE_FIELDS:
                    values[field_name] = _as_date(raw_value)
                elif field_name in PROFIT_NOTICE_INT_FIELDS:
                    values[field_name] = _as_int(raw_value)
                elif field_name in PROFIT_NOTICE_STRING_FIELDS:
                    values[field_name] = _as_str(raw_value)
                else:
                    values[field_name] = _as_float(raw_value)
            if not values.get("market_code"):
                continue
            yield ProfitNoticeRow(**values)

    def fetch_fund_share(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_fund_share(**kwargs)
        logger.info(
            "AmazingData get_fund_share loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_fund_share",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield FundShareRow(
                market_code=market_code,
                fund_share=_as_float(_record_get(record, "FUND_SHARE", "fund_share")),
                change_reason=_as_str(_record_get(record, "CHANGE_REASON", "change_reason")),
                is_consolidated_data=_as_int(
                    _record_get(record, "IS_CONSOLIDATED_DATA", "is_consolidated_data")
                ),
                ann_date=_as_date(_record_get(record, "ANN_DATE", "ann_date")),
                total_share=_as_float(_record_get(record, "TOTAL_SHARE", "total_share")),
                change_date=_as_date(_record_get(record, "CHANGE_DATE", "change_date")),
                float_share=_as_float(_record_get(record, "FLOAT_SHARE", "float_share")),
            )

    def fetch_fund_iopv(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_fund_iopv(**kwargs)
        logger.info(
            "AmazingData get_fund_iopv loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_fund_iopv",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield FundIopvRow(
                market_code=market_code,
                price_date=_as_str(_record_get(record, "PRICE_DATE", "price_date")),
                iopv_nav=_as_float(_record_get(record, "IOPV_NAV", "iopv_nav")),
            )

    def fetch_kzz_issuance(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_kzz_issuance(**kwargs)
        logger.info(
            "AmazingData get_kzz_issuance loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_kzz_issuance",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            values: dict[str, Any] = {}
            for field_name in KZZ_ISSUANCE_FIELD_NAMES:
                raw_value = _record_get(record, field_name.upper(), field_name)
                if field_name in KZZ_ISSUANCE_INT_FIELDS:
                    values[field_name] = _as_int(raw_value)
                elif field_name in KZZ_ISSUANCE_STRING_FIELDS:
                    values[field_name] = _as_str(raw_value)
                else:
                    values[field_name] = _as_float(raw_value)
            if not values.get("market_code"):
                continue
            yield KzzIssuanceRow(**values)

    def fetch_kzz_share(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_kzz_share(**kwargs)
        logger.info(
            "AmazingData get_kzz_share loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_kzz_share",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield KzzShareRow(
                change_date=_as_str(_record_get(record, "CHANGE_DATE", "change_date")),
                ann_date=_as_str(_record_get(record, "ANN_DATE", "ann_date")),
                market_code=market_code,
                bond_share=_as_float(_record_get(record, "BOND_SHARE", "bond_share")),
                conv_share=_as_float(_record_get(record, "CONV_SHARE", "conv_share")),
                change_reason=_as_str(_record_get(record, "CHANGE_REASON", "change_reason")),
            )

    def fetch_kzz_suspend(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_kzz_suspend(**kwargs)
        logger.info(
            "AmazingData get_kzz_suspend loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_kzz_suspend",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield KzzSuspendRow(
                market_code=market_code,
                suspend_date=_as_str(_record_get(record, "SUSPEND_DATE", "suspend_date")),
                suspend_type=_as_int(_record_get(record, "SUSPEND_TYPE", "suspend_type")),
                resump_date=_as_str(_record_get(record, "RESUMP_DATE", "resump_date")),
                change_reason=_as_str(_record_get(record, "CHANGE_REASON", "change_reason")),
                change_reason_code=_as_int(_record_get(record, "CHANGE_REASON_CODE", "change_reason_code")),
                resump_time=_as_str(_record_get(record, "RESUMP_TIME", "resump_time")),
            )

    def fetch_option_basic_info(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        self._clear_cache_file_once("infodata/option_basic_info/option_basic_info.h5")
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self._call_info_with_cache_recovery(
            method_name="get_option_basic_info",
            kwargs=kwargs,
            cache_relative_path="infodata/option_basic_info/option_basic_info.h5",
        )
        logger.info(
            "AmazingData get_option_basic_info loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_option_basic_info",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield OptionBasicInfoRow(
                contract_full_name=_as_str(_record_get(record, "CONTRACT_FULL_NAME", "contract_full_name")),
                contract_type=_as_str(_record_get(record, "CONTRACT_TYPE", "contract_type")),
                delivery_month=_as_str(_record_get(record, "DELIVERY_MONTH", "delivery_month")),
                expiry_date=_as_str(_record_get(record, "EXPIRY_DATE", "expiry_date")),
                exercise_price=_as_float(_record_get(record, "EXERCISE_PRICE", "exercise_price")),
                exercise_end_date=_as_str(_record_get(record, "EXERCISE_END_DATE", "exercise_end_date")),
                start_trade_date=_as_str(_record_get(record, "START_TRADE_DATE", "start_trade_date")),
                listing_ref_price=_as_float(_record_get(record, "LISTING_REF_PRICE", "listing_ref_price")),
                last_trade_date=_as_str(_record_get(record, "LAST_TRADE_DATE", "last_trade_date")),
                exchange_code=_as_str(_record_get(record, "EXCHANGE_CODE", "exchange_code")),
                delivery_date=_as_str(_record_get(record, "DELIVERY_DATE", "delivery_date")),
                contract_unit=_as_int(_record_get(record, "CONTRACT_UNIT", "contract_unit")),
                is_trade=_as_str(_record_get(record, "IS_TRADE", "is_trade")),
                exchange_short_name=_as_str(_record_get(record, "EXCHANGE_SHORT_NAME", "exchange_short_name")),
                contract_adjust_flag=_as_str(_record_get(record, "CONTRACT_ADJUST_FLAG", "contract_adjust_flag")),
                market_code=market_code,
            )

    def fetch_option_std_ctr_specs(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self._call_info_with_cache_recovery(
            method_name="get_option_std_ctr_specs",
            kwargs=kwargs,
            cache_relative_path="infodata/option_std_ctr_specs/option_std_ctr_specs.h5",
        )
        logger.info(
            "AmazingData get_option_std_ctr_specs loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_option_std_ctr_specs",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield OptionStdCtrSpecsRow(
                exercise_date=_as_str(_record_get(record, "EXERCISE_DATE", "exercise_date")),
                contract_unit=_as_int(_record_get(record, "CONTRACT_UNIT", "contract_unit")),
                position_declare_min=_as_str(_record_get(record, "POSITION_DECLARE_MIN", "position_declare_min")),
                quote_currency_unit=_as_str(_record_get(record, "QUOTE_CURRENCY_UNIT", "quote_currency_unit")),
                last_trading_date=_as_str(_record_get(record, "LAST_TRADING_DATE", "last_trading_date")),
                position_limit=_as_str(_record_get(record, "POSITION_LIMIT", "position_limit")),
                delist_date=_as_str(_record_get(record, "DELIST_DATE", "delist_date")),
                notional_value=_as_str(_record_get(record, "NOTIONAL_VALUE", "notional_value")),
                exercise_method=_as_str(_record_get(record, "EXERCISE_METHOD", "exercise_method")),
                delivery_method=_as_str(_record_get(record, "DELIVERY_METHOD", "delivery_method")),
                settlement_month=_as_str(_record_get(record, "SETTLEMENT_MONTH", "settlement_month")),
                trading_fee=_as_str(_record_get(record, "TRADING_FEE", "trading_fee")),
                exchange_name=_as_str(_record_get(record, "EXCHANGE_NAME", "exchange_name")),
                option_en_name=_as_str(_record_get(record, "OPTION_EN_NAME", "option_en_name")),
                contract_value=_as_float(_record_get(record, "CONTRACT_VALUE", "contract_value")),
                is_simulation=_as_int(_record_get(record, "IS_SIMULATION", "is_simulation")),
                contract_unit_dimension=_as_str(_record_get(record, "CONTRACT_UNIT_DIMENSION", "contract_unit_dimension")),
                option_strike_price=_as_str(_record_get(record, "OPTION_STRIKE_PRICE", "option_strike_price")),
                is_simulation_trade=_as_str(_record_get(record, "IS_SIMULATION_TRADE", "is_simulation_trade")),
                listed_date=_as_str(_record_get(record, "LISTED_DATE", "listed_date")),
                option_name=_as_str(_record_get(record, "OPTION_NAME", "option_name")),
                premium=_as_str(_record_get(record, "PREMIUM", "premium")),
                option_type=_as_str(_record_get(record, "OPTION_TYPE", "option_type")),
                trading_hours_desc=_as_str(_record_get(record, "TRADING_HOURS_DESC", "trading_hours_desc")),
                final_settlement_date=_as_str(_record_get(record, "FINAL_SETTLEMENT_DATE", "final_settlement_date")),
                final_settlement_price=_as_str(_record_get(record, "FINAL_SETTLEMENT_PRICE", "final_settlement_price")),
                min_price_unit=_as_str(_record_get(record, "MIN_PRICE_UNIT", "min_price_unit")),
                market_code=market_code,
                contract_multiplier=_as_int(_record_get(record, "CONTRACT_MULTIPLIER", "contract_multiplier")),
            )

    def fetch_option_mon_ctr_specs(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self._call_info_with_cache_recovery(
            method_name="get_option_mon_ctr_specs",
            kwargs=kwargs,
            cache_relative_path="infodata/option_mon_ctr_specs/option_mon_ctr_specs.h5",
        )
        logger.info(
            "AmazingData get_option_mon_ctr_specs loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_option_mon_ctr_specs",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield OptionMonCtrSpecsRow(
                code_old=_as_str(_record_get(record, "CODE_OLD", "code_old")),
                change_date=_as_str(_record_get(record, "CHANGE_DATE", "change_date")),
                market_code=market_code,
                name_new=_as_str(_record_get(record, "NAME_NEW", "name_new")),
                exercise_price_new=_as_float(_record_get(record, "EXERCISE_PRICE_NEW", "exercise_price_new")),
                name_old=_as_str(_record_get(record, "NAME_OLD", "name_old")),
                code_new=_as_str(_record_get(record, "CODE_NEW", "code_new")),
                exercise_price_old=_as_float(_record_get(record, "EXERCISE_PRICE_OLD", "exercise_price_old")),
                unit_old=_as_float(_record_get(record, "UNIT_OLD", "unit_old")),
                unit_new=_as_float(_record_get(record, "UNIT_NEW", "unit_new")),
                change_reason=_as_str(_record_get(record, "CHANGE_REASON", "change_reason")),
            )

    def fetch_treasury_yield(self, code_list, start_date=None, end_date=None):
        normalized_terms = [str(code).strip() for code in code_list if str(code).strip()]
        if not normalized_terms:
            return
        kwargs: dict[str, Any] = {
            "term_list": normalized_terms,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        if start_date is not None:
            kwargs["begin_date"] = to_yyyymmdd(start_date)
        if end_date is not None:
            kwargs["end_date"] = to_yyyymmdd(end_date)
        try:
            result = self.session.info.get_treasury_yield(**kwargs)
        except TypeError as exc:
            if "code_list" not in str(exc):
                raise
            retry_kwargs = dict(kwargs)
            retry_kwargs.pop("term_list", None)
            retry_kwargs["code_list"] = normalized_terms
            result = self.session.info.get_treasury_yield(**retry_kwargs)
        logger.info(
            "AmazingData get_treasury_yield loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法处理 SDK 返回的 DataFrame。")
        if not isinstance(result, dict):
            raise TypeError(f"get_treasury_yield 期望返回 dict，实际得到 {type(result).__name__}")
        for term, frame in result.items():
            term_text = _as_str(term)
            if not term_text:
                continue
            normalized_frame = _ensure_dataframe(frame, f"get_treasury_yield({term_text})")
            for trade_date, row in normalized_frame.iterrows():
                current_date = to_ch_date(trade_date)
                if start_date is not None and current_date < start_date:
                    continue
                if end_date is not None and current_date > end_date:
                    continue
                yield TreasuryYieldRow(
                    term=term_text,
                    trade_date=current_date,
                    yield_value=_as_float(_series_get(row, "YIELD", "yield")),
                )

    def fetch_kzz_conv_change(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_kzz_conv_change(**kwargs)
        logger.info(
            "AmazingData get_kzz_conv_change loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_kzz_conv_change",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield KzzConvChangeRow(
                market_code=market_code,
                change_date=_as_date(_record_get(record, "CHANGE_DATE", "change_date")),
                ann_date=_as_date(_record_get(record, "ANN_DATE", "ann_date")),
                conv_price=_as_float(_record_get(record, "CONV_PRICE", "conv_price")),
                change_reason=_as_str(_record_get(record, "CHANGE_REASON", "change_reason")),
            )

    def fetch_kzz_corr(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_kzz_corr(**kwargs)
        logger.info(
            "AmazingData get_kzz_corr loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_kzz_corr",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield KzzCorrRow(
                market_code=market_code,
                start_date=_as_date(_record_get(record, "START_DATE", "start_date")),
                end_date=_as_date(_record_get(record, "END_DATE", "end_date")),
                corr_trig_calc_max_period=_as_float(
                    _record_get(record, "CORR_TRIG_CALC_MAX_PERIOD", "corr_trig_calc_max_period")
                ),
                corr_trig_calc_period=_as_float(
                    _record_get(record, "CORR_TRIG_CALC_PERIOD", "corr_trig_calc_period")
                ),
                spec_corr_trig_ratio=_as_float(
                    _record_get(record, "SPEC_CORR_TRIG_RATIO", "spec_corr_trig_ratio")
                ),
                corr_conv_price_floor_desc=_as_str(
                    _record_get(record, "CORR_CONV_PRICE_FLOOR_DESC", "corr_conv_price_floor_desc")
                ),
                ref_price_is_avg_price=_as_int(
                    _record_get(record, "REF_PRICE_IS_AVG_PRICE", "ref_price_is_avg_price")
                ),
                corr_times_limit=_as_str(_record_get(record, "CORR_TIMES_LIMIT", "corr_times_limit")),
                is_timepoint_corr_clause_flag=_as_int(
                    _record_get(record, "IS_TIMEPOINT_CORR_CLAUSE_FLAG", "is_timepoint_corr_clause_flag")
                ),
                timepoint_count=_as_float(_record_get(record, "TIMEPOINT_COUNT", "timepoint_count")),
                timepoint_corr_text_clause=_as_str(
                    _record_get(record, "TIMEPOINT_CORR_TEXT_CLAUSE", "timepoint_corr_text_clause")
                ),
                spec_corr_range=_as_float(_record_get(record, "SPEC_CORR_RANGE", "spec_corr_range")),
                is_spec_down_corr_clause_flag=_as_int(
                    _record_get(record, "IS_SPEC_DOWN_CORR_CLAUSE_FLAG", "is_spec_down_corr_clause_flag")
                ),
            )

    def fetch_kzz_call_explanation(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_kzz_call_explanation(**kwargs)
        logger.info(
            "AmazingData get_kzz_call_explanation loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_kzz_call_explanation",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield KzzCallExplanationRow(
                market_code=market_code,
                call_date=_as_date(_record_get(record, "CALL_DATE", "call_date")),
                call_price=_as_float(_record_get(record, "CALL_PRICE", "call_price")),
                call_announcement_date=_as_date(
                    _record_get(record, "CALL_ANNOUNCEMENT_DATE", "call_announcement_date")
                ),
                call_ful_res_ann_date=_as_date(
                    _record_get(record, "CALL_FUL_RES_ANN_DATE", "call_ful_res_ann_date")
                ),
                call_amount=_as_float(_record_get(record, "CALL_AMOUNT", "call_amount")),
                call_outstanding_amount=_as_float(
                    _record_get(record, "CALL_OUTSTANDING_AMOUNT", "call_outstanding_amount")
                ),
                call_date_pub=_as_date(_record_get(record, "CALL_DATE_PUB", "call_date_pub")),
                call_fund_arrival_date=_as_date(
                    _record_get(record, "CALL_FUND_ARRIVAL_DATE", "call_fund_arrival_date")
                ),
                call_record_day=_as_date(_record_get(record, "CALL_RECORD_DAY", "call_record_day")),
                call_reason=_as_str(_record_get(record, "CALL_REASON", "call_reason")),
            )

    def fetch_kzz_put_explanation(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_kzz_put_explanation(**kwargs)
        logger.info(
            "AmazingData get_kzz_put_explanation loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_kzz_put_explanation",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield KzzPutExplanationRow(
                market_code=market_code,
                put_fund_arrival_date=_as_str(_record_get(record, "PUT_FUND_ARRIVAL_DATE", "put_fund_arrival_date")),
                put_price=_as_float(_record_get(record, "PUT_PRICE", "put_price")),
                put_announcement_date=_as_str(_record_get(record, "PUT_ANNOUNCEMENT_DATE", "put_announcement_date")),
                put_ex_date=_as_str(_record_get(record, "PUT_EX_DATE", "put_ex_date")),
                put_amount=_as_float(_record_get(record, "PUT_AMOUNT", "put_amount")),
                put_outstanding=_as_float(_record_get(record, "PUT_OUTSTANDING", "put_outstanding")),
                repurchase_start_date=_as_str(_record_get(record, "REPURCHASE_START_DATE", "repurchase_start_date")),
                repurchase_end_date=_as_str(_record_get(record, "REPURCHASE_END_DATE", "repurchase_end_date")),
                resale_start_date=_as_str(_record_get(record, "RESALE_START_DATE", "resale_start_date")),
                fund_end_date=_as_str(_record_get(record, "FUND_END_DATE", "fund_end_date")),
                repurchase_code=_as_str(_record_get(record, "REPURCHASE_CODE", "repurchase_code")),
                resale_amount=_as_float(_record_get(record, "RESALE_AMOUNT", "resale_amount")),
                resale_imp_amount=_as_float(_record_get(record, "RESALE_IMP_AMOUNT", "resale_imp_amount")),
                resale_end_date=_as_str(_record_get(record, "RESALE_END_DATE", "resale_end_date")),
            )

    def fetch_kzz_put_call_item(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_kzz_put_call_item(**kwargs)
        logger.info(
            "AmazingData get_kzz_put_call_item loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_kzz_put_call_item",
            injected_code_fields=("MARKET_CODE", "MARKETCODE", "market_code", "marketcode", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "MARKETCODE", "market_code", "marketcode", "CODE", "code"))
            if not market_code:
                continue
            yield KzzPutCallItemRow(
                market_code=market_code,
                mand_put_period=_as_str(_record_get(record, "MANDPUTPERIOD", "mand_put_period")),
                mand_put_price=_as_float(_record_get(record, "MANDPUTPRICE", "mand_put_price")),
                mand_put_start_date=_as_date(_record_get(record, "MANDPUTSTARTDATE", "mand_put_start_date")),
                mand_put_end_date=_as_date(_record_get(record, "MANDPUTENDDATE", "mand_put_end_date")),
                mand_put_text=_as_str(_record_get(record, "MANDPUTTEXT", "mand_put_text")),
                is_mand_put_contain_current=_as_int(
                    _record_get(record, "ISMAND_PUTCONTAIN_CURRENT", "is_mand_put_contain_current")
                ),
                con_put_start_date=_as_date(_record_get(record, "CONPUTSTARTDATE", "con_put_start_date")),
                con_put_end_date=_as_date(_record_get(record, "CONPUTENDDATE", "con_put_end_date")),
                max_put_triper=_as_float(_record_get(record, "MAXPUTTRIPER", "max_put_triper")),
                put_triperiod=_as_float(_record_get(record, "PUT_TRIPERIOD", "put_triperiod")),
                add_put_con=_as_str(_record_get(record, "ADDPUTCON", "add_put_con")),
                add_put_price_ins=_as_str(_record_get(record, "ADD_PUT_PRICE_INS", "add_put_price_ins")),
                put_num_ins=_as_str(_record_get(record, "PUT_NUM_INS", "put_num_ins")),
                put_pro_period=_as_float(_record_get(record, "PUT_PRO_PERIOD", "put_pro_period")),
                put_no_pery=_as_float(_record_get(record, "PUT_NO_PERY", "put_no_pery")),
                is_put_item=_as_int(_record_get(record, "IS_PUT_ITEM", "is_put_item")),
                is_term_put_item=_as_int(_record_get(record, "IS_TERM_PUT_ITEM", "is_term_put_item")),
                is_mand_put_item=_as_int(_record_get(record, "IS_MAND_PUT_ITEM", "is_mand_put_item")),
                is_time_put_item=_as_int(_record_get(record, "ISTIMEPUTITEM", "is_time_put_item")),
                time_put_no=_as_float(_record_get(record, "TIME_PUT_NO", "time_put_no")),
                time_put_item=_as_str(_record_get(record, "TIMEPUTITEM", "time_put_item")),
                term_put_price=_as_float(_record_get(record, "TERMPUTPRICE", "term_put_price")),
                con_call_start_date=_as_date(_record_get(record, "CONCALLSTARTDATE", "con_call_start_date")),
                con_call_end_date=_as_date(_record_get(record, "CONCALLENDDATE", "con_call_end_date")),
                call_tri_con_ins=_as_str(_record_get(record, "CALLTRICONINS", "call_tri_con_ins")),
                max_call_triper=_as_float(_record_get(record, "MAXCALLTRIPER", "max_call_triper")),
                call_tri_per=_as_float(_record_get(record, "CALL_TRI_PER", "call_tri_per")),
                call_num_ber_ins=_as_str(_record_get(record, "CALL_NUM_BER_INS", "call_num_ber_ins")),
                is_call_item=_as_int(_record_get(record, "ISCALLITEM", "is_call_item")),
                call_pro_period=_as_float(_record_get(record, "CALLPROPERIOD", "call_pro_period")),
                call_no_pery=_as_float(_record_get(record, "CALLNOPERY", "call_no_pery")),
                is_time_call_item=_as_int(_record_get(record, "IS_TIME_CALL_ITEM", "is_time_call_item")),
                time_call_no=_as_float(_record_get(record, "TIMECALLNO", "time_call_no")),
                time_call_text=_as_str(_record_get(record, "TIMECALLTEXT", "time_call_text")),
                expired_redemption_price=_as_float(
                    _record_get(record, "EXPIREDREDEMPTIONPRICE", "expired_redemption_price")
                ),
                put_tri_cond_desc=_as_str(_record_get(record, "PUTTRICONDESC", "put_tri_cond_desc")),
            )

    def fetch_kzz_put(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_kzz_put(**kwargs)
        logger.info(
            "AmazingData get_kzz_put loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_kzz_put",
            injected_code_fields=("MARKETCODE", "marketcode", "MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKETCODE", "marketcode", "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield KzzPutRow(
                market_code=market_code,
                put_price=_as_float(_record_get(record, "PUTPRICE", "put_price")),
                begin_date=_as_date(_record_get(record, "BEGIN_DATE", "begin_date")),
                end_date=_as_date(_record_get(record, "END_DATE", "end_date")),
                tri_ratio=_as_float(_record_get(record, "TRI_RATIO", "tri_ratio")),
            )

    def fetch_kzz_call(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_kzz_call(**kwargs)
        logger.info(
            "AmazingData get_kzz_call loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_kzz_call",
            injected_code_fields=("MARKETCODE", "marketcode", "MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKETCODE", "marketcode", "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield KzzCallRow(
                market_code=market_code,
                call_price=_as_float(_record_get(record, "CALLPRICE", "call_price")),
                begin_date=_as_date(_record_get(record, "BEGIN_DATE", "begin_date")),
                end_date=_as_date(_record_get(record, "END_DATE", "end_date")),
                tri_ratio=_as_float(_record_get(record, "TRI_RATIO", "tri_ratio")),
            )

    def fetch_kzz_conv(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_kzz_conv(**kwargs)
        logger.info(
            "AmazingData get_kzz_conv loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_kzz_conv",
            injected_code_fields=("MARKET_CODE", "MARKETCODE", "market_code", "marketcode", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "MARKETCODE", "market_code", "marketcode", "CODE", "code"))
            if not market_code:
                continue
            yield KzzConvRow(
                market_code=market_code,
                ann_date=_as_date(_record_get(record, "ANN_DATE", "ann_date")),
                conv_code=_as_str(_record_get(record, "CONV_CODE", "conv_code")),
                conv_name=_as_str(_record_get(record, "CONVNAME", "conv_name")),
                conv_price=_as_float(_record_get(record, "CONVPRICE", "conv_price")),
                currency_code=_as_str(_record_get(record, "CURRENCYCODE", "currency_code")),
                conv_start_date=_as_date(_record_get(record, "CONVSTARTDATE", "conv_start_date")),
                conv_end_date=_as_date(_record_get(record, "CONVENDDATE", "conv_end_date")),
                trade_date_last=_as_date(_record_get(record, "TRADEDATELAST", "trade_date_last")),
                forced_conv_date=_as_date(_record_get(record, "FORCEDCONVDATE", "forced_conv_date")),
                forced_conv_price=_as_float(_record_get(record, "FORCEDCONVPRICE", "forced_conv_price")),
                rel_conv_month=_as_float(_record_get(record, "RELCONVMONTH", "rel_conv_month")),
                is_forced=_as_float(_record_get(record, "ISFORCED", "is_forced")),
                forced_conv_reason=_as_str(_record_get(record, "FORCED_CONVREASON", "forced_conv_reason")),
            )

    def fetch_block_trading(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_block_trading(**kwargs)
        logger.info(
            "AmazingData get_block_trading loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(result, action="get_block_trading"):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "MARKETCODE", "market_code", "marketcode"))
            trade_date = _as_date(_record_get(record, "TRADE_DATE", "TRADEDATE", "trade_date"))
            if not market_code or trade_date is None:
                continue
            yield BlockTradingRow(
                market_code=market_code,
                trade_date=trade_date,
                bshare_price=_as_float(_record_get(record, "BSHAREPRICE", "bshare_price")),
                bshare_volume=_as_float(_record_get(record, "BSHAREVOLUME", "bshare_volume")),
                b_frequency=_as_int(_record_get(record, "B_FREQUENCY", "b_frequency")),
                block_avg_volume=_as_float(_record_get(record, "BLOCKAVGVOLUME", "block_avg_volume")),
                bshare_amount=_as_float(_record_get(record, "BSHAREAMOUNT", "bshare_amount")),
                bbuyer_name=_as_str(_record_get(record, "BBUYERNAME", "bbuyer_name")),
                bseller_name=_as_str(_record_get(record, "BSELLERNAME", "bseller_name")),
            )

    def fetch_long_hu_bang(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_long_hu_bang(**kwargs)
        logger.info(
            "AmazingData get_long_hu_bang loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(result, action="get_long_hu_bang"):
            market_code = _as_str(_record_get(record, "MARKETCODE", "MARKET_CODE", "market_code", "marketcode"))
            trade_date = _as_date(_record_get(record, "TRADEDATE", "TRADE_DATE", "trade_date"))
            if not market_code or trade_date is None:
                continue
            yield LongHuBangRow(
                market_code=market_code,
                trade_date=trade_date,
                security_name=_as_str(_record_get(record, "SECURITYNAME", "security_name")),
                reason_type=_as_str(_record_get(record, "REASONTYPE", "reason_type")),
                reason_type_name=_as_str(_record_get(record, "REASON_TYPE_NAME", "reason_type_name")),
                change_range=_as_float(_record_get(record, "CHANGE_RANGE", "change_range")),
                trader_name=_as_str(_record_get(record, "TRADER_NAME", "trader_name")),
                buy_amount=_as_float(_record_get(record, "BUY_AMOUNT", "buy_amount")),
                sell_amount=_as_float(_record_get(record, "SELL_AMOUNT", "sell_amount")),
                flow_mark=_as_int(_record_get(record, "FLOW_MARK", "flow_mark")),
                total_amount=_as_float(_record_get(record, "TOTAL_AMOUNT", "total_amount")),
                total_volume=_as_float(_record_get(record, "TOTAL_VOLUME", "total_volume")),
            )

    def fetch_margin_detail(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_margin_detail(**kwargs)
        logger.info(
            "AmazingData get_margin_detail loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_margin_detail",
            injected_code_fields=("MARKETCODE", "marketcode", "MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKETCODE", "marketcode", "MARKET_CODE", "market_code", "CODE", "code"))
            trade_date = _as_date(_record_get(record, "TRADEDATE", "TRADE_DATE", "trade_date"))
            if not market_code or trade_date is None:
                continue
            yield MarginDetailRow(
                market_code=market_code,
                security_name=_as_str(_record_get(record, "SECURITYNAME", "security_name")),
                trade_date=trade_date,
                borrow_money_balance=_as_float(_record_get(record, "BORROWMONEYBALANCE", "borrow_money_balance")),
                purch_with_borrow_money=_as_float(_record_get(record, "PURCHWITHBORROWMONEY", "purch_with_borrow_money")),
                repayment_of_borrow_money=_as_float(
                    _record_get(record, "REPAYMENT_OF_BORROW_MONEY", "repayment_of_borrow_money")
                ),
                sec_lending_balance=_as_float(_record_get(record, "SEC_LENDING_BALANCE", "sec_lending_balance")),
                sales_of_borrowed_sec=_as_int(_record_get(record, "SALES_OFBORROWED_SEC", "sales_of_borrowed_sec")),
                repayment_of_borrow_sec=_as_int(
                    _record_get(record, "REPAYMENTOFBORROWSEC", "repayment_of_borrow_sec")
                ),
                sec_lending_balance_vol=_as_int(
                    _record_get(record, "SEC_LENDING_BALANCE_VOL", "sec_lending_balance_vol")
                ),
                margin_trade_balance=_as_float(_record_get(record, "MARGINTRADEBALANCE", "margin_trade_balance")),
            )

    def fetch_margin_summary(self, code_list=None, start_date=None, end_date=None):
        kwargs: dict[str, Any] = {
            "local_path": self.config.local_path,
            "is_local": False,
        }
        if start_date is not None:
            kwargs["begin_date"] = to_yyyymmdd(start_date)
        if end_date is not None:
            kwargs["end_date"] = to_yyyymmdd(end_date)
        try:
            result = self.session.info.get_margin_summary(**kwargs)
        except TypeError as exc:
            if "'NoneType' object is not subscriptable" not in str(exc):
                raise
            logger.warning(
                "AmazingData get_margin_summary returned empty/invalid payload begin_date=%s end_date=%s error=%s",
                kwargs.get("begin_date"),
                kwargs.get("end_date"),
                exc,
            )
            return
        logger.info(
            "AmazingData get_margin_summary loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(result, action="get_margin_summary", index_field="TRADE_DATE"):
            trade_date = _as_date(_record_get(record, "TRADE_DATE", "trade_date"))
            if trade_date is None:
                continue
            yield MarginSummaryRow(
                trade_date=trade_date,
                sum_borrow_money_balance=_as_float(
                    _record_get(record, "SUM_BORROW_MONEY_BALANCE", "sum_borrow_money_balance")
                ),
                sum_purch_with_borrow_money=_as_float(
                    _record_get(record, "SUM_PURCH_WITH_BORROW_MONEY", "sum_purch_with_borrow_money")
                ),
                sum_repayment_of_borrow_money=_as_float(
                    _record_get(record, "SUM_REPAYMENT_OF_BORROW_MONEY", "sum_repayment_of_borrow_money")
                ),
                sum_sec_lending_balance=_as_float(
                    _record_get(record, "SUM_SEC_LENDING_BALANCE", "sum_sec_lending_balance")
                ),
                sum_sales_of_borrowed_sec=_as_int(
                    _record_get(record, "SUM_SALES_OF_BORROWED_SEC", "sum_sales_of_borrowed_sec")
                ),
                sum_margin_trade_balance=_as_float(
                    _record_get(record, "SUM_MARGIN_TRADE_BALANCE", "sum_margin_trade_balance")
                ),
            )

    def fetch_share_holder(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_share_holder(**kwargs)
        logger.info("AmazingData get_share_holder loaded result_type=%s rows=%s", type(result).__name__, _count_sdk_result_rows(result))
        for record in _iter_records_from_sdk_result(
            result,
            action="get_share_holder",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield ShareHolderRow(
                ann_date=_as_date(_record_get(record, "ANN_DATE", "ann_date")),
                market_code=market_code,
                holder_enddate=_as_date(_record_get(record, "HOLDER_ENDDATE", "holder_enddate")),
                holder_type=_as_int(_record_get(record, "HOLDER_TYPE", "holder_type")),
                qty_num=_as_int(_record_get(record, "QTY_NUM", "qty_num")),
                holder_name=_as_str(_record_get(record, "HOLDER_NAME", "holder_name")),
                holder_holder_category=_as_int(
                    _record_get(record, "HOLDER_HOLDER_CATEGORY", "holder_holder_category")
                ),
                holder_quantity=_as_float(_record_get(record, "HOLDER_QUANTITY", "holder_quantity")),
                holder_pct=_as_float(_record_get(record, "HOLDER_PCT", "holder_pct")),
                holder_sharecategoryname=_as_str(
                    _record_get(record, "HOLDER_SHARECATEGORYNAME", "holder_sharecategoryname")
                ),
                float_qty=_as_float(_record_get(record, "FLOAT_QTY", "float_qty")),
            )

    def fetch_holder_num(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_holder_num(**kwargs)
        logger.info("AmazingData get_holder_num loaded result_type=%s rows=%s", type(result).__name__, _count_sdk_result_rows(result))
        for record in _iter_records_from_sdk_result(
            result,
            action="get_holder_num",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield HolderNumRow(
                market_code=market_code,
                ann_dt=_as_date(_record_get(record, "ANN_DT", "ann_dt", "ANN_DATE", "ann_date")),
                holder_enddate=_as_date(_record_get(record, "HOLDER_ENDDATE", "holder_enddate")),
                holder_total_num=_as_float(_record_get(record, "HOLDER_TOTAL_NUM", "holder_total_num")),
                holder_num=_as_float(_record_get(record, "HOLDER_NUM", "holder_num")),
            )

    def fetch_equity_structure(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_equity_structure(**kwargs)
        logger.info(
            "AmazingData get_equity_structure loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_equity_structure",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield EquityStructureRow(
                market_code=market_code,
                ann_date=_as_date(_record_get(record, "ANN_DATE", "ann_date")),
                change_date=_as_date(_record_get(record, "CHANGE_DATE", "change_date")),
                share_change_reason_str=_as_str(
                    _record_get(record, "SHARE_CHANGE_REASON_STR", "share_change_reason_str")
                ),
                ex_change_date=_as_date(_record_get(record, "EX_CHANGE_DATE", "ex_change_date")),
                current_sign=_as_int(_record_get(record, "CURRENT_SIGN", "current_sign")),
                is_valid=_as_int(_record_get(record, "IS_VALID", "is_valid")),
                tot_share=_as_float(_record_get(record, "TOT_SHARE", "tot_share")),
                float_share=_as_float(_record_get(record, "FLOAT_SHARE", "float_share")),
                float_a_share=_as_float(_record_get(record, "FLOAT_A_SHARE", "float_a_share")),
                float_b_share=_as_float(_record_get(record, "FLOAT_B_SHARE", "float_b_share")),
                float_hk_share=_as_float(_record_get(record, "FLOAT_HK_SHARE", "float_hk_share")),
                float_os_share=_as_float(_record_get(record, "FLOAT_OS_SHARE", "float_os_share")),
                tot_tradable_share=_as_float(_record_get(record, "TOT_TRADABLE_SHARE", "tot_tradable_share")),
                rtd_a_share_inst=_as_float(_record_get(record, "RTD_A_SHARE_INST", "rtd_a_share_inst")),
                rtd_a_share_domesnp=_as_float(_record_get(record, "RTD_A_SHARE_DOMESNP", "rtd_a_share_domesnp")),
                rtd_share_senior=_as_float(_record_get(record, "RTD_SHARE_SENIOR", "rtd_share_senior")),
                rtd_a_share_foreign=_as_float(_record_get(record, "RTD_A_SHARE_FOREIGN", "rtd_a_share_foreign")),
                rtd_a_share_forjur=_as_float(_record_get(record, "RTD_A_SHARE_FORJUR", "rtd_a_share_forjur")),
                rtd_a_share_fornp=_as_float(_record_get(record, "RTD_A_SHARE_FORNP", "rtd_a_share_fornp")),
                restricted_b_share=_as_float(_record_get(record, "RESTRICTED_B_SHARE", "restricted_b_share")),
                other_rtd_share=_as_float(_record_get(record, "OTHER_RTD_SHARE", "other_rtd_share")),
                non_tradable_share=_as_float(_record_get(record, "NON_TRADABLE_SHARE", "non_tradable_share")),
                ntrd_share_state_pct=_as_float(_record_get(record, "NTRD_SHARE_STATE_PCT", "ntrd_share_state_pct")),
                ntrd_share_state=_as_float(_record_get(record, "NTRD_SHARE_STATE", "ntrd_share_state")),
                ntrd_share_statejur=_as_float(_record_get(record, "NTRD_SHARE_STATEJUR", "ntrd_share_statejur")),
                ntrd_share_domesjur=_as_float(_record_get(record, "NTRD_SHARE_DOMESJUR", "ntrd_share_domesjur")),
                ntrd_share_domes_initiator=_as_float(
                    _record_get(record, "NTRD_SHARE_DOMES_INITIATOR", "ntrd_share_domes_initiator")
                ),
                ntrd_share_ipojuris=_as_float(_record_get(record, "NTRD_SHARE_IPOJURIS", "ntrd_share_ipojuris")),
                ntrd_share_genjuris=_as_float(_record_get(record, "NTRD_SHARE_GENJURIS", "ntrd_share_genjuris")),
                ntrd_share_stra_investor=_as_float(
                    _record_get(record, "NTRD_SHARE_STRA_INVESTOR", "ntrd_share_stra_investor")
                ),
                ntrd_share_fund=_as_float(_record_get(record, "NTRD_SHARE_FUND", "ntrd_share_fund")),
                ntrd_share_nat=_as_float(_record_get(record, "NTRD_SHARE_NAT", "ntrd_share_nat")),
                tran_share=_as_float(_record_get(record, "TRAN_SHARE", "tran_share")),
                float_share_senior=_as_float(_record_get(record, "FLOAT_SHARE_SENIOR", "float_share_senior")),
                share_inemp=_as_float(_record_get(record, "SHARE_INEMP", "share_inemp")),
                preferred_share=_as_float(_record_get(record, "PREFERRED_SHARE", "preferred_share")),
                ntrd_share_nlist_frgn=_as_float(
                    _record_get(record, "NTRD_SHARE_NLIST_FRGN", "ntrd_share_nlist_frgn")
                ),
                staq_share=_as_float(_record_get(record, "STAQ_SHARE", "staq_share")),
                net_share=_as_float(_record_get(record, "NET_SHARE", "net_share")),
                share_change_reason=_as_str(_record_get(record, "SHARE_CHANGE_REASON", "share_change_reason")),
                tot_a_share=_as_float(_record_get(record, "TOT_A_SHARE", "tot_a_share")),
                tot_b_share=_as_float(_record_get(record, "TOT_B_SHARE", "tot_b_share")),
                otca_share=_as_float(_record_get(record, "OTCA_SHARE", "otca_share")),
                otcb_share=_as_float(_record_get(record, "OTCB_SHARE", "otcb_share")),
                tot_otc_share=_as_float(_record_get(record, "TOT_OTC_SHARE", "tot_otc_share")),
                share_hk=_as_float(_record_get(record, "SHARE_HK", "share_hk")),
                pre_non_tradable_share=_as_float(
                    _record_get(record, "PRE_NON_TRADABLE_SHARE", "pre_non_tradable_share")
                ),
                restricted_a_share=_as_float(_record_get(record, "RESTRICTED_A_SHARE", "restricted_a_share")),
                rtd_a_share_state=_as_float(_record_get(record, "RTD_A_SHARE_STATE", "rtd_a_share_state")),
                rtd_a_share_statejur=_as_float(_record_get(record, "RTD_A_SHARE_STATEJUR", "rtd_a_share_statejur")),
                rtd_a_share_other_domes=_as_float(
                    _record_get(record, "RTD_A_SHARE_OTHER_DOMES", "rtd_a_share_other_domes")
                ),
                rtd_a_share_other_domesjur=_as_float(
                    _record_get(record, "RTD_A_SHARE_OTHER_DOMESJUR", "rtd_a_share_other_domesjur")
                ),
                tot_restricted_share=_as_float(_record_get(record, "TOT_RESTRICTED_SHARE", "tot_restricted_share")),
            )

    def fetch_equity_pledge_freeze(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_equity_pledge_freeze(**kwargs)
        logger.info(
            "AmazingData get_equity_pledge_freeze loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_equity_pledge_freeze",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield EquityPledgeFreezeRow(
                market_code=market_code,
                ann_date=_as_date(_record_get(record, "ANN_DATE", "ann_date")),
                holder_name=_as_str(_record_get(record, "HOLDER_NAME", "holder_name")),
                holder_type_code=_as_int(_record_get(record, "HOLDER_TYPE_CODE", "holder_type_code")),
                total_holding_shr=_as_float(_record_get(record, "TOTAL_HOLDING_SHR", "total_holding_shr")),
                total_holding_shr_ratio=_as_float(
                    _record_get(record, "TOTAL_HOLDING_SHR_RATIO", "total_holding_shr_ratio")
                ),
                fro_shares=_as_float(_record_get(record, "FRO_SHARES", "fro_shares")),
                fro_shr_to_total_holding_ratio=_as_float(
                    _record_get(record, "FRO_SHR_TO_TOTAL_HOLDING_RATIO", "fro_shr_to_total_holding_ratio")
                ),
                fro_shr_to_total_ratio=_as_float(
                    _record_get(record, "FRO_SHR_TO_TOTAL_RATIO", "fro_shr_to_total_ratio")
                ),
                total_pledge_shr=_as_float(_record_get(record, "TOTAL_PLEDGE_SHR", "total_pledge_shr")),
                is_equity_pledge_repo=_as_int(
                    _record_get(record, "IS_EQUITY_PLEDGE_REPO", "is_equity_pledge_repo")
                ),
                begin_date=_as_date(_record_get(record, "BEGIN_DATE", "begin_date")),
                end_date=_as_date(_record_get(record, "END_DATE", "end_date")),
                is_disfrozen=_as_int(_record_get(record, "IS_DISFROZEN", "is_disfrozen")),
                frozen_institution=_as_str(_record_get(record, "FROZEN_INSTITUTION", "frozen_institution")),
                disfrozen_time=_as_date(_record_get(record, "DISFROZEN_TIME", "disfrozen_time")),
                shr_category_code=_as_int(_record_get(record, "SHR_CATEGORY_CODE", "shr_category_code")),
                freeze_type=_as_int(_record_get(record, "FREEZE_TYPE", "freeze_type")),
            )

    def fetch_equity_restricted(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_equity_restricted(**kwargs)
        logger.info(
            "AmazingData get_equity_restricted loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_equity_restricted",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield EquityRestrictedRow(
                market_code=market_code,
                list_date=_as_date(_record_get(record, "LIST_DATE", "list_date")),
                share_ratio=_as_float(_record_get(record, "SHARE_RATIO", "share_ratio")),
                share_lst_type_name=_as_str(_record_get(record, "SHARE_LST_TYPE_NAME", "share_lst_type_name")),
                share_lst=_as_int(_record_get(record, "SHARE_LST", "share_lst")),
                share_lst_is_ann=_as_int(_record_get(record, "SHARE_LST_IS_ANN", "share_lst_is_ann")),
                close_price=_as_float(_record_get(record, "CLOSE_PRICE", "close_price")),
                share_lst_market_value=_as_float(
                    _record_get(record, "SHARE_LST_MARKET_VALUE", "share_lst_market_value")
                ),
            )

    def fetch_dividend(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_dividend(**kwargs)
        logger.info(
            "AmazingData get_dividend loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_dividend",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield DividendRow(
                market_code=market_code,
                div_progress=_as_str(_record_get(record, "DIV_PROGRESS", "div_progress")),
                dvd_per_share_stk=_as_float(_record_get(record, "DVD_PER_SHARE_STK", "dvd_per_share_stk")),
                dvd_per_share_pre_tax_cash=_as_float(
                    _record_get(record, "DVD_PER_SHARE_PRE_TAX_CASH", "dvd_per_share_pre_tax_cash")
                ),
                dvd_per_share_after_tax_cash=_as_float(
                    _record_get(record, "DVD_PER_SHARE_AFTER_TAX_CASH", "dvd_per_share_after_tax_cash")
                ),
                date_eqy_record=_as_date(_record_get(record, "DATE_EQY_RECORD", "date_eqy_record")),
                date_ex=_as_date(_record_get(record, "DATE_EX", "date_ex")),
                date_dvd_payout=_as_date(_record_get(record, "DATE_DVD_PAYOUT", "date_dvd_payout")),
                listingdate_of_dvd_shr=_as_date(
                    _record_get(record, "LISTINGDATE_OF_DVD_SHR", "listingdate_of_dvd_shr")
                ),
                div_prelandate=_as_date(_record_get(record, "DIV_PRELANDATE", "div_prelandate")),
                div_smtgdate=_as_date(_record_get(record, "DIV_SMTGDATE", "div_smtgdate")),
                date_dvd_ann=_as_date(_record_get(record, "DATE_DVD_ANN", "date_dvd_ann")),
                div_basedate=_as_date(_record_get(record, "DIV_BASEDATE", "div_basedate")),
                div_baseshare=_as_float(_record_get(record, "DIV_BASESHARE", "div_baseshare")),
                currency_code=_as_str(_record_get(record, "CURRENCY_CODE", "currency_code")),
                ann_date=_as_date(_record_get(record, "ANN_DATE", "ann_date")),
                is_changed=_as_int(_record_get(record, "IS_CHANGED", "is_changed")),
                report_period=_as_str(_record_get(record, "REPORT_PERIOD", "report_period")),
                div_change=_as_str(_record_get(record, "DIV_CHANGE", "div_change")),
                div_bonusrate=_as_float(_record_get(record, "DIV_BONUSRATE", "div_bonusrate")),
                div_conversedrate=_as_float(_record_get(record, "DIV_CONVERSEDRATE", "div_conversedrate")),
                remark=_as_str(_record_get(record, "REMARK", "remark")),
                div_preann_date=_as_date(_record_get(record, "DIV_PREANN_DATE", "div_preann_date")),
                div_target=_as_str(_record_get(record, "DIV_TARGET", "div_target")),
            )

    def fetch_right_issue(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_right_issue(**kwargs)
        logger.info(
            "AmazingData get_right_issue loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_right_issue",
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            yield RightIssueRow(
                market_code=market_code,
                progress=_as_int(_record_get(record, "PROGRESS", "progress")),
                price=_as_float(_record_get(record, "PRICE", "price")),
                ratio=_as_float(_record_get(record, "RATIO", "ratio")),
                amt_plan=_as_float(_record_get(record, "AMT_PLAN", "amt_plan")),
                amt_real=_as_float(_record_get(record, "AMT_REAL", "amt_real")),
                collection_fund=_as_float(_record_get(record, "COLLECTION_FUND", "collection_fund")),
                shareb_reg_date=_as_date(_record_get(record, "SHAREB_REG_DATE", "shareb_reg_date")),
                ex_dividend_date=_as_date(_record_get(record, "EX_DIVIDEND_DATE", "ex_dividend_date")),
                listed_date=_as_date(_record_get(record, "LISTED_DATE", "listed_date")),
                pay_start_date=_as_date(_record_get(record, "PAY_START_DATE", "pay_start_date")),
                pay_end_date=_as_date(_record_get(record, "PAY_END_DATE", "pay_end_date")),
                preplan_date=_as_date(_record_get(record, "PREPLAN_DATE", "preplan_date")),
                smtg_ann_date=_as_date(_record_get(record, "SMTG_ANN_DATE", "smtg_ann_date")),
                pass_date=_as_date(_record_get(record, "PASS_DATE", "pass_date")),
                approved_date=_as_date(_record_get(record, "APPROVED_DATE", "approved_date")),
                execute_date=_as_date(_record_get(record, "EXECUTE_DATE", "execute_date")),
                result_date=_as_date(_record_get(record, "RESULT_DATE", "result_date")),
                list_ann_date=_as_date(_record_get(record, "LIST_ANN_DATE", "list_ann_date")),
                guarantor=_as_str(_record_get(record, "GUARANTOR", "guarantor")),
                guartype=_as_float(_record_get(record, "GUARTYPE", "guartype")),
                rightsissue_code=_as_str(_record_get(record, "RIGHTSISSUE_CODE", "rightsissue_code")),
                ann_date=_as_date(_record_get(record, "ANN_DATE", "ann_date")),
                rightsissue_year=_as_str(_record_get(record, "RIGHTSISSUE_YEAR", "rightsissue_year")),
                rightsissue_desc=_as_str(_record_get(record, "RIGHTSISSUE_DESC", "rightsissue_desc")),
                rightsissue_name=_as_str(_record_get(record, "RIGHTSISSUE_NAME", "rightsissue_name")),
                ratio_denominator=_as_float(_record_get(record, "RATIO_DENOMINATOR", "ratio_denominator")),
                ratio_molecular=_as_float(_record_get(record, "RATIO_MOLECULAR", "ratio_molecular")),
                subs_method=_as_str(_record_get(record, "SUBS_METHOD", "subs_method")),
                expected_fund_raising=_as_float(
                    _record_get(record, "EXPECTED_FUND_RAISING", "expected_fund_raising")
                ),
            )

    def fetch_index_constituent(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_index_constituent(**kwargs)
        logger.info(
            "AmazingData get_index_constituent loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_index_constituent",
            injected_code_fields=("INDEX_CODE", "index_code"),
            index_field="INDATE",
        ):
            index_code = _as_str(_record_get(record, "INDEX_CODE", "index_code", "CODE", "code"))
            con_code = _as_str(_record_get(record, "CON_CODE", "con_code"))
            if not index_code or not con_code:
                continue
            yield IndexConstituentRow(
                index_code=index_code,
                con_code=con_code,
                in_date=_as_date(_record_get(record, "INDATE", "in_date", "IN_DATE")),
                out_date=_as_date(_record_get(record, "OUTDATE", "out_date", "OUT_DATE")),
                index_name=_as_str(_record_get(record, "INDEX_NAME", "index_name")),
            )

    def fetch_index_weight(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        try:
            result = self.session.info.get_index_weight(**kwargs)
        except Exception as exc:
            raise RuntimeError(
                "AmazingData get_index_weight failed "
                f"code_count={len(normalized_codes)} code_preview={','.join(normalized_codes[:5])} "
                f"errors=is_local=False: {type(exc).__name__}: {exc}"
            ) from exc

        logger.info(
            "AmazingData get_index_weight loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_index_weight",
            injected_code_fields=("INDEX_CODE", "index_code"),
            index_field="TRADE_DATE",
        ):
            index_code = _as_str(_record_get(record, "INDEX_CODE", "index_code", "CODE", "code"))
            con_code = _as_str(_record_get(record, "CON_CODE", "con_code"))
            trade_date = _as_date(_record_get(record, "TRADE_DATE", "trade_date", "DATE", "date"))
            if not index_code or not con_code or trade_date is None:
                continue
            yield IndexWeightRow(
                index_code=index_code,
                con_code=con_code,
                trade_date=trade_date,
                total_share=_as_float(_record_get(record, "TOTAL_SHARE", "total_share")),
                free_share_ratio=_as_float(_record_get(record, "FREE_SHARE_RATIO", "free_share_ratio")),
                calc_share=_as_float(_record_get(record, "CALC_SHARE", "calc_share")),
                weight_factor=_as_float(_record_get(record, "WEIGHT_FACTOR", "weight_factor")),
                weight=_as_float(_record_get(record, "WEIGHT", "weight")),
                close=_as_float(_record_get(record, "CLOSE", "close")),
            )

    def fetch_industry_base_info(self):
        kwargs: dict[str, Any] = {
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_industry_base_info(**kwargs)
        logger.info(
            "AmazingData get_industry_base_info loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_industry_base_info",
            injected_code_fields=("INDEX_CODE", "index_code"),
        ):
            index_code = _as_str(_record_get(record, "INDEX_CODE", "index_code", "CODE", "code"))
            if not index_code:
                continue
            yield IndustryBaseInfoRow(
                index_code=index_code,
                industry_code=_as_str(_record_get(record, "INDUSTRY_CODE", "industry_code")),
                level_type=_as_int(_record_get(record, "LEVEL_TYPE", "level_type")),
                level1_name=_as_str(_record_get(record, "LEVEL1_NAME", "level1_name")),
                level2_name=_as_str(_record_get(record, "LEVEL2_NAME", "level2_name")),
                level3_name=_as_str(_record_get(record, "LEVEL3_NAME", "level3_name")),
                is_pub=_as_int(_record_get(record, "IS_PUB", "is_pub")),
                change_reason=_as_str(_record_get(record, "CHANGE_REASON", "change_reason")),
            )

    def fetch_industry_constituent(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        result = self.session.info.get_industry_constituent(**kwargs)
        logger.info(
            "AmazingData get_industry_constituent loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_industry_constituent",
            injected_code_fields=("INDEX_CODE", "index_code"),
            index_field="INDATE",
        ):
            index_code = _as_str(_record_get(record, "INDEX_CODE", "index_code", "CODE", "code"))
            con_code = _as_str(_record_get(record, "CON_CODE", "con_code"))
            if not index_code or not con_code:
                continue
            yield IndustryConstituentRow(
                index_code=index_code,
                con_code=con_code,
                in_date=_as_date(_record_get(record, "INDATE", "in_date", "IN_DATE")),
                out_date=_as_date(_record_get(record, "OUTDATE", "out_date", "OUT_DATE")),
                index_name=_as_str(_record_get(record, "INDEX_NAME", "index_name")),
            )

    def fetch_industry_weight(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_industry_weight(**kwargs)
        logger.info(
            "AmazingData get_industry_weight loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_industry_weight",
            injected_code_fields=("INDEX_CODE", "index_code"),
            index_field="TRADE_DATE",
        ):
            index_code = _as_str(_record_get(record, "INDEX_CODE", "index_code", "CODE", "code"))
            con_code = _as_str(_record_get(record, "CON_CODE", "con_code"))
            trade_date = _as_date(_record_get(record, "TRADE_DATE", "trade_date"))
            if not index_code or not con_code or trade_date is None:
                continue
            yield IndustryWeightRow(
                index_code=index_code,
                con_code=con_code,
                trade_date=trade_date,
                weight=_as_float(_record_get(record, "WEIGHT", "weight")),
            )

    def fetch_industry_daily(self, code_list, start_date=None, end_date=None):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return

        kwargs = self._build_info_kwargs(normalized_codes, start_date, end_date)
        result = self.session.info.get_industry_daily(**kwargs)
        logger.info(
            "AmazingData get_industry_daily loaded result_type=%s rows=%s",
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action="get_industry_daily",
            injected_code_fields=("INDEX_CODE", "index_code"),
            index_field="TRADE_DATE",
        ):
            index_code = _as_str(_record_get(record, "INDEX_CODE", "index_code", "CODE", "code"))
            trade_date = _as_date(_record_get(record, "TRADE_DATE", "trade_date"))
            if not index_code or trade_date is None:
                continue
            yield IndustryDailyRow(
                index_code=index_code,
                trade_date=trade_date,
                open=_as_float(_record_get(record, "OPEN", "open")),
                high=_as_float(_record_get(record, "HIGH", "high")),
                close=_as_float(_record_get(record, "CLOSE", "close")),
                low=_as_float(_record_get(record, "LOW", "low")),
                amount=_as_float(_record_get(record, "AMOUNT", "amount")),
                volume=_as_float(_record_get(record, "VOLUME", "volume")),
                pb=_as_float(_record_get(record, "PB", "pb")),
                pe=_as_float(_record_get(record, "PE", "pe")),
                total_cap=_as_float(_record_get(record, "TOTAL_CAP", "total_cap")),
                a_float_cap=_as_float(_record_get(record, "A_FLOAT_CAP", "a_float_cap")),
                pre_close=_as_float(_record_get(record, "PRE_CLOSE", "pre_close", "PRECLOSE")),
            )

    def _fetch_info_payload_records(
        self,
        method_name: str,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ):
        normalized_codes = normalize_code_list(code_list)
        if not normalized_codes:
            return
        method = getattr(self.session.info, method_name)
        kwargs: dict[str, Any] = {
            "code_list": normalized_codes,
            "local_path": self.config.local_path,
            "is_local": False,
        }
        if start_date is not None:
            kwargs["begin_date"] = to_yyyymmdd(start_date)
        if end_date is not None:
            kwargs["end_date"] = to_yyyymmdd(end_date)
        logger.info(
            "AmazingData %s start code_count=%s start_date=%s end_date=%s",
            method_name,
            len(normalized_codes),
            start_date,
            end_date,
        )
        result = method(**kwargs)
        logger.info(
            "AmazingData %s loaded result_type=%s rows=%s",
            method_name,
            type(result).__name__,
            _count_sdk_result_rows(result),
        )
        for record in _iter_records_from_sdk_result(
            result,
            action=method_name,
            injected_code_fields=("MARKET_CODE", "market_code", "CODE", "code"),
        ):
            market_code = _as_str(_record_get(record, "MARKET_CODE", "market_code", "CODE", "code"))
            if not market_code:
                continue
            report_date_value = _record_get(
                record,
                "REPORT_DATE",
                "report_date",
                "REPORT_PERIOD",
                "report_period",
                "END_DATE",
                "end_date",
                "ANN_DATE",
                "ann_date",
                "NOTICE_DATE",
                "notice_date",
                "TRADE_DATE",
                "trade_date",
            )
            report_date = None
            report_date_raw = _as_str(report_date_value)
            if report_date_value is not None:
                try:
                    report_date = to_ch_date(report_date_value)
                except Exception:
                    report_date = None
            yield InfoPayloadRow(
                market_code=market_code,
                report_date=report_date,
                report_date_raw=report_date_raw,
                payload_json=json.dumps(record, ensure_ascii=False, default=str, separators=(",", ":")),
            )

    def _build_info_kwargs(
        self,
        code_list: Sequence[str],
        start_date: Optional[date],
        end_date: Optional[date],
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "code_list": list(code_list),
            "local_path": self.config.local_path,
            "is_local": False,
        }
        if start_date is not None:
            kwargs["begin_date"] = to_yyyymmdd(start_date)
        if end_date is not None:
            kwargs["end_date"] = to_yyyymmdd(end_date)
        return kwargs

    def _call_info_with_cache_recovery(
        self,
        method_name: str,
        kwargs: dict[str, Any],
        cache_relative_path: str,
    ):
        method = getattr(self.session.info, method_name)
        try:
            return method(**kwargs)
        except Exception as exc:
            if not _is_hdf5_cache_error(exc):
                raise
            cache_path = Path(self.config.local_path.replace("//", "/")) / cache_relative_path
            recovered = _quarantine_corrupt_hdf5_cache(cache_path)
            if not recovered:
                raise
            logger.warning(
                "AmazingData %s detected corrupt HDF5 cache, quarantined file=%s and retrying once",
                method_name,
                cache_path,
            )
            return method(**kwargs)

    def _clear_cache_file_once(self, cache_relative_path: str) -> None:
        if cache_relative_path in self._cleared_cache_files:
            return
        cache_path = Path(self.config.local_path.replace("//", "/")) / cache_relative_path
        self._cleared_cache_files.add(cache_relative_path)
        try:
            if cache_path.exists():
                cache_path.unlink()
                logger.warning(
                    "AmazingData removed local cache before sync method=get_option_basic_info file=%s",
                    cache_path,
                )
        except Exception:
            logger.exception("删除本地缓存失败 file=%s", cache_path)


def _normalize_local_path(local_path: str) -> str:
    path = str(Path(local_path).resolve()).replace("\\", "//")
    if not path.endswith("//"):
        path += "//"
    return path


def _is_hdf5_cache_error(exc: Exception) -> bool:
    text = str(exc)
    lowered = text.lower()
    return (
        "hdf5" in lowered
        or "bad object header version number" in lowered
        or "incorrect metadata checksum" in lowered
        or ".h5" in lowered
    )


def _quarantine_corrupt_hdf5_cache(path: Path) -> bool:
    try:
        if not path.exists():
            return False
        suffix = datetime.now().strftime("%Y%m%d%H%M%S")
        target = path.with_name(f"{path.name}.bad.{suffix}")
        path.rename(target)
        return True
    except Exception:
        logger.exception("隔离损坏 HDF5 缓存失败 path=%s", path)
        return False


def _format_missing_runtime_error(field_names: str | Sequence[str], runtime_path: Path) -> str:
    if isinstance(field_names, str):
        missing = [field_names]
    else:
        missing = [str(item).strip() for item in field_names if str(item).strip()]
    missing_text = "、".join(missing) if missing else "sync.amazingdata"
    snippet = "\n".join(
        [
            "sync:",
            "  amazingdata:",
            "    username: YOUR_AMAZINGDATA_ACCOUNT",
            "    password: YOUR_AMAZINGDATA_PASSWORD",
            "    host: YOUR_AMAZINGDATA_HOST",
            "    port: 8600",
            "    local_path: /path/to/amazing_data_cache",
        ]
    )
    return (
        f"缺少运行配置 {missing_text}；已尝试读取 runtime 文件: {runtime_path}\n"
        f"请在 runtime.local.yaml 中补齐如下配置：\n{snippet}"
    )


def _normalize_calendar_result(result: Any) -> list:
    if result is None:
        return []
    if isinstance(result, list):
        normalized: list = []
        for item in result:
            try:
                normalized.append(to_yyyymmdd(item))
            except Exception:
                text = _as_str(item)
                if text is None:
                    continue
                normalized.append(text)
        return normalized
    return []


def _normalize_price_factor_frame(frame, requested_codes: Sequence[str]):
    if pd is None:  # pragma: no cover
        return frame

    normalized = frame.copy()
    requested = [str(code).strip() for code in requested_codes if str(code).strip()]
    if not requested or normalized.empty:
        return normalized

    column_names = [str(col).strip() for col in normalized.columns]
    index_names = [str(idx).strip() for idx in normalized.index]

    # 某些 SDK 版本会把 code 放在 index、日期放在 columns，这里统一转回“index=日期, columns=code”。
    if not any(col in requested for col in column_names) and any(idx in requested for idx in index_names):
        normalized = normalized.transpose()
        column_names = [str(col).strip() for col in normalized.columns]

    # 单只 code 查询时，优先相信请求入参的 code，避免 SDK 返回奇怪列名导致入库 code 错乱。
    if len(requested) == 1 and len(normalized.columns) == 1:
        normalized.columns = [requested[0]]
        return normalized

    if len(normalized.columns) == len(requested):
        matched_count = sum(1 for col in column_names if col in requested)
        if matched_count == 0:
            normalized.columns = requested

    return normalized


def _normalize_single_price_factor_frame(frame, requested_code: str):
    if pd is None:  # pragma: no cover
        return frame

    normalized = frame.copy()
    if normalized.empty:
        return normalized

    requested = str(requested_code).strip()
    column_names = [str(col).strip() for col in normalized.columns]
    index_names = [str(idx).strip() for idx in normalized.index]

    if requested not in column_names and requested in index_names:
        normalized = normalized.transpose()
        column_names = [str(col).strip() for col in normalized.columns]

    if requested in column_names:
        return normalized.loc[:, [requested]]

    return normalized


def _iter_natural_dates(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _ensure_dataframe(obj: Any, action: str):
    if pd is None:  # pragma: no cover
        raise RuntimeError("未安装 pandas，无法处理 SDK 返回的 DataFrame。")
    if not isinstance(obj, pd.DataFrame):
        raise TypeError(f"{action} 期望返回 DataFrame，实际得到 {type(obj).__name__}")
    return obj


def _frame_to_records(frame, index_field: str | None = None):
    normalized = frame.copy()
    if index_field is not None and index_field not in normalized.columns:
        normalized[index_field] = normalized.index
    return normalized.to_dict("records")


def _iter_records_from_sdk_result(
    obj: Any,
    action: str,
    injected_code_fields: Sequence[str] = ("MARKET_CODE", "market_code"),
    index_field: str | None = None,
):
    if pd is None:  # pragma: no cover
        raise RuntimeError("未安装 pandas，无法处理 SDK 返回的 DataFrame。")

    if isinstance(obj, pd.DataFrame):
        yield from _frame_to_records(obj, index_field=index_field)
        return

    if isinstance(obj, dict):
        for code, value in obj.items():
            if value is None:
                continue
            if not isinstance(value, pd.DataFrame):
                raise TypeError(
                    f"{action} 返回 dict 时，value 期望为 DataFrame，实际得到 {type(value).__name__}"
                )
            code_text = _as_str(code)
            for record in _frame_to_records(value, index_field=index_field):
                if code_text and all(_record_get(record, field) is None for field in injected_code_fields):
                    record[injected_code_fields[0]] = code_text
                yield record
        return

    raise TypeError(f"{action} 期望返回 DataFrame 或 dict，实际得到 {type(obj).__name__}")


def _iter_code_frames_from_result(obj: Any, action: str):
    if pd is None:  # pragma: no cover
        raise RuntimeError("未安装 pandas，无法处理 SDK 返回的 DataFrame。")

    yield from _iter_code_frames_from_result_inner(obj=obj, action=action, parent_code=None)


def _iter_code_frames_from_result_inner(obj: Any, action: str, parent_code: Optional[str]):
    if pd is None:  # pragma: no cover
        raise RuntimeError("未安装 pandas，无法处理 SDK 返回的 DataFrame。")

    if isinstance(obj, pd.DataFrame):
        code_column = None
        for candidate in ("code", "CODE", "market_code", "MARKET_CODE"):
            if candidate in obj.columns:
                code_column = candidate
                break
        if code_column is not None:
            for code, frame in obj.groupby(code_column):
                yield _as_str(code), frame.copy()
            return
        if parent_code is not None:
            yield parent_code, obj
            return
        raise TypeError(f"{action} 返回 DataFrame 时缺少 code 列，无法拆分为 dict[code, DataFrame]")

    if isinstance(obj, dict):
        for key, value in obj.items():
            if value is None:
                continue
            current_code = _as_str(key) or parent_code
            if isinstance(value, (dict, pd.DataFrame)):
                yield from _iter_code_frames_from_result_inner(
                    obj=value,
                    action=action,
                    parent_code=current_code,
                )
                continue
            raise TypeError(
                f"{action} 返回 dict 时，叶子节点期望为 DataFrame 或 dict，实际得到 {type(value).__name__}"
            )
        return

    raise TypeError(f"{action} 期望返回 DataFrame 或 dict，实际得到 {type(obj).__name__}")


def _count_sdk_result_rows(obj: Any) -> int:
    if pd is not None and isinstance(obj, pd.DataFrame):
        return int(len(obj))
    if isinstance(obj, dict):
        total = 0
        for value in obj.values():
            total += _count_sdk_result_rows(value)
        return total
    return 0


def _is_sdk_result_empty(obj: Any) -> bool:
    if obj is None:
        return True
    if pd is not None and isinstance(obj, pd.DataFrame):
        return obj.empty
    if isinstance(obj, dict):
        if not obj:
            return True
        return all(_is_sdk_result_empty(value) for value in obj.values())
    if isinstance(obj, (list, tuple, set)):
        return len(obj) == 0
    return False


def _prepare_market_time_frame(frame, action: str, time_field: str = "trade_time"):
    if pd is None:  # pragma: no cover
        raise RuntimeError("未安装 pandas，无法处理 SDK 返回的 DataFrame。")

    normalized = frame.copy()
    if time_field in normalized.columns:
        return normalized
    if time_field.upper() in normalized.columns:
        normalized[time_field] = normalized[time_field.upper()]
        return normalized

    for alias in ("kline_time", "KLINE_TIME", "snapshot_time", "SNAPSHOT_TIME"):
        if alias in normalized.columns:
            normalized[time_field] = normalized[alias]
            return normalized

    for candidate in ("trade_date", "TRADE_DATE", "date", "DATE", "datetime", "DATETIME"):
        if candidate in normalized.columns:
            normalized[time_field] = normalized[candidate]
            return normalized

    if _index_looks_like_datetime(normalized.index):
        normalized[time_field] = normalized.index
        return normalized

    raise TypeError(f"{action} DataFrame 缺少可识别的时间列，且 index 不是时间索引。")


def _index_looks_like_datetime(index) -> bool:
    if pd is None:  # pragma: no cover
        return False
    if isinstance(index, pd.DatetimeIndex):
        return True
    if isinstance(index, pd.PeriodIndex):
        return True
    if isinstance(index, pd.RangeIndex):
        return False
    preview = list(index[:3]) if len(index) >= 3 else list(index)
    if not preview:
        return False
    parsed_count = 0
    for value in preview:
        dt = _to_datetime(value)
        if dt is None:
            return False
        if dt.year < 1990:
            return False
        parsed_count += 1
    return parsed_count == len(preview)


def _record_get(record: dict[str, Any], *candidates: str) -> Any:
    for candidate in candidates:
        if candidate in record:
            return record[candidate]
        upper = candidate.upper()
        lower = candidate.lower()
        if upper in record:
            return record[upper]
        if lower in record:
            return record[lower]
    return None


def _to_datetime(value: Any):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=SHANGHAI_TZ)
        return value
    text = _as_str(value)
    if text:
        digits = "".join(ch for ch in text if ch.isdigit())
        try:
            if len(digits) == 8:
                dt = datetime.strptime(digits, "%Y%m%d")
                return dt.replace(tzinfo=SHANGHAI_TZ)
            if len(digits) == 12:
                dt = datetime.strptime(digits, "%Y%m%d%H%M")
                return dt.replace(tzinfo=SHANGHAI_TZ)
            if len(digits) == 14:
                dt = datetime.strptime(digits, "%Y%m%d%H%M%S")
                return dt.replace(tzinfo=SHANGHAI_TZ)
            if len(digits) == 17:
                dt = datetime.strptime(digits, "%Y%m%d%H%M%S%f")
                return dt.replace(tzinfo=SHANGHAI_TZ)
        except Exception:
            pass
    if pd is not None:
        try:
            dt = pd.to_datetime(value)
            if pd.isna(dt):
                return None
            py_dt = dt.to_pydatetime()
            if py_dt.tzinfo is None:
                return py_dt.replace(tzinfo=SHANGHAI_TZ)
            return py_dt
        except Exception:
            return None
    return None


def _series_get(series, *candidates: str) -> Any:
    for candidate in candidates:
        if candidate in series:
            return series[candidate]
        upper = candidate.upper()
        lower = candidate.lower()
        if upper in series:
            return series[upper]
        if lower in series:
            return series[lower]
    return None


def _as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if pd is not None and pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if pd is not None and pd.isna(value):
        return None
    try:
        return int(value)
    except Exception:
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if pd is not None and pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        text = str(value).strip()
        if not text:
            return None
        return float(text)


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if pd is not None and pd.isna(value):
        return None
    try:
        return to_ch_date(value)
    except Exception:
        return None


def _stringify(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        return ",".join(str(item) for item in value)
    return _as_str(value)


__all__ = [
    "AmazingDataSDKConfig",
    "AmazingDataSDKProvider",
    "AmazingDataSDKSession",
]
