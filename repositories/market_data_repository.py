#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MarketData 对应的 ClickHouse 读写层."""

from __future__ import annotations

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from sync_data_system.amazingdata_constants import KLINE_FIELDS, SNAPSHOT_FIELDS
from sync_data_system.clickhouse_tables import (
    AD_MARKET_KLINE_DAILY_TABLE,
    AD_MARKET_KLINE_MINUTE_TABLE,
    AD_MARKET_SNAPSHOT_TABLE,
    iter_market_data_table_ddls,
)
from sync_data_system.data_models import MarketKlineQuery, MarketKlineRow, MarketSnapshotQuery, MarketSnapshotRow
from sync_data_system.repositories.base_data_repository import BaseDataRepository


class MarketDataRepository(BaseDataRepository):
    """MarketData 的 repository."""

    MARKET_KLINE_COLUMNS = (
        "trade_time",
        "code",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
    )
    MARKET_SNAPSHOT_COLUMNS = (
        "trade_time",
        "code",
        "pre_close",
        "last",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "num_trades",
        "high_limited",
        "low_limited",
        "ask_price1",
        "ask_price2",
        "ask_price3",
        "ask_price4",
        "ask_price5",
        "ask_volume1",
        "ask_volume2",
        "ask_volume3",
        "ask_volume4",
        "ask_volume5",
        "bid_price1",
        "bid_price2",
        "bid_price3",
        "bid_price4",
        "bid_price5",
        "bid_volume1",
        "bid_volume2",
        "bid_volume3",
        "bid_volume4",
        "bid_volume5",
        "iopv",
        "trading_phase_code",
        "total_long_position",
        "pre_settle",
        "auction_price",
        "auction_volume",
        "settle",
        "contract_type",
        "expire_date",
        "underlying_security_code",
        "exercise_price",
        "action_day",
        "trading_day",
        "pre_open_interest",
        "open_interest",
        "average_price",
        "nominal_price",
        "ref_price",
        "bid_price_limit_up",
        "bid_price_limit_down",
        "offer_price_limit_up",
        "offer_price_limit_down",
    )

    def ensure_tables(self) -> None:
        super().ensure_tables()
        for ddl in iter_market_data_table_ddls():
            self.client.command(ddl)

    def save_market_kline_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_MARKET_KLINE_DAILY_TABLE,
            columns=self.MARKET_KLINE_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_market_kline_minute_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_MARKET_KLINE_MINUTE_TABLE,
            columns=self.MARKET_KLINE_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def save_market_snapshot_rows(self, rows) -> int:
        return self._insert_dataclass_rows_in_batches(
            table=AD_MARKET_SNAPSHOT_TABLE,
            columns=self.MARKET_SNAPSHOT_COLUMNS,
            rows=rows,
            single_insert=True,
        )

    def load_latest_kline_trade_date(self, code_list: list[str]):
        if not code_list:
            return None
        return self._load_latest_trade_date(AD_MARKET_KLINE_DAILY_TABLE, code_list)

    def load_latest_kline_minute_trade_date(self, code_list: list[str]):
        if not code_list:
            return None
        return self._load_latest_trade_date(AD_MARKET_KLINE_MINUTE_TABLE, code_list)

    def load_latest_kline_trade_date_map(self, code_list: list[str]) -> dict[str, object]:
        if not code_list:
            return {}
        return self._load_latest_trade_date_map(AD_MARKET_KLINE_DAILY_TABLE, code_list)

    def load_latest_kline_minute_trade_date_map(self, code_list: list[str]) -> dict[str, object]:
        if not code_list:
            return {}
        return self._load_latest_trade_date_map(AD_MARKET_KLINE_MINUTE_TABLE, code_list)

    def load_latest_snapshot_trade_date(self, code_list: list[str]):
        if not code_list:
            return None
        return self._load_latest_trade_date(AD_MARKET_SNAPSHOT_TABLE, code_list)

    def _load_latest_trade_date(self, table_name: str, code_list: list[str]):
        if not code_list:
            return None
        sql = f"""
        SELECT code, max(toDate(trade_time)) AS latest_trade_date
        FROM {table_name}
        WHERE code IN {{code_list:Array(String)}}
        GROUP BY code
        ORDER BY code
        """
        rows = self.client.query_rows(sql, {"code_list": code_list})
        if len(rows) != len(code_list):
            return None
        latest_dates = [row[1] for row in rows if len(row) > 1 and row[1] is not None]
        if not latest_dates:
            return None
        return min(latest_dates)

    def _load_latest_trade_date_map(self, table_name: str, code_list: list[str]) -> dict[str, object]:
        if not code_list:
            return {}
        sql = f"""
        SELECT code, max(toDate(trade_time)) AS latest_trade_date
        FROM {table_name}
        WHERE code IN {{code_list:Array(String)}}
        GROUP BY code
        ORDER BY code
        """
        rows = self.client.query_rows(sql, {"code_list": code_list})
        result = {str(code): None for code in code_list}
        for row in rows:
            if not row:
                continue
            code = str(row[0])
            latest_trade_date = row[1] if len(row) > 1 else None
            if latest_trade_date is not None:
                # 过滤明显错误的脏时间，避免历史错误数据把增量基准拉回 1970 年。
                if str(latest_trade_date) < "1990-01-01":
                    latest_trade_date = None
            result[code] = latest_trade_date
        return result

    def load_kline_dict(self, query: MarketKlineQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")
        if not query.code_list:
            return {}
        return self._load_kline_dict_from_table(AD_MARKET_KLINE_DAILY_TABLE, query)

    def load_kline_minute_dict(self, query: MarketKlineQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")
        if not query.code_list:
            return {}
        return self._load_kline_dict_from_table(AD_MARKET_KLINE_MINUTE_TABLE, query)

    def _load_kline_dict_from_table(self, table_name: str, query: MarketKlineQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")
        if not query.code_list:
            return {}

        sql = f"""
        SELECT
            code,
            trade_time,
            any(open) AS open,
            any(high) AS high,
            any(low) AS low,
            any(close) AS close,
            any(volume) AS volume,
            any(amount) AS amount
        FROM {table_name}
        WHERE code IN {{code_list:Array(String)}}
          AND toDate(trade_time) >= {{begin_date:Date}}
          AND toDate(trade_time) <= {{end_date:Date}}
        GROUP BY code, trade_time
        ORDER BY code, trade_time
        """
        frame = self.client.query_df(
            sql,
            {
                "code_list": list(query.code_list),
                "begin_date": query.begin_date,
                "end_date": query.end_date,
            },
        )
        if frame.empty:
            return {}

        frame["trade_time"] = pd.to_datetime(frame["trade_time"])
        frame = _filter_kline_time(frame, query.begin_time, query.end_time)
        if frame.empty:
            return {}

        return _build_market_dict(
            frame=frame,
            code_list=query.code_list,
            ordered_columns=[field for field in KLINE_FIELDS if field != "trade_time"],
        )

    def load_snapshot_dict(self, query: MarketSnapshotQuery):
        if pd is None:  # pragma: no cover
            raise RuntimeError("未安装 pandas，无法返回 DataFrame。")
        if not query.code_list:
            return {}

        sql = f"""
        SELECT
            code,
            trade_time,
            any(pre_close) AS pre_close,
            any(last) AS last,
            any(open) AS open,
            any(high) AS high,
            any(low) AS low,
            any(close) AS close,
            any(volume) AS volume,
            any(amount) AS amount,
            any(num_trades) AS num_trades,
            any(high_limited) AS high_limited,
            any(low_limited) AS low_limited,
            any(ask_price1) AS ask_price1,
            any(ask_price2) AS ask_price2,
            any(ask_price3) AS ask_price3,
            any(ask_price4) AS ask_price4,
            any(ask_price5) AS ask_price5,
            any(ask_volume1) AS ask_volume1,
            any(ask_volume2) AS ask_volume2,
            any(ask_volume3) AS ask_volume3,
            any(ask_volume4) AS ask_volume4,
            any(ask_volume5) AS ask_volume5,
            any(bid_price1) AS bid_price1,
            any(bid_price2) AS bid_price2,
            any(bid_price3) AS bid_price3,
            any(bid_price4) AS bid_price4,
            any(bid_price5) AS bid_price5,
            any(bid_volume1) AS bid_volume1,
            any(bid_volume2) AS bid_volume2,
            any(bid_volume3) AS bid_volume3,
            any(bid_volume4) AS bid_volume4,
            any(bid_volume5) AS bid_volume5,
            any(iopv) AS iopv,
            any(trading_phase_code) AS trading_phase_code,
            any(total_long_position) AS total_long_position,
            any(pre_settle) AS pre_settle,
            any(auction_price) AS auction_price,
            any(auction_volume) AS auction_volume,
            any(settle) AS settle,
            any(contract_type) AS contract_type,
            any(expire_date) AS expire_date,
            any(underlying_security_code) AS underlying_security_code,
            any(exercise_price) AS exercise_price,
            any(action_day) AS action_day,
            any(trading_day) AS trading_day,
            any(pre_open_interest) AS pre_open_interest,
            any(open_interest) AS open_interest,
            any(average_price) AS average_price,
            any(nominal_price) AS nominal_price,
            any(ref_price) AS ref_price,
            any(bid_price_limit_up) AS bid_price_limit_up,
            any(bid_price_limit_down) AS bid_price_limit_down,
            any(offer_price_limit_up) AS offer_price_limit_up,
            any(offer_price_limit_down) AS offer_price_limit_down
        FROM {AD_MARKET_SNAPSHOT_TABLE}
        WHERE code IN {{code_list:Array(String)}}
          AND toDate(trade_time) >= {{begin_date:Date}}
          AND toDate(trade_time) <= {{end_date:Date}}
        GROUP BY code, trade_time
        ORDER BY code, trade_time
        """
        frame = self.client.query_df(
            sql,
            {
                "code_list": list(query.code_list),
                "begin_date": query.begin_date,
                "end_date": query.end_date,
            },
        )
        if frame.empty:
            return {}

        frame["trade_time"] = pd.to_datetime(frame["trade_time"])
        frame = _filter_snapshot_time(frame, query.begin_time, query.end_time)
        if frame.empty:
            return {}

        return _build_snapshot_dict(frame=frame, code_list=query.code_list)


def _build_market_dict(frame, code_list, ordered_columns: list[str]):
    result = {}
    for code in code_list:
        subset = frame[frame["code"] == code].copy()
        if subset.empty:
            continue
        subset = subset.set_index("trade_time")
        result[code] = subset.loc[:, ordered_columns]
    return result


def _build_snapshot_dict(frame, code_list):
    result = {}
    for code in code_list:
        subset = frame[frame["code"] == code].copy()
        if subset.empty:
            continue
        ordered_columns = [field for field in SNAPSHOT_FIELDS if field != "trade_time"]
        subset = subset.set_index("trade_time")
        result[code] = subset.loc[:, [col for col in ordered_columns if col in subset.columns]]
    return result


def _filter_kline_time(frame, begin_time, end_time):
    if begin_time is None and end_time is None:
        return frame
    hhmm = frame["trade_time"].dt.hour * 100 + frame["trade_time"].dt.minute
    mask = pd.Series(True, index=frame.index)
    if begin_time is not None:
        mask &= hhmm >= int(begin_time)
    if end_time is not None:
        mask &= hhmm <= int(end_time)
    return frame.loc[mask].copy()


def _filter_snapshot_time(frame, begin_time, end_time):
    if begin_time is None and end_time is None:
        return frame
    hhmmssmmm = (
        frame["trade_time"].dt.hour * 10000000
        + frame["trade_time"].dt.minute * 100000
        + frame["trade_time"].dt.second * 1000
        + (frame["trade_time"].dt.microsecond // 1000)
    )
    mask = pd.Series(True, index=frame.index)
    if begin_time is not None:
        mask &= hhmmssmmm >= int(begin_time)
    if end_time is not None:
        mask &= hhmmssmmm <= int(end_time)
    return frame.loc[mask].copy()


__all__ = ["MarketDataRepository"]
