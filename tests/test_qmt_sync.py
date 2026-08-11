#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

from sync_data_system.providers.qmt.provider import iter_qmt_rows, normalize_qmt_code, normalize_qmt_code_list
from sync_data_system.providers.qmt.repository import QmtRepository
from sync_data_system.providers.qmt.runner import (
    SyncArgs,
    apply_task_defaults,
    build_fetch_kwargs,
    build_request_meta,
    expand_task_args,
    load_execution_plan_from_toml,
    resolve_effective_request_meta,
    resolve_auto_symbol_universe,
    run_sync_args,
    validate_required_request,
)
from sync_data_system.providers.qmt.specs import QMT_TASK_SPECS


class _FakeClickHouseClient:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.insert_calls: list[tuple[str, list[str], list[tuple]]] = []
        self.query_value_calls: list[tuple[str, dict | None]] = []
        self.query_value_result = 0
        self.query_rows_result: list[tuple] = []

    def command(self, sql: str, parameters=None):
        self.commands.append(sql)

    def insert_rows(self, table: str, column_names, rows):
        self.insert_calls.append((table, list(column_names), list(rows)))

    def query_value(self, sql: str, parameters=None):
        self.query_value_calls.append((sql, parameters))
        return self.query_value_result

    def query_rows(self, sql: str, parameters=None):
        return list(self.query_rows_result)


class _FakeQmtProvider:
    def __init__(self, envelope, *, sector_envelope=None, sector_envelopes=None) -> None:
        self.envelope = envelope
        self.sector_envelope = sector_envelope
        self.sector_envelopes = dict(sector_envelopes or {})
        self.fetch_calls: list[dict] = []

    def fetch_task(self, task: str, **kwargs):
        self.fetch_calls.append({"task": task, **kwargs})
        if task == "sectors" and kwargs.get("sector_name") in self.sector_envelopes:
            return self.sector_envelopes[kwargs["sector_name"]]
        if task == "sectors" and self.sector_envelope is not None:
            return self.sector_envelope
        return self.envelope


class _FakeIncrementalRepository:
    def __init__(self, latest_cursor: str | None = None) -> None:
        self.latest_cursor = latest_cursor

    def load_latest_cursor(self, task: str, *, symbol: str | None = None):
        return self.latest_cursor


class QmtProviderHelperTest(unittest.TestCase):
    def test_normalize_qmt_code(self) -> None:
        self.assertEqual(normalize_qmt_code("sh.600000"), "600000.SH")
        self.assertEqual(normalize_qmt_code("000001.sz"), "000001.SZ")
        self.assertEqual(normalize_qmt_code("IF2406.CFFEX"), "IF2406.CFFEX")

    def test_normalize_qmt_code_list_deduplicates(self) -> None:
        self.assertEqual(
            normalize_qmt_code_list(["sh.600000", "600000.SH", "sz.000001"]),
            ["600000.SH", "000001.SZ"],
        )

    def test_iter_kline_history_rows_expands_bars(self) -> None:
        envelope = {
            "success": True,
            "data": {
                "items": [
                    {
                        "symbol": "600000.SH",
                        "bars": [
                            {"time_ms": 1704038400000, "open": 8.1, "close": 8.2},
                            {"time_ms": 1704124800000, "open": 8.2, "close": 8.3},
                        ],
                    }
                ]
            },
        }

        rows = iter_qmt_rows(
            QMT_TASK_SPECS["kline_history"],
            envelope,
            {"symbol": "600000.SH", "period": "1d", "start_time": "20240101", "end_time": "20240131"},
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["symbol"], "600000.SH")
        self.assertNotIn("period", rows[0])
        self.assertEqual(rows[0]["time_ms"], 1704038400000)

    def test_iter_sector_rows_preserves_qmt_symbols_collection(self) -> None:
        envelope = {"success": True, "data": {"items": [{"sector_name": "沪深A股", "symbols": ["sh.600000", "sz.000001"]}]}}

        rows = iter_qmt_rows(QMT_TASK_SPECS["sectors"], envelope, {"sector_name": "沪深A股"})

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbols"], ["600000.SH", "000001.SZ"])
        self.assertEqual(rows[0]["sector_name"], "沪深A股")


class QmtRepositoryTest(unittest.TestCase):
    def test_business_tables_exclude_ingestion_metadata(self) -> None:
        repository = QmtRepository(_FakeClickHouseClient(), database="qmt")

        for task, spec in QMT_TASK_SPECS.items():
            with self.subTest(task=task):
                columns = repository.table_columns_for_spec(spec)
                self.assertTrue(
                    {
                        "task",
                        "source",
                        "fetched_at",
                        "ingested_at",
                        "payload_json",
                        "request_start_time",
                        "request_end_time",
                        "record_index",
                        "field_name",
                        "field_value",
                        "extra_fields",
                    }.isdisjoint(columns)
                )
                ddl = repository._create_task_table_ddl(spec)
                self.assertNotIn("source String", ddl)
                self.assertNotIn("fetched_at", ddl)
                self.assertNotIn("ingested_at", ddl)
                self.assertNotIn("payload_json", ddl)
                self.assertIn("ENGINE = ReplacingMergeTree()", ddl)

    def test_kline_table_contains_only_qmt_bar_fields(self) -> None:
        columns = QmtRepository.table_columns_for_spec(QMT_TASK_SPECS["kline_history"])

        self.assertEqual(
            columns,
            (
                "symbol",
                "time_ms",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "settle",
                "open_interest",
                "pre_close",
                "suspend_flag",
            ),
        )

    def test_latest_cursor_query_uses_only_real_table_columns(self) -> None:
        client = _FakeClickHouseClient()
        client.query_value_result = 1704038400000
        repository = QmtRepository(client, database="qmt")

        cursor = repository.load_latest_cursor("kline_history", symbol="sh.600000")

        self.assertEqual(cursor, "1704038400000")
        sql, parameters = client.query_value_calls[0]
        self.assertNotIn("task =", sql)
        self.assertIn("symbol =", sql)
        self.assertEqual(parameters, {"symbol": "600000.SH"})

    def test_nested_frame_cursor_is_not_queried_as_a_fake_column(self) -> None:
        client = _FakeClickHouseClient()
        repository = QmtRepository(client, database="qmt")

        self.assertIsNone(repository.load_latest_cursor("market_data_ex", symbol="600000.SH"))
        self.assertEqual(client.query_value_calls, [])

    def test_outdated_payload_table_is_dropped_for_resync(self) -> None:
        client = _FakeClickHouseClient()

        def query_rows(sql: str, parameters=None):
            if "system.columns" in sql:
                return [("task",), ("payload_json",)]
            return []

        client.query_rows = query_rows
        repository = QmtRepository(client, database="qmt")
        spec = QMT_TASK_SPECS["kline_history"]

        repository._recreate_outdated_task_table(spec)

        commands = "\n".join(client.commands)
        self.assertIn("DROP TABLE IF EXISTS qmt.qmt_kline_history", commands)
        self.assertIn("open Float64", commands)

    def test_ensure_tables_clears_qmt_progress_after_schema_rebuild(self) -> None:
        client = _FakeClickHouseClient()

        def query_rows(sql: str, parameters=None):
            if "system.columns" in sql:
                return [("task",), ("payload_json",)]
            return []

        client.query_rows = query_rows
        repository = QmtRepository(client, database="qmt")

        repository.ensure_tables()

        commands = "\n".join(client.commands)
        self.assertIn("TRUNCATE TABLE qmt.qmt_sync_task_log", commands)
        self.assertIn("TRUNCATE TABLE qmt.qmt_sync_checkpoint", commands)


class QmtRunnerTest(unittest.TestCase):
    def _args(self, **overrides) -> SyncArgs:
        data = {
            "task": "kline_history",
            "symbols_raw": "sh.600000",
            "symbol": "",
            "market": "",
            "index_code": "",
            "stock_code": "",
            "table_names_raw": "",
            "sector_name": "",
            "code_market": "",
            "begin_time": "20240101",
            "end_time": "20240131",
            "period": "1d",
            "fields_raw": "",
            "adjust_type": "none",
            "fill_data": True,
            "count": -1,
            "incrementally": False,
            "complete": False,
            "limit": 0,
            "force": True,
            "continue_on_error": False,
            "runtime_path": None,
            "database": "qmt",
            "log_level": "INFO",
        }
        data.update(overrides)
        return SyncArgs(**data)

    def test_build_request_meta_normalizes_symbols(self) -> None:
        meta = build_request_meta(self._args(symbols_raw="sh.600000,600000.SH,sz.000001"))

        self.assertEqual(meta["symbols"], ["600000.SH", "000001.SZ"])
        self.assertEqual(meta["period"], "1d")
        self.assertEqual(meta["start_time"], "20240101")

    def test_build_fetch_kwargs_for_kline(self) -> None:
        args = self._args()
        meta = build_request_meta(args)

        kwargs = build_fetch_kwargs(args, meta)

        self.assertEqual(kwargs["symbols"], ["600000.SH"])
        self.assertEqual(kwargs["start_time"], "20240101")
        self.assertEqual(kwargs["end_time"], "20240131")
        self.assertEqual(kwargs["period"], "1d")
        self.assertTrue(kwargs["fill_data"])

    def test_run_sync_args_saves_response(self) -> None:
        provider = _FakeQmtProvider(
            {
                "success": True,
                "data": {
                    "items": [
                        {
                            "symbol": "600000.SH",
                            "bars": [
                                {
                                    "time_ms": 1704038400000,
                                    "open": 8.1,
                                }
                            ],
                        }
                    ]
                },
            }
        )
        client = _FakeClickHouseClient()
        repository = QmtRepository(client, database="qmt")

        inserted = run_sync_args(self._args(), provider, repository)

        self.assertEqual(inserted, 1)
        self.assertEqual(provider.fetch_calls[0]["symbols"], ["600000.SH"])
        table, columns, rows = client.insert_calls[0]
        self.assertEqual(table, "qmt.qmt_kline_history")
        self.assertNotIn("payload_json", columns)
        self.assertIn("open", columns)
        row = dict(zip(columns, rows[0]))
        self.assertEqual(row["symbol"], "600000.SH")
        self.assertEqual(row["time_ms"], 1704038400000)
        self.assertEqual(row["open"], 8.1)
        self.assertNotIn("extra_fields", row)

    def test_tick_payload_is_materialized_into_typed_columns(self) -> None:
        provider = _FakeQmtProvider(
            {
                "success": True,
                "data": {
                    "items": [
                        {
                            "symbol": "600000.SH",
                            "ticks": [
                                {
                                    "time_ms": 1704072600000,
                                    "last_price": 8.2,
                                    "ask_price": [8.21, 8.22],
                                    "ask_vol": [1000, 2000],
                                }
                            ],
                        }
                    ]
                },
            }
        )
        client = _FakeClickHouseClient()
        repository = QmtRepository(client, database="qmt")

        inserted = run_sync_args(self._args(task="tick_history"), provider, repository)

        self.assertEqual(inserted, 1)
        _, columns, rows = client.insert_calls[0]
        row = dict(zip(columns, rows[0]))
        self.assertEqual(row["last_price"], 8.2)
        self.assertEqual(row["ask_price"], [8.21, 8.22])
        self.assertEqual(row["ask_vol"], [1000, 2000])
        self.assertNotIn("extra_fields", row)

    def test_instrument_preserves_exact_symbol_and_fields_response(self) -> None:
        provider = _FakeQmtProvider(
            {
                "success": True,
                "data": {
                    "symbol": "600000.SH",
                    "fields": {
                        "InstrumentID": "600000.SH",
                        "InstrumentName": "浦发银行",
                    },
                },
            }
        )
        client = _FakeClickHouseClient()
        repository = QmtRepository(client, database="qmt")

        inserted = run_sync_args(
            self._args(task="instrument", symbols_raw="600000.SH", begin_time="", end_time=""),
            provider,
            repository,
        )

        self.assertEqual(inserted, 1)
        _, columns, rows = client.insert_calls[0]
        self.assertEqual(columns, ["symbol", "fields"])
        row = dict(zip(columns, rows[0]))
        self.assertEqual(row["symbol"], "600000.SH")
        self.assertEqual(row["fields"]["InstrumentName"], "浦发银行")

    def test_run_sync_args_auto_resolves_qmt_symbol_universe(self) -> None:
        provider = _FakeQmtProvider(
            {"success": True, "data": {"items": [{"symbol": "600000.SH", "bars": [{"time_ms": 1704038400000, "open": 8.1}]}]}},
            sector_envelope={
                "success": True,
                "data": {"items": [{"sector_name": "沪深A股", "symbols": ["sh.600000", "sz.000001"]}]},
            },
        )
        client = _FakeClickHouseClient()
        repository = QmtRepository(client, database="qmt")

        inserted = run_sync_args(self._args(symbols_raw="", limit=1), provider, repository)

        self.assertEqual(inserted, 1)
        self.assertEqual(
            [(call["task"], call.get("market")) for call in provider.fetch_calls[:2]],
            [("download_history_contracts", "SH"), ("download_history_contracts", "SZ")],
        )
        self.assertEqual(provider.fetch_calls[3]["task"], "sectors")
        self.assertEqual(provider.fetch_calls[3]["sector_name"], "沪深A股")
        self.assertEqual(provider.fetch_calls[4]["sector_name"], "过期沪深A股")
        self.assertEqual(provider.fetch_calls[5]["task"], "kline_history")
        self.assertEqual(provider.fetch_calls[5]["symbols"], ["600000.SH"])

    def test_cb_info_uses_historical_convertible_bond_universe(self) -> None:
        provider = _FakeQmtProvider(
            {
                "success": True,
                "data": {
                    "symbol": "113001.SH",
                    "fields": {"bondCode": "113001.SH"},
                },
            },
            sector_envelopes={
                "沪深转债": {
                    "success": True,
                    "data": {"items": [{"sector_name": "沪深转债", "symbols": ["113001.SH"]}]},
                },
                "过期沪深转债": {
                    "success": True,
                    "data": {"items": [{"sector_name": "过期沪深转债", "symbols": ["123001.SZ"]}]},
                },
            },
        )
        repository = QmtRepository(_FakeClickHouseClient(), database="qmt")

        inserted = run_sync_args(
            self._args(task="cb_info", symbols_raw="", begin_time="", end_time="", limit=0),
            provider,
            repository,
        )

        self.assertEqual(inserted, 2)
        self.assertEqual(
            [call["task"] for call in provider.fetch_calls[:4]],
            ["download_history_contracts", "download_history_contracts", "download_sector", "download_cb"],
        )
        self.assertEqual(
            [call["symbol"] for call in provider.fetch_calls if call["task"] == "cb_info"],
            ["113001.SH", "123001.SZ"],
        )

    def test_cb_info_requires_qmt_historical_universe(self) -> None:
        provider = _FakeQmtProvider({"success": True, "data": {}})
        repository = QmtRepository(_FakeClickHouseClient(), database="qmt")

        with self.assertRaisesRegex(ValueError, "QMT 历史合约和板块数据"):
            run_sync_args(
                self._args(task="cb_info", symbols_raw="", begin_time="", end_time=""),
                provider,
                repository,
            )

    def test_single_symbol_task_expands_all_supplied_codes(self) -> None:
        tasks = expand_task_args(
            self._args(task="cb_info", symbols_raw="113001.SH,123001.SZ", symbol="113001.SH")
        )

        self.assertEqual([task.symbol for task in tasks], ["113001.SH", "123001.SZ"])

    def test_etf_info_uses_qmt_all_etf_response_without_external_codes(self) -> None:
        provider = _FakeQmtProvider(
            {
                "success": True,
                "data": {
                    "510300.SH": {"stockCode": "510300.SH", "stockName": "沪深300ETF"}
                },
            }
        )
        client = _FakeClickHouseClient()
        repository = QmtRepository(client, database="qmt")

        inserted = run_sync_args(
            self._args(
                task="etf_info",
                symbols_raw="",
                symbol="",
                begin_time="",
                end_time="",
            ),
            provider,
            repository,
        )

        self.assertEqual(inserted, 1)
        _, columns, rows = client.insert_calls[0]
        self.assertEqual(dict(zip(columns, rows[0]))["fields"]["stockName"], "沪深300ETF")

    def test_registered_qmt_tasks_resolve_required_defaults_without_ui_params(self) -> None:
        provider = _FakeQmtProvider(
            {"success": True, "data": {"items": []}},
            sector_envelopes={
                "沪深A股": {
                    "success": True,
                    "data": {
                        "items": [
                            {"sector_name": "沪深A股", "symbols": ["600000.SH"]}
                        ]
                    },
                },
                "过期沪深A股": {
                    "success": True,
                    "data": {
                        "items": [
                            {"sector_name": "过期沪深A股", "symbols": ["600001.SH"]}
                        ]
                    },
                },
            },
        )
        tasks = (
            "divid_factors",
            "download_financial",
            "download_history",
            "download_history_batch",
            "financial",
            "full_tick",
            "instrument",
            "instrument_type",
        )

        for task in tasks:
            with self.subTest(task=task):
                args = apply_task_defaults(
                    self._args(
                        task=task,
                        symbols_raw="",
                        symbol="",
                        stock_code="",
                        table_names_raw="",
                        begin_time="",
                        end_time="",
                    )
                )
                args = resolve_auto_symbol_universe(args, provider)
                requests = expand_task_args(args)
                self.assertTrue(requests)
                for request_args in requests:
                    validate_required_request(
                        request_args,
                        build_request_meta(request_args),
                    )

    def test_task_defaults_expand_required_market_index_and_financial_dimensions(self) -> None:
        contract_requests = expand_task_args(
            apply_task_defaults(
                self._args(
                    task="download_history_contracts",
                    symbols_raw="",
                    market="",
                    begin_time="",
                    end_time="",
                )
            )
        )
        self.assertEqual([item.market for item in contract_requests], ["SH", "SZ"])

        index_request = expand_task_args(
            apply_task_defaults(
                self._args(
                    task="index_weight",
                    symbols_raw="",
                    index_code="",
                    begin_time="",
                    end_time="",
                )
            )
        )[0]
        self.assertEqual(index_request.index_code, "000300.SH")

        financial_args = apply_task_defaults(
            self._args(task="financial", table_names_raw="")
        )
        self.assertEqual(
            financial_args.table_names_raw,
            "Balance,Income,CashFlow",
        )

    def test_realtime_auto_universe_excludes_historical_symbols(self) -> None:
        provider = _FakeQmtProvider(
            {"success": True, "data": {"items": []}},
            sector_envelopes={
                "沪深A股": {
                    "success": True,
                    "data": {
                        "items": [
                            {"sector_name": "沪深A股", "symbols": ["600000.SH"]}
                        ]
                    },
                },
                "过期沪深A股": {
                    "success": True,
                    "data": {
                        "items": [
                            {"sector_name": "过期沪深A股", "symbols": ["600001.SH"]}
                        ]
                    },
                },
            },
        )

        resolved = resolve_auto_symbol_universe(
            self._args(task="full_tick", symbols_raw=""),
            provider,
        )

        self.assertEqual(resolved.symbols_raw, "600000.SH")
        self.assertEqual(
            [call["task"] for call in provider.fetch_calls],
            ["download_sector", "sectors"],
        )

    def test_tick_history_keeps_intraday_time_window(self) -> None:
        args = self._args(
            task="tick_history",
            begin_time="20240101093000",
            end_time="20240101150000",
        )
        meta = build_request_meta(args)

        effective = resolve_effective_request_meta(args, _FakeIncrementalRepository(), meta)

        self.assertIsNotNone(effective)
        self.assertEqual(effective["start_time"], "20240101093000")
        self.assertEqual(effective["end_time"], "20240101150000")

    def test_load_execution_plan_from_toml(self) -> None:
        content = textwrap.dedent(
            """
            source = "qmt"
            log_level = "INFO"
            continue_on_error = true
            database = "qmt"

            [defaults]
            codes = ["600000.SH"]
            begin_date = 20240101
            end_date = 20240131

            [[tasks]]
            task = "kline_history"
            period = "1d"

            [[tasks]]
            task = "sectors"
            sector_name = "沪深A股"
            """
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "cfg.toml"
            path.write_text(content, encoding="utf-8")
            plan = load_execution_plan_from_toml(str(path))

        self.assertEqual(plan.database, "qmt")
        self.assertEqual(len(plan.tasks), 2)
        self.assertEqual(plan.tasks[0].task, "kline_history")
        self.assertEqual(plan.tasks[0].symbols_raw, "600000.SH")
        self.assertEqual(plan.tasks[0].begin_time, "20240101")
        self.assertEqual(plan.tasks[1].sector_name, "沪深A股")


if __name__ == "__main__":
    unittest.main()
