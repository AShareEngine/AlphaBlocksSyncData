#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from sync_data_system.amazingdata_constants import FactorType
from sync_data_system.run_sync import TaskRunSpec, build_execution_plan, build_resume_scope_pairs, filter_code_list_for_resume, resolve_code_list


class _FakeRepository:
    def __init__(self, successful_scope_keys=None) -> None:
        self.successful_scope_keys = set(successful_scope_keys or [])
        self.last_task_name = ""
        self.last_scope_keys = []

    def load_successful_scope_keys(self, task_name: str, scope_keys):
        self.last_task_name = task_name
        self.last_scope_keys = list(scope_keys)
        return set(scope_keys) & self.successful_scope_keys


class _FakeBaseData:
    def __init__(self, repository) -> None:
        self.repository = repository
        self.hist_code_calls = []
        self.option_code_calls = []
        self.stock_universe_calls = []
        self.index_universe_calls = []
        self.etf_universe_calls = []

    @staticmethod
    def _build_factor_scope_key(factor_type: str, code_list):
        return f"factor:{factor_type}:{code_list[0]}"

    @staticmethod
    def _build_code_scope_key(task_name: str, code_list):
        return f"base:{task_name}:{code_list[0]}"

    def get_hist_code_list(self, security_type: str, start_date: int, end_date: int, local_path: str):
        self.hist_code_calls.append((security_type, start_date, end_date, local_path))
        return ["10000001.SH", "10000002.SH"]

    def get_option_code_list(self, security_type: str, force: bool = False):
        self.option_code_calls.append((security_type, force))
        return ["10000001.SH", "10000002.SH"]

    def get_stock_universe(self, security_type: str, force: bool = False):
        self.stock_universe_calls.append((security_type, force))
        return ["000001.SZ", "600000.SH"]

    def get_index_universe(self, security_type: str, force: bool = False):
        self.index_universe_calls.append((security_type, force))
        return ["000300.SH"]

    def get_etf_universe(self, security_type: str, force: bool = False):
        self.etf_universe_calls.append((security_type, force))
        return ["510300.SH"]


class _FakeInfoData:
    @staticmethod
    def _build_code_scope_key(task_name: str, code_list, begin_date=None, end_date=None):
        return f"info:{task_name}:{code_list[0]}:{begin_date}:{end_date}"


class _FakeMarketData:
    @staticmethod
    def _build_market_scope_key(task_name: str, code_list, begin_date, end_date, period=None, begin_time=None, end_time=None):
        return f"market:{task_name}:{code_list[0]}:{begin_date}:{end_date}:{period}:{begin_time}:{end_time}"


class RunSyncResumeTest(unittest.TestCase):
    def _build_context(self, successful_scope_keys=None):
        repository = _FakeRepository(successful_scope_keys=successful_scope_keys)
        return SimpleNamespace(
            base_data=_FakeBaseData(repository),
            info_data=_FakeInfoData(),
            market_data=_FakeMarketData(),
        )

    def test_build_resume_scope_pairs_for_adj_factor(self) -> None:
        context = self._build_context()

        result = build_resume_scope_pairs(
            context=context,
            task="adj_factor",
            code_list=["000001.SZ", "000002.SZ"],
            begin_date=20240101,
            end_date=20240131,
        )

        self.assertEqual(
            result,
            [
                ("000001.SZ", f"factor:{FactorType.ADJ}:000001.SZ", "get_adj_factor"),
                ("000002.SZ", f"factor:{FactorType.ADJ}:000002.SZ", "get_adj_factor"),
            ],
        )

    def test_filter_code_list_for_resume_skips_successful_codes(self) -> None:
        successful = {"factor:backward:000001.SZ", "factor:backward:000002.SZ"}
        context = self._build_context(successful_scope_keys=successful)
        task_spec = TaskRunSpec(task="backward_factor", resume=True)

        result = filter_code_list_for_resume(
            context=context,
            task_spec=task_spec,
            code_list=["000001.SZ", "000002.SZ", "000004.SZ"],
            begin_date=20240101,
            end_date=20240131,
        )

        self.assertEqual(result, ["000004.SZ"])
        self.assertEqual(context.base_data.repository.last_task_name, "get_backward_factor")

    def test_etf_pcf_resume_requires_info_and_constituent_both_success(self) -> None:
        repository = _FakeRepository(
            successful_scope_keys={
                "base:get_etf_pcf:510050.SH",
                "base:get_etf_pcf_constituent:510050.SH",
                "base:get_etf_pcf:510300.SH",
            }
        )
        context = SimpleNamespace(
            base_data=_FakeBaseData(repository),
            info_data=_FakeInfoData(),
            market_data=_FakeMarketData(),
        )
        task_spec = TaskRunSpec(task="etf_pcf", resume=True)

        result = filter_code_list_for_resume(
            context=context,
            task_spec=task_spec,
            code_list=["510050.SH", "510300.SH"],
            begin_date=20240101,
            end_date=20240131,
        )

        self.assertEqual(result, ["510300.SH"])

    def test_build_resume_scope_pairs_for_new_info_tasks(self) -> None:
        context = self._build_context()

        fund_iopv_pairs = build_resume_scope_pairs(
            context=context,
            task="fund_iopv",
            code_list=["510050.SH"],
            begin_date=20240101,
            end_date=20240131,
        )
        kzz_issuance_pairs = build_resume_scope_pairs(
            context=context,
            task="kzz_issuance",
            code_list=["110000.SH"],
            begin_date=20240101,
            end_date=20240131,
        )
        kzz_share_pairs = build_resume_scope_pairs(
            context=context,
            task="kzz_share",
            code_list=["110000.SH"],
            begin_date=20240101,
            end_date=20240131,
        )
        kzz_suspend_pairs = build_resume_scope_pairs(
            context=context,
            task="kzz_suspend",
            code_list=["110000.SH"],
            begin_date=20240101,
            end_date=20240131,
        )

        self.assertEqual(
            fund_iopv_pairs,
            [("510050.SH", "info:get_fund_iopv:510050.SH:2024-01-01:2024-01-31", "get_fund_iopv")],
        )
        self.assertEqual(
            kzz_issuance_pairs,
            [("110000.SH", "info:get_kzz_issuance:110000.SH:None:None", "get_kzz_issuance")],
        )
        self.assertEqual(
            kzz_share_pairs,
            [("110000.SH", "info:get_kzz_share:110000.SH:None:None", "get_kzz_share")],
        )
        self.assertEqual(
            kzz_suspend_pairs,
            [("110000.SH", "info:get_kzz_suspend:110000.SH:None:None", "get_kzz_suspend")],
        )
        put_pairs = build_resume_scope_pairs(
            context=context,
            task="kzz_put_explanation",
            code_list=["110000.SH"],
            begin_date=20240101,
            end_date=20240131,
        )
        self.assertEqual(
            put_pairs,
            [("110000.SH", "info:get_kzz_put_explanation:110000.SH:None:None", "get_kzz_put_explanation")],
        )

    def test_build_resume_scope_pairs_for_option_basic_info(self) -> None:
        context = self._build_context()

        pairs = build_resume_scope_pairs(
            context=context,
            task="option_basic_info",
            code_list=["10000001.SH"],
            begin_date=20240101,
            end_date=20240131,
        )

        self.assertEqual(
            pairs,
            [("10000001.SH", "info:get_option_basic_info:10000001.SH:None:None", "get_option_basic_info")],
        )

        std_pairs = build_resume_scope_pairs(
            context=context,
            task="option_std_ctr_specs",
            code_list=["510050.SH"],
            begin_date=20240101,
            end_date=20240131,
        )

        self.assertEqual(
            std_pairs,
            [("510050.SH", "info:get_option_std_ctr_specs:510050.SH:None:None", "get_option_std_ctr_specs")],
        )

        mon_pairs = build_resume_scope_pairs(
            context=context,
            task="option_mon_ctr_specs",
            code_list=["510050.SH"],
            begin_date=20240101,
            end_date=20240131,
        )

        self.assertEqual(
            mon_pairs,
            [("510050.SH", "info:get_option_mon_ctr_specs:510050.SH:None:None", "get_option_mon_ctr_specs")],
        )

    def test_option_basic_info_can_auto_resolve_codes_from_option_code_list(self) -> None:
        context = self._build_context()

        result = resolve_code_list(
            base_data=context.base_data,
            task="option_basic_info",
            raw_codes="",
            limit=0,
            local_path="/tmp/amazing_data_cache",
            end_date=20240131,
        )

        self.assertEqual(result, ["10000001.SH", "10000002.SH"])
        self.assertEqual(
            context.base_data.option_code_calls,
            [("EXTRA_ETF_OP", False)],
        )

    def test_treasury_yield_defaults_terms_and_builds_resume_scope(self) -> None:
        context = self._build_context()

        result = resolve_code_list(
            base_data=context.base_data,
            task="treasury_yield",
            raw_codes="",
            limit=0,
        )

        self.assertEqual(result, ["m3", "m6", "y1", "y10", "y2", "y20", "y3", "y30", "y5", "y7"])

    def test_daily_kline_uses_stock_index_etf_universes(self) -> None:
        context = self._build_context()

        result = resolve_code_list(
            base_data=context.base_data,
            task="daily_kline",
            raw_codes="",
            limit=0,
        )

        self.assertEqual(result, ["000001.SZ", "600000.SH", "000300.SH", "510300.SH"])
        self.assertEqual(context.base_data.stock_universe_calls, [("EXTRA_STOCK_A", False)])
        self.assertEqual(context.base_data.index_universe_calls, [("EXTRA_INDEX_A", False)])
        self.assertEqual(context.base_data.etf_universe_calls, [("EXTRA_ETF", False)])

        pairs = build_resume_scope_pairs(
            context=context,
            task="treasury_yield",
            code_list=["m3", "y10"],
            begin_date=20240101,
            end_date=20240131,
        )

        self.assertEqual(
            pairs,
            [
                ("m3", "info:get_treasury_yield:m3:2024-01-01:2024-01-31", "get_treasury_yield"),
                ("y10", "info:get_treasury_yield:y10:2024-01-01:2024-01-31", "get_treasury_yield"),
            ],
        )

    def test_default_config_resume_applies_to_all_tasks(self) -> None:
        args = SimpleNamespace(
            task=None,
            config=None,
            runtime_path=None,
            codes="",
            begin_date=None,
            end_date=None,
            limit=0,
            force=False,
            resume=True,
            log_level=None,
        )

        with patch("sync_data_system.run_sync.DEFAULT_PLAN_CONFIG", "run_sync.example.toml"):
            plan = build_execution_plan(args)

        self.assertTrue(plan.tasks)
        self.assertTrue(all(task.resume for task in plan.tasks))


if __name__ == "__main__":
    unittest.main()
