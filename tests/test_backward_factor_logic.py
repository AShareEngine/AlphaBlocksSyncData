#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from datetime import date

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

from sync_data_system.amazingdata_constants import FactorType
from sync_data_system.amazingdata_sdk_provider import _normalize_single_price_factor_frame
from sync_data_system.base_data import BaseData
from sync_data_system.data_models import PriceFactorRow


class _FakeRepository:
    def __init__(self) -> None:
        self.saved_adj_batches: list[list[PriceFactorRow]] = []
        self.saved_backward_batches: list[list[PriceFactorRow]] = []
        self.saved_codes: set[str] = set()
        self.sync_logs = []

    def load_sync_checkpoint_date(self, task_name: str, scope_key: str):
        return None

    def has_successful_sync_today(self, task_name: str, scope_key: str, run_date: date) -> bool:
        return False

    def save_adj_factor_rows(self, rows) -> int:
        batch = list(rows)
        self.saved_adj_batches.append(batch)
        self.saved_codes.update(row.code for row in batch)
        return len(batch)

    def save_backward_factor_rows(self, rows) -> int:
        batch = list(rows)
        self.saved_backward_batches.append(batch)
        self.saved_codes.update(row.code for row in batch)
        return len(batch)

    def load_existing_codes(self, table_name: str, code_list):
        return {code for code in code_list if code in self.saved_codes}

    def insert_sync_log(self, row) -> None:
        self.sync_logs.append(row)


class _BufferedSpyBaseData(BaseData):
    def __init__(self) -> None:
        super().__init__(repository=_FakeRepository(), sync_provider=object())
        self.provider_calls: list[tuple[str, list[str], object]] = []

    def _validate_local_path(self, local_path: str) -> None:
        return None

    def _provider_fetch_price_factor(self, factor_type: str, code_list, start_date):
        self.provider_calls.append((factor_type, list(code_list), start_date))
        code = list(code_list)[0]
        yield PriceFactorRow(
            trade_date=date(2024, 1, 2),
            code=code,
            factor_value=1.0,
        )


class BackwardFactorSyncTest(unittest.TestCase):
    def test_sync_adj_factor_requests_one_code_at_a_time_but_saves_in_one_batch(self) -> None:
        base_data = _BufferedSpyBaseData()

        inserted = base_data.sync_adj_factor(
            code_list=["600000.SH", "600004.SH"],
            local_path="/tmp/amazing_data_cache",
            force=True,
        )

        self.assertEqual(inserted, 2)
        self.assertEqual(
            base_data.provider_calls,
            [
                (FactorType.ADJ, ["600000.SH"], None),
                (FactorType.ADJ, ["600004.SH"], None),
            ],
        )
        self.assertEqual(len(base_data.repository.saved_adj_batches), 1)
        self.assertEqual(
            [row.code for row in base_data.repository.saved_adj_batches[0]],
            ["600000.SH", "600004.SH"],
        )

    def test_sync_backward_factor_requests_one_code_at_a_time_but_saves_in_one_batch(self) -> None:
        base_data = _BufferedSpyBaseData()

        inserted = base_data.sync_backward_factor(
            code_list=["600000.SH", "600004.SH"],
            local_path="/tmp/amazing_data_cache",
            force=True,
        )

        self.assertEqual(inserted, 2)
        self.assertEqual(
            base_data.provider_calls,
            [
                (FactorType.BACKWARD, ["600000.SH"], None),
                (FactorType.BACKWARD, ["600004.SH"], None),
            ],
        )
        self.assertEqual(len(base_data.repository.saved_backward_batches), 1)
        self.assertEqual(
            [row.code for row in base_data.repository.saved_backward_batches[0]],
            ["600000.SH", "600004.SH"],
        )


@unittest.skipIf(pd is None, "pandas is required")
class BackwardFactorFrameNormalizeTest(unittest.TestCase):
    def test_normalize_single_backward_factor_transposes_when_code_is_on_index(self) -> None:
        frame = pd.DataFrame(
            [[1.0, 1.1]],
            index=["600000.SH"],
            columns=["2024-01-02", "2024-01-03"],
        )

        normalized = _normalize_single_price_factor_frame(frame, "600000.SH")

        self.assertEqual(list(normalized.columns), ["600000.SH"])
        self.assertEqual(list(normalized.index.astype(str)), ["2024-01-02", "2024-01-03"])

    def test_normalize_single_backward_factor_does_not_relabel_unknown_column(self) -> None:
        frame = pd.DataFrame({"688698.SH": [1.0, 1.1]}, index=["2024-01-02", "2024-01-03"])

        normalized = _normalize_single_price_factor_frame(frame, "600000.SH")

        self.assertEqual(list(normalized.columns), ["688698.SH"])


if __name__ == "__main__":
    unittest.main()
