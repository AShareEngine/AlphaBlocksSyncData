#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest

from sync_data_system.service.table_watermark import (
    TableWatermark,
    TableWatermarkRepository,
    is_current_watermark,
    source_part_state,
)


class _FakeClient:
    def __init__(self) -> None:
        self.commands = []
        self.queries = []
        self.inserts = []

    def command(self, sql, parameters=None):
        self.commands.append(sql)

    def query_rows(self, sql, parameters=None):
        self.queries.append((sql, parameters))
        return [
            (
                "market",
                "daily",
                "trade_date",
                "2026-07-29",
                1,
                "2026-07-29 18:00:00",
                "signature-1",
                "2026-07-29 18:01:00",
            )
        ]

    def insert_rows(self, table, column_names, rows):
        self.inserts.append((table, tuple(column_names), list(rows)))


class TableWatermarkRepositoryTest(unittest.TestCase):
    def test_table_uses_complete_key_and_explicit_replacing_version(self) -> None:
        client = _FakeClient()
        repository = TableWatermarkRepository(client)

        repository.ensure_table()

        ddl = "\n".join(client.commands)
        self.assertIn("`alphablocks`.`sync_table_watermark`", ddl)
        self.assertIn("ReplacingMergeTree(_version)", ddl)
        self.assertIn("ORDER BY (source_database, source_table)", ddl)

    def test_load_uses_argmax_and_save_writes_one_versioned_batch(self) -> None:
        client = _FakeClient()
        repository = TableWatermarkRepository(client)

        loaded = repository.load([("market", "daily")])
        saved = repository.save(
            [
                TableWatermark(
                    source_database="market",
                    source_table="daily",
                    latest_field="trade_date",
                    latest_date="2026-07-30",
                    has_data=True,
                    source_last_update_time="2026-07-30 18:00:00",
                    source_signature="signature-2",
                )
            ]
        )

        self.assertEqual(loaded[("market", "daily")].latest_date, "2026-07-29")
        self.assertIn("argMax(latest_date, _version)", client.queries[0][0])
        self.assertEqual(saved, 1)
        self.assertEqual(client.inserts[0][0], "`alphablocks`.`sync_table_watermark`")
        self.assertGreater(client.inserts[0][2][0][-1], 0)

    def test_source_signature_controls_cache_validity(self) -> None:
        watermark = TableWatermark(
            source_database="market",
            source_table="daily",
            latest_field="trade_date",
            latest_date="2026-07-29",
            has_data=True,
            source_last_update_time="2026-07-29 18:00:00",
            source_signature="signature-1",
        )

        self.assertTrue(
            is_current_watermark(
                watermark,
                latest_field="trade_date",
                has_data=True,
                source_signature="signature-1",
            )
        )
        self.assertFalse(
            is_current_watermark(
                watermark,
                latest_field="trade_date",
                has_data=True,
                source_signature="signature-2",
            )
        )
        self.assertEqual(
            source_part_state(("market", "daily", 1, "time", "signature-1")),
            (True, "time", "signature-1"),
        )


if __name__ == "__main__":
    unittest.main()
