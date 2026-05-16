#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import unittest
from types import SimpleNamespace

from sync_data_system.scripts.install_provider_deps import (
    build_pip_command,
    dedupe_dependencies,
    dedupe_import_modules,
    requirement_package_name,
)


class InstallProviderDepsTest(unittest.TestCase):
    def test_requirement_package_name_handles_version_specifiers(self) -> None:
        self.assertEqual(requirement_package_name("clickhouse-connect>=0.8"), "clickhouse-connect")
        self.assertEqual(requirement_package_name("pandas"), "pandas")
        self.assertEqual(requirement_package_name("my_pkg[extra]>=1.0"), "my-pkg")

    def test_dedupe_dependencies_preserves_order(self) -> None:
        manifests = [
            SimpleNamespace(dependencies=("pandas", "clickhouse-connect")),
            SimpleNamespace(dependencies=("pandas", "baostock")),
        ]

        self.assertEqual(dedupe_dependencies(manifests), ["pandas", "clickhouse-connect", "baostock"])

    def test_build_pip_command(self) -> None:
        self.assertEqual(
            build_pip_command("/usr/bin/python", ["pandas", "baostock"], upgrade=True),
            ["/usr/bin/python", "-m", "pip", "install", "--upgrade", "pandas", "baostock"],
        )
        self.assertEqual(build_pip_command("/usr/bin/python", [], upgrade=False), [])

    def test_dedupe_import_modules_preserves_order(self) -> None:
        manifests = [
            SimpleNamespace(import_modules=("AmazingData", "baostock")),
            SimpleNamespace(import_modules=("baostock", "clickhouse_connect")),
        ]

        self.assertEqual(dedupe_import_modules(manifests), ["AmazingData", "baostock", "clickhouse_connect"])


if __name__ == "__main__":
    unittest.main()
