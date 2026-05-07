#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BaoStock 同步脚本入口."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from program_bootstrap import install_sync_data_system_alias

install_sync_data_system_alias(PROJECT_ROOT)

from sync_data_system.sources.baostock.runner import main


if __name__ == "__main__":
    raise SystemExit(main())
