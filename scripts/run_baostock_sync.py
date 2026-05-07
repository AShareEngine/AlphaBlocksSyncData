#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BaoStock 同步脚本入口."""

from __future__ import annotations

from sync_data_system.sources.baostock.runner import main


if __name__ == "__main__":
    raise SystemExit(main())
