#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData runner 兼容导出层.

当前正式同步入口仍然是仓库根目录的 `run_sync.py`，
这里先提供统一的包路径，便于多数据源结构收敛。
"""

from sync_data_system.run_sync import *  # noqa: F401,F403
