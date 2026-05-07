#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AmazingData 数据源命名空间.

当前阶段先把仓库根目录实现通过包层归一到 `sources/amazingdata/`，
后续再逐步把真实实现文件迁移进来。
"""

from .base import *  # noqa: F401,F403
from .constants import *  # noqa: F401,F403
from .info import *  # noqa: F401,F403
from .market import *  # noqa: F401,F403
from .provider import *  # noqa: F401,F403
from .runner import *  # noqa: F401,F403

