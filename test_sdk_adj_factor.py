#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""直接测试 AmazingData SDK 的 adj_factor / backward_factor 行为.

这个脚本故意不走当前项目的同步封装层，只做最小调用：
1. 登录 AmazingData
2. 获取代码列表
3. 调用 `get_adj_factor` 或 `get_backward_factor`

适合用来判断：
- SDK 在 `is_local` 不同取值下的行为
- `local_path` 是否会被 SDK 自己清理 / 重建
"""

from __future__ import annotations

import argparse
from pathlib import Path

from sync_data_system.amazingdata_sdk_provider import AmazingDataSDKConfig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="直接测试 AmazingData SDK 的 get_adj_factor / get_backward_factor")
    parser.add_argument("--runtime-path", default=None, help="默认读取外层 config/runtime.local.yaml")
    parser.add_argument(
        "--local-path",
        default=None,
        help="SDK 本地缓存目录",
    )
    parser.add_argument(
        "--security-type",
        default="EXTRA_STOCK_A",
        help="默认测试 A 股代码池",
    )
    parser.add_argument(
        "--factor-type",
        choices=["adj", "backward"],
        default="adj",
        help="选择测试前复权还是后复权，默认 adj",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="限制测试代码数量，默认 20；传 0 表示全部",
    )
    parser.add_argument(
        "--is-local",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="是否传 is_local=True，默认 False",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = AmazingDataSDKConfig.from_env(
        runtime_path=args.runtime_path,
        local_path=args.local_path,
    )
    local_path = str(Path(cfg.local_path.replace("//", "/")).resolve())

    import AmazingData as ad

    ok = ad.login(
        username=cfg.username,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
    )
    if ok is False:
        raise RuntimeError("AmazingData 登录失败")

    base_data_object = ad.BaseData()
    code_list = base_data_object.get_code_list(security_type=args.security_type)
    if args.limit and args.limit > 0:
        code_list = list(code_list)[: args.limit]

    print(f"security_type={args.security_type}")
    print(f"factor_type={args.factor_type}")
    print(f"code_count={len(code_list)}")
    print(f"local_path={local_path}")
    print(f"is_local={args.is_local}")
    print(f"runtime_path={args.runtime_path}")

    if args.factor_type == "adj":
        factor = base_data_object.get_adj_factor(
            code_list,
            local_path=local_path,
            is_local=args.is_local,
        )
    else:
        factor = base_data_object.get_backward_factor(
            code_list,
            local_path=local_path,
            is_local=args.is_local,
        )
    print(factor)
    print(f"result_type={type(factor).__name__}")
    if hasattr(factor, "shape"):
        print(f"shape={factor.shape}")
    if hasattr(factor, "columns"):
        print("columns_preview=", list(factor.columns))
    if hasattr(factor, "head"):
        print(factor.head())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
