#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""正式同步入口.

当前版本只服务一条主线：
- 只同步 `EXTRA_STOCK_A`
- 不做批量调度，直接逐股顺序同步

正式任务：
- `code_info`
- `hist_code_list`
- `stock_basic`
- `history_stock_status`
- `adj_factor`
- `backward_factor`
- `balance_sheet`
- `cash_flow`
- `income`
- `profit_express`
- `profit_notice`
- `share_holder`
- `holder_num`
- `equity_structure`
- `equity_pledge_freeze`
- `equity_restricted`
- `dividend`
- `right_issue`
- `index_constituent`
- `index_weight`
- `industry_base_info`
- `industry_constituent`
- `industry_weight`
- `industry_daily`
- `etf_pcf`
- `fund_share`
- `kzz_conv_change`
- `kzz_corr`
- `kzz_call_explanation`
- `kzz_put_call_item`
- `kzz_put`
- `kzz_call`
- `kzz_conv`
- `block_trading`
- `long_hu_bang`
- `margin_detail`
- `margin_summary`
- `daily_kline`
- `minute_kline`
- `market_snapshot`
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from sync_data_system.amazingdata_constants import FactorType, PeriodName, SecurityType
from sync_data_system.amazingdata_sdk_provider import AmazingDataSDKConfig, AmazingDataSDKProvider
from sync_data_system.base_data import BaseData, BaseDataCacheMissError
from sync_data_system.clickhouse_client import ClickHouseConfig
from sync_data_system.config_paths import resolve_config_candidate, resolve_runtime_config_path
from sync_data_system.data_models import normalize_code_list, to_ch_date
from sync_data_system.info_data import InfoData
from sync_data_system.market_data import MarketData
from sync_data_system.sources.baostock.runner import run_config_file as run_baostock_config_file
from sync_data_system.toml_compat import tomllib


logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_FULL_SYNC_BEGIN_DATE = 20100101
DEFAULT_SYNC_SECURITY_TYPE = SecurityType.EXTRA_STOCK_A
DEFAULT_INDEX_SYNC_SECURITY_TYPE = SecurityType.EXTRA_INDEX_A_SH_SZ
DEFAULT_ETF_SYNC_SECURITY_TYPE = SecurityType.EXTRA_ETF
DEFAULT_KZZ_SYNC_SECURITY_TYPE = SecurityType.EXTRA_KZZ
DEFAULT_RUNTIME_PATH = str(resolve_runtime_config_path(PROJECT_ROOT))
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_PLAN_CONFIG = "run_sync.full.toml"
OPTION_SYNC_CODE_BATCH_SIZE = 500
DEFAULT_OPTION_HIST_BEGIN_DATE = 20130101
DEFAULT_OPTION_STD_UNDERLYING_CODES = (
    "159919.SZ",
    "159915.SZ",
    "159922.SZ",
    "159901.SZ",
    "510300.SH",
    "588000.SH",
    "588080.SH",
    "510050.SH",
    "510500.SH",
)
DEFAULT_TREASURY_TERMS = (
    "m3",
    "m6",
    "y1",
    "y2",
    "y3",
    "y5",
    "y7",
    "y10",
    "y20",
    "y30",
)

TASK_CHOICES = (
    "code_info",
    "hist_code_list",
    "bj_code_mapping",
    "stock_basic",
    "history_stock_status",
    "adj_factor",
    "backward_factor",
    "balance_sheet",
    "cash_flow",
    "income",
    "profit_express",
    "profit_notice",
    "share_holder",
    "holder_num",
    "equity_structure",
    "equity_pledge_freeze",
    "equity_restricted",
    "dividend",
    "right_issue",
    "index_constituent",
    "index_weight",
    "industry_base_info",
    "industry_constituent",
    "industry_weight",
    "industry_daily",
    "etf_pcf",
    "fund_share",
    "fund_iopv",
    "kzz_issuance",
    "kzz_share",
    "kzz_suspend",
    "kzz_put_call_item",
    "kzz_put",
    "kzz_call",
    "kzz_conv",
    "option_basic_info",
    "option_std_ctr_specs",
    "option_mon_ctr_specs",
    "treasury_yield",
    "kzz_conv_change",
    "kzz_corr",
    "kzz_call_explanation",
    "kzz_put_explanation",
    "block_trading",
    "long_hu_bang",
    "margin_detail",
    "margin_summary",
    "daily_kline",
    "minute_kline",
    "market_snapshot",
)

TASK_TARGET_TABLE_MAP = {
    "code_info": "ad_code_info",
    "hist_code_list": "ad_hist_code_daily",
    "bj_code_mapping": "ad_bj_code_mapping",
    "stock_basic": "ad_stock_basic",
    "history_stock_status": "ad_history_stock_status",
    "adj_factor": "ad_adj_factor",
    "backward_factor": "ad_backward_factor",
    "balance_sheet": "ad_balance_sheet",
    "cash_flow": "ad_cash_flow",
    "income": "ad_income",
    "profit_express": "ad_profit_express",
    "profit_notice": "ad_profit_notice",
    "share_holder": "ad_share_holder",
    "holder_num": "ad_holder_num",
    "equity_structure": "ad_equity_structure",
    "equity_pledge_freeze": "ad_equity_pledge_freeze",
    "equity_restricted": "ad_equity_restricted",
    "dividend": "ad_dividend",
    "right_issue": "ad_right_issue",
    "index_constituent": "ad_index_constituent",
    "index_weight": "ad_index_weight",
    "industry_base_info": "ad_industry_base_info",
    "industry_constituent": "ad_industry_constituent",
    "industry_weight": "ad_industry_weight",
    "industry_daily": "ad_industry_daily",
    "etf_pcf": "ad_etf_pcf",
    "fund_share": "ad_fund_share",
    "fund_iopv": "ad_fund_iopv",
    "kzz_issuance": "ad_kzz_issuance",
    "kzz_share": "ad_kzz_share",
    "kzz_suspend": "ad_kzz_suspend",
    "kzz_put_call_item": "ad_kzz_put_call_item",
    "kzz_put": "ad_kzz_put",
    "kzz_call": "ad_kzz_call",
    "kzz_conv": "ad_kzz_conv",
    "option_basic_info": "ad_option_basic_info",
    "option_std_ctr_specs": "ad_option_std_ctr_specs",
    "option_mon_ctr_specs": "ad_option_mon_ctr_specs",
    "treasury_yield": "ad_treasury_yield",
    "kzz_conv_change": "ad_kzz_conv_change",
    "kzz_corr": "ad_kzz_corr",
    "kzz_call_explanation": "ad_kzz_call_explanation",
    "kzz_put_explanation": "ad_kzz_put_explanation",
    "block_trading": "ad_block_trading",
    "long_hu_bang": "ad_long_hu_bang",
    "margin_detail": "ad_margin_detail",
    "margin_summary": "ad_margin_summary",
    "daily_kline": "ad_market_kline_daily",
    "minute_kline": "ad_market_kline_minute",
    "market_snapshot": "ad_market_snapshot",
}

TASK_INPUT_RESOLVER_MAP = {
    "daily_kline": "market_kline_defaults",
    "minute_kline": "market_kline_defaults",
}

TASK_CONFIG_KEYS = frozenset(
    {
        "task",
        "enabled",
        "codes",
        "begin_date",
        "end_date",
        "limit",
        "force",
        "resume",
    }
)
DEFAULT_CONFIG_KEYS = frozenset(
    {
        "codes",
        "begin_date",
        "end_date",
        "limit",
        "force",
        "resume",
    }
)


@dataclass(frozen=True)
class TaskRunSpec:
    task: str
    codes_raw: str = ""
    begin_date: int | None = None
    end_date: int | None = None
    limit: int = 0
    force: bool = False
    resume: bool = False


@dataclass(frozen=True)
class ExecutionPlan:
    runtime_path: str
    log_level: str
    continue_on_error: bool
    tasks: tuple[TaskRunSpec, ...]


@dataclass(frozen=True)
class SyncExecutionContext:
    sdk_config: AmazingDataSDKConfig
    provider: AmazingDataSDKProvider
    base_data: BaseData
    info_data: InfoData
    market_data: MarketData


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AmazingData 正式同步入口")
    parser.add_argument(
        "task",
        nargs="?",
        choices=TASK_CHOICES,
    )
    parser.add_argument("--config", help="TOML 配置文件路径；传入后按配置中的 tasks 列表顺序执行")
    parser.add_argument("--runtime-path", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--codes", default="", help="逗号分隔的证券代码列表；不传则自动从代码池获取")
    parser.add_argument("--begin-date", type=int, help="开始日期 YYYYMMDD；默认 20100101")
    parser.add_argument("--end-date", type=int, help="结束日期 YYYYMMDD；默认最新交易日")
    parser.add_argument("--limit", type=int, default=0, help="调试时限制同步证券数量，0 表示不限制")
    parser.add_argument("--force", action="store_true", help="忽略当天成功跳过逻辑，强制同步")
    parser.add_argument("--resume", action="store_true", help="按成功 checkpoint 跳过已完成 code，从未完成部分继续")
    parser.add_argument("--log-level", default=None)
    args = parser.parse_args()
    if args.task and args.config:
        parser.error("task 和 --config 不能同时使用。")
    if args.config and (
        args.codes.strip()
        or args.begin_date is not None
        or args.end_date is not None
        or args.limit != 0
        or args.force
        or args.resume
    ):
        parser.error("--config 模式下不要再混用 --codes/--begin-date/--end-date/--limit/--force/--resume。")
    return args


def build_execution_plan(args: argparse.Namespace) -> ExecutionPlan:
    if not args.task and not args.config:
        default_plan_path = _resolve_project_path(DEFAULT_PLAN_CONFIG)
        if not default_plan_path.is_file():
            raise FileNotFoundError(
                f"未提供 task 或 --config，且默认全量配置文件不存在: {default_plan_path}"
            )
        logger.info("未提供 task，默认按全量配置执行 config=%s", default_plan_path)
        args.config = str(default_plan_path)

    if args.config:
        config_plan = load_execution_plan_from_toml(args.config)
        tasks = config_plan.tasks
        if args.resume:
            tasks = tuple(replace(task, resume=True) for task in tasks)
        return ExecutionPlan(
            runtime_path=args.runtime_path or config_plan.runtime_path,
            log_level=args.log_level or config_plan.log_level,
            continue_on_error=config_plan.continue_on_error,
            tasks=tasks,
        )

    if args.task is None:
        raise ValueError("未提供 task。")

    return ExecutionPlan(
        runtime_path=args.runtime_path or DEFAULT_RUNTIME_PATH,
        log_level=args.log_level or DEFAULT_LOG_LEVEL,
        continue_on_error=False,
        tasks=(
            TaskRunSpec(
                task=args.task,
                codes_raw=args.codes,
                begin_date=args.begin_date,
                end_date=args.end_date,
                limit=args.limit,
                force=args.force,
                resume=args.resume,
            ),
        ),
    )


def load_execution_plan_from_toml(path: str) -> ExecutionPlan:
    config_path = _resolve_project_path(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")

    with config_path.open("rb") as fh:
        data = tomllib.load(fh)

    if not isinstance(data, dict):
        raise ValueError("配置文件格式错误：顶层必须是 TOML table。")

    unexpected_top_level_keys = set(data.keys()) - {"source", "runtime_path", "log_level", "continue_on_error", "defaults", "tasks"}
    if unexpected_top_level_keys:
        raise ValueError(f"配置文件存在未知顶层字段: {sorted(unexpected_top_level_keys)}")

    source = str(data.get("source") or "amazingdata").strip() or "amazingdata"
    if source != "amazingdata":
        raise ValueError(f"AmazingData 配置文件 source 必须是 'amazingdata'，当前值: {source!r}")

    defaults = data.get("defaults", {})
    if defaults is None:
        defaults = {}
    if not isinstance(defaults, dict):
        raise ValueError("[defaults] 必须是 TOML table。")

    unexpected_default_keys = set(defaults.keys()) - DEFAULT_CONFIG_KEYS
    if unexpected_default_keys:
        raise ValueError(f"[defaults] 存在未知字段: {sorted(unexpected_default_keys)}")

    raw_tasks = data.get("tasks")
    if not isinstance(raw_tasks, list) or not raw_tasks:
        raise ValueError("配置文件至少需要一个 [[tasks]]。")

    task_specs: list[TaskRunSpec] = []
    for index, raw_task in enumerate(raw_tasks, start=1):
        if not isinstance(raw_task, dict):
            raise ValueError(f"tasks[{index}] 必须是 TOML table。")
        unexpected_task_keys = set(raw_task.keys()) - TASK_CONFIG_KEYS
        if unexpected_task_keys:
            raise ValueError(f"tasks[{index}] 存在未知字段: {sorted(unexpected_task_keys)}")
        if not _as_bool(raw_task.get("enabled", True), field_name=f"tasks[{index}].enabled"):
            continue

        merged = dict(defaults)
        merged.update(raw_task)
        task_name = str(merged.get("task", "") or "").strip()
        if task_name not in TASK_CHOICES:
            raise ValueError(f"tasks[{index}].task 非法: {task_name!r}")

        task_specs.append(
            TaskRunSpec(
                task=task_name,
                codes_raw=_normalize_config_codes(merged.get("codes"), field_name=f"tasks[{index}].codes"),
                begin_date=_as_optional_int(merged.get("begin_date"), field_name=f"tasks[{index}].begin_date"),
                end_date=_as_optional_int(merged.get("end_date"), field_name=f"tasks[{index}].end_date"),
                limit=_as_non_negative_int(merged.get("limit", 0), field_name=f"tasks[{index}].limit"),
                force=_as_bool(merged.get("force", False), field_name=f"tasks[{index}].force"),
                resume=_as_bool(merged.get("resume", False), field_name=f"tasks[{index}].resume"),
            )
        )

    if not task_specs:
        raise ValueError("配置文件中的 [[tasks]] 全部被禁用，无法执行。")

    return ExecutionPlan(
        runtime_path=str(data.get("runtime_path") or DEFAULT_RUNTIME_PATH),
        log_level=str(data.get("log_level") or DEFAULT_LOG_LEVEL),
        continue_on_error=_as_bool(data.get("continue_on_error", False), field_name="continue_on_error"),
        tasks=tuple(task_specs),
    )


def _resolve_project_path(path_like: str | Path) -> Path:
    return resolve_config_candidate(path_like, project_root=PROJECT_ROOT)


def _normalize_config_codes(value: Any, field_name: str) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        codes = [str(item).strip() for item in value if str(item).strip()]
        return ",".join(codes)
    raise ValueError(f"{field_name} 必须是字符串或字符串数组。")


def _as_optional_int(value: Any, field_name: str) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception as exc:
        raise ValueError(f"{field_name} 必须是整数。") from exc


def _as_non_negative_int(value: Any, field_name: str) -> int:
    try:
        result = int(value)
    except Exception as exc:
        raise ValueError(f"{field_name} 必须是整数。") from exc
    if result < 0:
        raise ValueError(f"{field_name} 不能小于 0。")
    return result


def _as_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ValueError(f"{field_name} 必须是布尔值。")


def main() -> int:
    args = parse_args()
    if args.config and detect_config_source(args.config) == "baostock":
        if args.task:
            raise ValueError("BaoStock 配置模式下不要再额外传 task。")
        if args.resume:
            logger.warning("BaoStock 配置模式当前不支持 --resume，参数将被忽略。")
        return run_baostock_config_file(args.config, log_level_override=args.log_level)

    plan = build_execution_plan(args)
    logging.basicConfig(
        level=getattr(logging, str(plan.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    sdk_config = AmazingDataSDKConfig.from_env(runtime_path=plan.runtime_path)
    clickhouse_config = ClickHouseConfig.from_env(runtime_path=plan.runtime_path)
    provider = AmazingDataSDKProvider(sdk_config)

    base_data = None
    info_data = None
    market_data = None
    try:
        base_data = BaseData.from_clickhouse_config(clickhouse_config, sync_provider=provider)
        info_data = InfoData.from_clickhouse_config(clickhouse_config, sync_provider=provider)
        market_data = MarketData.from_clickhouse_config(clickhouse_config, sync_provider=provider)
        context = SyncExecutionContext(
            sdk_config=sdk_config,
            provider=provider,
            base_data=base_data,
            info_data=info_data,
            market_data=market_data,
        )
        failed_tasks: list[str] = []
        for index, task_spec in enumerate(plan.tasks, start=1):
            logger.info(
                "batch task start progress=%s/%s task=%s",
                index,
                len(plan.tasks),
                task_spec.task,
            )
            try:
                execute_task_spec(context, task_spec)
            except Exception:
                failed_tasks.append(task_spec.task)
                logger.exception("batch task failed progress=%s/%s task=%s", index, len(plan.tasks), task_spec.task)
                if not plan.continue_on_error:
                    raise
            else:
                logger.info(
                    "batch task finished progress=%s/%s task=%s",
                    index,
                    len(plan.tasks),
                    task_spec.task,
                )
        return 1 if failed_tasks else 0
    finally:
        if market_data is not None:
            try:
                market_data.close()
            except Exception:
                pass
        if info_data is not None:
            try:
                info_data.close()
            except Exception:
                pass
        if base_data is not None:
            try:
                base_data.close()
            except Exception:
                pass
        provider.close()


def detect_config_source(path: str) -> str:
    config_path = _resolve_project_path(path)
    if not config_path.is_file():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    with config_path.open("rb") as fh:
        data = tomllib.load(fh)
    if not isinstance(data, dict):
        raise ValueError("配置文件格式错误：顶层必须是 TOML table。")
    return str(data.get("source") or "amazingdata").strip() or "amazingdata"


def execute_task_spec(context: SyncExecutionContext, task_spec: TaskRunSpec) -> int:
    ignores_date_range = task_ignores_date_range(task_spec.task)
    begin_date, end_date = resolve_date_window(
        provider=context.provider,
        begin_date=None if ignores_date_range else task_spec.begin_date,
        end_date=None if ignores_date_range else task_spec.end_date,
    )
    display_begin_date = "N/A" if ignores_date_range else begin_date
    display_end_date = "N/A" if ignores_date_range else end_date
    code_list: list[str] = []
    if task_requires_code_list(task_spec.task):
        if task_spec.task == "backward_factor":
            code_list = resolve_backward_factor_code_list(
                base_data=context.base_data,
                raw_codes=task_spec.codes_raw,
                limit=task_spec.limit,
            )
        elif task_spec.task in {"industry_constituent", "industry_weight", "industry_daily"}:
            code_list = resolve_industry_code_list(
                info_data=context.info_data,
                raw_codes=task_spec.codes_raw,
                limit=task_spec.limit,
            )
        else:
            code_list = resolve_code_list(
                base_data=context.base_data,
                task=task_spec.task,
                raw_codes=task_spec.codes_raw,
                limit=task_spec.limit,
                local_path=context.sdk_config.local_path,
                end_date=end_date,
            )
        code_list = filter_code_list_for_resume(
            context=context,
            task_spec=task_spec,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
        )

    warn_ignored_task_params(task_spec)

    logger.info(
        "sync task=%s total_code_count=%s security_type=%s begin_date=%s end_date=%s mode=%s",
        task_spec.task,
        len(code_list),
        resolve_task_security_type(task_spec.task),
        display_begin_date,
        display_end_date,
        resolve_task_run_mode(task_spec.task),
    )

    if task_spec.task == "code_info":
        return run_code_info(base_data=context.base_data, force=task_spec.force)
    if task_spec.task == "hist_code_list":
        return run_hist_code_list(
            base_data=context.base_data,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "bj_code_mapping":
        return run_bj_code_mapping(info_data=context.info_data, force=task_spec.force)
    if task_spec.task == "stock_basic":
        return run_stock_basic(info_data=context.info_data, code_list=code_list, force=task_spec.force)
    if task_spec.task == "history_stock_status":
        return run_history_stock_status(
            info_data=context.info_data,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "adj_factor":
        return run_adj_factor(
            base_data=context.base_data,
            code_list=code_list,
            force=task_spec.force,
            local_path=context.sdk_config.local_path,
        )
    if task_spec.task == "backward_factor":
        return run_backward_factor(
            base_data=context.base_data,
            code_list=code_list,
            force=task_spec.force,
            local_path=context.sdk_config.local_path,
        )
    if task_spec.task == "balance_sheet":
        return run_info_payload_task(
            task_name="balance_sheet",
            sync_fn=context.info_data.sync_balance_sheet,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "cash_flow":
        return run_info_payload_task(
            task_name="cash_flow",
            sync_fn=context.info_data.sync_cash_flow,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "income":
        return run_info_payload_task(
            task_name="income",
            sync_fn=context.info_data.sync_income,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "profit_express":
        return run_info_payload_task(
            task_name="profit_express",
            sync_fn=context.info_data.sync_profit_express,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "profit_notice":
        return run_info_payload_task(
            task_name="profit_notice",
            sync_fn=context.info_data.sync_profit_notice,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "share_holder":
        return run_info_payload_task(
            task_name="share_holder",
            sync_fn=context.info_data.sync_share_holder,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "holder_num":
        return run_info_payload_task(
            task_name="holder_num",
            sync_fn=context.info_data.sync_holder_num,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "equity_structure":
        return run_info_payload_task(
            task_name="equity_structure",
            sync_fn=context.info_data.sync_equity_structure,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "equity_pledge_freeze":
        return run_info_payload_task(
            task_name="equity_pledge_freeze",
            sync_fn=context.info_data.sync_equity_pledge_freeze,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "equity_restricted":
        return run_info_payload_task(
            task_name="equity_restricted",
            sync_fn=context.info_data.sync_equity_restricted,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "dividend":
        return run_info_payload_task(
            task_name="dividend",
            sync_fn=context.info_data.sync_dividend,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "right_issue":
        return run_info_payload_task(
            task_name="right_issue",
            sync_fn=context.info_data.sync_right_issue,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "index_constituent":
        return run_index_constituent(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "index_weight":
        return run_info_payload_task(
            task_name="index_weight",
            sync_fn=context.info_data.sync_index_weight,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "industry_base_info":
        return run_industry_base_info(info_data=context.info_data, force=task_spec.force)
    if task_spec.task == "industry_constituent":
        return run_industry_constituent(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "industry_weight":
        return run_info_payload_task(
            task_name="industry_weight",
            sync_fn=context.info_data.sync_industry_weight,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "industry_daily":
        return run_info_payload_task(
            task_name="industry_daily",
            sync_fn=context.info_data.sync_industry_daily,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "etf_pcf":
        return run_etf_pcf(
            base_data=context.base_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "fund_share":
        return run_info_payload_task(
            task_name="fund_share",
            sync_fn=context.info_data.sync_fund_share,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "fund_iopv":
        return run_info_payload_task(
            task_name="fund_iopv",
            sync_fn=context.info_data.sync_fund_iopv,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "kzz_issuance":
        return run_kzz_issuance(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "kzz_share":
        return run_kzz_share(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "kzz_suspend":
        return run_kzz_suspend(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "kzz_put_call_item":
        return run_kzz_put_call_item(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "kzz_put":
        return run_kzz_put(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "kzz_call":
        return run_kzz_call(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "kzz_conv":
        return run_kzz_conv(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "option_basic_info":
        return run_option_basic_info(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "option_std_ctr_specs":
        return run_option_std_ctr_specs(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "option_mon_ctr_specs":
        return run_option_mon_ctr_specs(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "treasury_yield":
        return run_info_payload_task(
            task_name="treasury_yield",
            sync_fn=context.info_data.sync_treasury_yield,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "kzz_conv_change":
        return run_kzz_conv_change(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "kzz_corr":
        return run_kzz_corr(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "kzz_call_explanation":
        return run_kzz_call_explanation(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "kzz_put_explanation":
        return run_kzz_put_explanation(
            info_data=context.info_data,
            code_list=code_list,
            force=task_spec.force,
        )
    if task_spec.task == "block_trading":
        return run_info_payload_task(
            task_name="block_trading",
            sync_fn=context.info_data.sync_block_trading,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "long_hu_bang":
        return run_info_payload_task(
            task_name="long_hu_bang",
            sync_fn=context.info_data.sync_long_hu_bang,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "margin_detail":
        return run_info_payload_task(
            task_name="margin_detail",
            sync_fn=context.info_data.sync_margin_detail,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "margin_summary":
        return run_margin_summary(
            info_data=context.info_data,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "daily_kline":
        return run_daily_kline(
            market_data=context.market_data,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "minute_kline":
        return run_minute_kline(
            market_data=context.market_data,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    if task_spec.task == "market_snapshot":
        return run_market_snapshot(
            market_data=context.market_data,
            code_list=code_list,
            begin_date=begin_date,
            end_date=end_date,
            force=task_spec.force,
        )
    raise ValueError(f"未知任务: {task_spec.task}")


def warn_ignored_task_params(task_spec: TaskRunSpec) -> None:
    if task_spec.task == "bj_code_mapping":
        if _task_spec_has_date_range(task_spec):
            logger.warning("bj_code_mapping 不支持 begin_date/end_date，本次将忽略这些参数。")
        if task_spec.codes_raw.strip():
            logger.warning("bj_code_mapping 不支持 codes，本次将忽略该参数。")
        if task_spec.limit:
            logger.warning("bj_code_mapping 不支持 limit，本次将忽略该参数。")
    if task_spec.task == "index_constituent" and _task_spec_has_date_range(task_spec):
        logger.warning("index_constituent 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "industry_base_info":
        if _task_spec_has_date_range(task_spec):
            logger.warning("industry_base_info 不支持 begin_date/end_date，本次将忽略这些参数。")
        if task_spec.codes_raw.strip():
            logger.warning("industry_base_info 不支持 codes，本次将忽略该参数。")
        if task_spec.limit:
            logger.warning("industry_base_info 不支持 limit，本次将忽略该参数。")
    if task_spec.task == "industry_constituent" and _task_spec_has_date_range(task_spec):
        logger.warning("industry_constituent 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "etf_pcf" and _task_spec_has_date_range(task_spec):
        logger.warning("etf_pcf 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "kzz_conv_change" and _task_spec_has_date_range(task_spec):
        logger.warning("kzz_conv_change 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "kzz_corr" and _task_spec_has_date_range(task_spec):
        logger.warning("kzz_corr 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "kzz_call_explanation" and _task_spec_has_date_range(task_spec):
        logger.warning("kzz_call_explanation 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "kzz_issuance" and _task_spec_has_date_range(task_spec):
        logger.warning("kzz_issuance 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "kzz_share" and _task_spec_has_date_range(task_spec):
        logger.warning("kzz_share 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "kzz_suspend" and _task_spec_has_date_range(task_spec):
        logger.warning("kzz_suspend 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "kzz_put_call_item" and _task_spec_has_date_range(task_spec):
        logger.warning("kzz_put_call_item 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "kzz_put" and _task_spec_has_date_range(task_spec):
        logger.warning("kzz_put 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "kzz_call" and _task_spec_has_date_range(task_spec):
        logger.warning("kzz_call 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "kzz_conv" and _task_spec_has_date_range(task_spec):
        logger.warning("kzz_conv 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "option_basic_info" and _task_spec_has_date_range(task_spec):
        logger.warning("option_basic_info 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "option_std_ctr_specs" and _task_spec_has_date_range(task_spec):
        logger.warning("option_std_ctr_specs 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "option_mon_ctr_specs" and _task_spec_has_date_range(task_spec):
        logger.warning("option_mon_ctr_specs 不支持 begin_date/end_date，本次将忽略这些参数。")
    if task_spec.task == "kzz_put_explanation" and _task_spec_has_date_range(task_spec):
        logger.warning("kzz_put_explanation 不支持 begin_date/end_date，本次将忽略这些参数。")


def _task_spec_has_date_range(task_spec: TaskRunSpec) -> bool:
    return task_spec.begin_date is not None or task_spec.end_date is not None


def task_ignores_date_range(task: str) -> bool:
    return task in {
        "bj_code_mapping",
        "index_constituent",
        "industry_base_info",
        "industry_constituent",
        "etf_pcf",
        "kzz_issuance",
        "kzz_share",
        "kzz_suspend",
        "kzz_put_call_item",
        "kzz_put",
        "kzz_call",
        "kzz_conv",
        "option_basic_info",
        "option_std_ctr_specs",
        "option_mon_ctr_specs",
        "kzz_conv_change",
        "kzz_corr",
        "kzz_call_explanation",
        "kzz_put_explanation",
    }


def run_code_info(base_data: BaseData, force: bool) -> int:
    logger.info("code_info start security_type=%s", DEFAULT_SYNC_SECURITY_TYPE)
    inserted = base_data.sync_code_info(security_type=DEFAULT_SYNC_SECURITY_TYPE, force=force)
    logger.info("code_info finished inserted_rows=%s", inserted)
    return 0


def run_hist_code_list(
    base_data: BaseData,
    begin_date: int,
    end_date: int,
    force: bool,
) -> int:
    logger.info(
        "hist_code_list start security_type=%s begin_date=%s end_date=%s",
        DEFAULT_SYNC_SECURITY_TYPE,
        begin_date,
        end_date,
    )
    inserted = base_data.sync_hist_code_list(
        security_type=DEFAULT_SYNC_SECURITY_TYPE,
        begin_date=begin_date,
        end_date=end_date,
        force=force,
    )
    logger.info("hist_code_list finished inserted_rows=%s", inserted)
    return 0


def run_bj_code_mapping(info_data: InfoData, force: bool) -> int:
    logger.info("bj_code_mapping start")
    inserted = info_data.sync_bj_code_mapping(force=force)
    logger.info("bj_code_mapping finished inserted_rows=%s", inserted)
    return 0


def run_stock_basic(info_data: InfoData, code_list: list[str], force: bool) -> int:
    logger.info("stock_basic start total_codes=%s", len(code_list))
    inserted = info_data.sync_stock_basic(code_list=code_list, force=force)
    logger.info("stock_basic finished inserted_rows=%s", inserted)
    return 0


def run_history_stock_status(
    info_data: InfoData,
    code_list: list[str],
    begin_date: int,
    end_date: int,
    force: bool,
) -> int:
    logger.info(
        "history_stock_status start total_codes=%s begin_date=%s end_date=%s",
        len(code_list),
        begin_date,
        end_date,
    )
    return _run_per_code_task(
        task_name="history_stock_status",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_history_stock_status(
            code_list=[code],
            begin_date=begin_date,
            end_date=end_date,
            force=force,
        ),
    )


def run_adj_factor(
    base_data: BaseData,
    code_list: list[str],
    force: bool,
    local_path: str,
) -> int:
    logger.info("adj_factor start total_codes=%s", len(code_list))
    total_inserted = base_data.sync_adj_factor(code_list=code_list, local_path=local_path, force=force)
    logger.info("adj_factor finished total_inserted=%s", total_inserted)
    return 0


def run_backward_factor(
    base_data: BaseData,
    code_list: list[str],
    force: bool,
    local_path: str,
) -> int:
    logger.info("backward_factor start total_codes=%s", len(code_list))
    total_inserted = base_data.sync_backward_factor(code_list=code_list, local_path=local_path, force=force)
    logger.info("backward_factor finished total_inserted=%s", total_inserted)
    return 0


def _run_per_code_task(
    task_name: str,
    code_list: list[str],
    sync_one,
    progress_logger=None,
) -> int:
    total_inserted = 0
    failed_codes: list[str] = []
    total_codes = len(code_list)
    for index, code in enumerate(code_list, start=1):
        if progress_logger is None:
            logger.info("%s progress=%s/%s", task_name, index, total_codes)
        else:
            progress_logger(index, code)
        try:
            inserted = sync_one(code)
        except Exception as exc:
            failed_codes.append(code)
            logger.warning("%s progress=%s/%s code=%s failed: %s", task_name, index, total_codes, code, exc)
            continue
        logger.info("%s progress=%s/%s inserted_rows=%s", task_name, index, total_codes, inserted)
        total_inserted += int(inserted)

    if failed_codes:
        logger.warning(
            "%s finished total_inserted=%s failed_count=%s failed_preview=%s",
            task_name,
            total_inserted,
            len(failed_codes),
            ",".join(failed_codes[:10]),
        )
    else:
        logger.info("%s finished total_inserted=%s", task_name, total_inserted)
    return 0


def _iter_code_batches(code_list: list[str], batch_size: int) -> Iterable[list[str]]:
    if batch_size <= 0:
        batch_size = 1
    for index in range(0, len(code_list), batch_size):
        yield code_list[index : index + batch_size]


def _run_batched_code_task(
    task_name: str,
    code_list: list[str],
    sync_batch,
    batch_size: int,
) -> int:
    total_inserted = 0
    batches = list(_iter_code_batches(code_list, batch_size))
    for index, batch_codes in enumerate(batches, start=1):
        logger.info(
            "%s progress=%s/%s batch_code_count=%s first_code=%s",
            task_name,
            index,
            len(batches),
            len(batch_codes),
            batch_codes[0] if batch_codes else "",
        )
        try:
            inserted = sync_batch(batch_codes)
        except Exception as exc:
            logger.warning(
                "%s progress=%s/%s batch_failed first_code=%s code_count=%s error=%s",
                task_name,
                index,
                len(batches),
                batch_codes[0] if batch_codes else "",
                len(batch_codes),
                exc,
            )
            continue
        total_inserted += int(inserted)
        logger.info("%s progress=%s/%s inserted_rows=%s", task_name, index, len(batches), inserted)

    logger.info("%s finished total_inserted=%s batch_count=%s", task_name, total_inserted, len(batches))
    return 0


def run_info_payload_task(
    task_name: str,
    sync_fn,
    code_list: list[str],
    begin_date: int,
    end_date: int,
    force: bool,
) -> int:
    logger.info(
        "%s start total_codes=%s begin_date=%s end_date=%s",
        task_name,
        len(code_list),
        begin_date,
        end_date,
    )
    return _run_per_code_task(
        task_name=task_name,
        code_list=code_list,
        sync_one=lambda code: sync_fn(
            code_list=[code],
            begin_date=begin_date,
            end_date=end_date,
            force=force,
        ),
    )


def run_index_constituent(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("index_constituent start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="index_constituent",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_index_constituent(
            code_list=[code],
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "index_constituent progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_INDEX_SYNC_SECURITY_TYPE,
        ),
    )


def run_industry_base_info(
    info_data: InfoData,
    force: bool,
) -> int:
    logger.info("industry_base_info start")
    inserted = info_data.sync_industry_base_info(force=force)
    logger.info("industry_base_info finished inserted_rows=%s", inserted)
    return 0


def run_industry_constituent(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("industry_constituent start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="industry_constituent",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_industry_constituent(
            code_list=[code],
            force=force,
        ),
    )


def run_etf_pcf(
    base_data: BaseData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("etf_pcf start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="etf_pcf",
        code_list=code_list,
        sync_one=lambda code: base_data.sync_etf_pcf(
            code_list=[code],
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "etf_pcf progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_ETF_SYNC_SECURITY_TYPE,
        ),
    )


def run_kzz_issuance(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("kzz_issuance start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="kzz_issuance",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_kzz_issuance(
            code_list=[code],
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "kzz_issuance progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_KZZ_SYNC_SECURITY_TYPE,
        ),
    )


def run_kzz_share(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("kzz_share start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="kzz_share",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_kzz_share(
            code_list=[code],
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "kzz_share progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_KZZ_SYNC_SECURITY_TYPE,
        ),
    )


def run_kzz_suspend(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("kzz_suspend start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="kzz_suspend",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_kzz_suspend(
            code_list=[code],
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "kzz_suspend progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_KZZ_SYNC_SECURITY_TYPE,
        ),
    )


def run_kzz_put_call_item(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("kzz_put_call_item start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="kzz_put_call_item",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_kzz_put_call_item(code_list=[code], force=force),
        progress_logger=lambda index, _code: logger.info(
            "kzz_put_call_item progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_KZZ_SYNC_SECURITY_TYPE,
        ),
    )


def run_kzz_put(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("kzz_put start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="kzz_put",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_kzz_put(code_list=[code], force=force),
        progress_logger=lambda index, _code: logger.info(
            "kzz_put progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_KZZ_SYNC_SECURITY_TYPE,
        ),
    )


def run_kzz_call(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("kzz_call start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="kzz_call",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_kzz_call(code_list=[code], force=force),
        progress_logger=lambda index, _code: logger.info(
            "kzz_call progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_KZZ_SYNC_SECURITY_TYPE,
        ),
    )


def run_kzz_conv(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("kzz_conv start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="kzz_conv",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_kzz_conv(code_list=[code], force=force),
        progress_logger=lambda index, _code: logger.info(
            "kzz_conv progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_KZZ_SYNC_SECURITY_TYPE,
        ),
    )


def run_margin_summary(
    info_data: InfoData,
    begin_date: int,
    end_date: int,
    force: bool,
) -> int:
    logger.info("margin_summary start begin_date=%s end_date=%s", begin_date, end_date)
    inserted = info_data.sync_margin_summary(
        begin_date=begin_date,
        end_date=end_date,
        force=force,
    )
    logger.info("margin_summary finished inserted_rows=%s", inserted)
    return 0


def run_option_basic_info(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("option_basic_info start total_codes=%s", len(code_list))
    return _run_batched_code_task(
        task_name="option_basic_info",
        code_list=code_list,
        sync_batch=lambda batch_codes: info_data.sync_option_basic_info(
            code_list=batch_codes,
            force=force,
        ),
        batch_size=OPTION_SYNC_CODE_BATCH_SIZE,
    )


def run_option_std_ctr_specs(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("option_std_ctr_specs start total_codes=%s", len(code_list))
    return _run_batched_code_task(
        task_name="option_std_ctr_specs",
        code_list=code_list,
        sync_batch=lambda batch_codes: info_data.sync_option_std_ctr_specs(
            code_list=batch_codes,
            force=force,
        ),
        batch_size=OPTION_SYNC_CODE_BATCH_SIZE,
    )


def run_option_mon_ctr_specs(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("option_mon_ctr_specs start total_codes=%s", len(code_list))
    return _run_batched_code_task(
        task_name="option_mon_ctr_specs",
        code_list=code_list,
        sync_batch=lambda batch_codes: info_data.sync_option_mon_ctr_specs(
            code_list=batch_codes,
            force=force,
        ),
        batch_size=OPTION_SYNC_CODE_BATCH_SIZE,
    )


def run_kzz_conv_change(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("kzz_conv_change start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="kzz_conv_change",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_kzz_conv_change(
            code_list=[code],
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "kzz_conv_change progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_KZZ_SYNC_SECURITY_TYPE,
        ),
    )


def run_kzz_corr(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("kzz_corr start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="kzz_corr",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_kzz_corr(
            code_list=[code],
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "kzz_corr progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_KZZ_SYNC_SECURITY_TYPE,
        ),
    )


def run_kzz_call_explanation(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("kzz_call_explanation start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="kzz_call_explanation",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_kzz_call_explanation(
            code_list=[code],
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "kzz_call_explanation progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_KZZ_SYNC_SECURITY_TYPE,
        ),
    )


def run_kzz_put_explanation(
    info_data: InfoData,
    code_list: list[str],
    force: bool,
) -> int:
    logger.info("kzz_put_explanation start total_codes=%s", len(code_list))
    return _run_per_code_task(
        task_name="kzz_put_explanation",
        code_list=code_list,
        sync_one=lambda code: info_data.sync_kzz_put_explanation(
            code_list=[code],
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "kzz_put_explanation progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_KZZ_SYNC_SECURITY_TYPE,
        ),
    )


def run_daily_kline(
    market_data: MarketData,
    code_list: list[str],
    begin_date: int,
    end_date: int,
    force: bool,
) -> int:
    """按单只股票顺序同步日线 K 线."""

    total_inserted = 0
    logger.info(
        "daily_kline start total_codes=%s begin_date=%s end_date=%s",
        len(code_list),
        begin_date,
        end_date,
    )
    return _run_per_code_task(
        task_name="daily_kline",
        code_list=code_list,
        sync_one=lambda code: market_data.sync_kline(
            code_list=[code],
            begin_date=begin_date,
            end_date=end_date,
            period=PeriodName.DAY,
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "daily_kline progress=%s/%s security_type=%s period=%s",
            index,
            len(code_list),
            DEFAULT_SYNC_SECURITY_TYPE,
            PeriodName.DAY,
        ),
    )


def run_market_snapshot(
    market_data: MarketData,
    code_list: list[str],
    begin_date: int,
    end_date: int,
    force: bool,
) -> int:
    """按单只股票顺序同步历史快照."""

    total_inserted = 0
    logger.info(
        "market_snapshot start total_codes=%s begin_date=%s end_date=%s",
        len(code_list),
        begin_date,
        end_date,
    )
    return _run_per_code_task(
        task_name="market_snapshot",
        code_list=code_list,
        sync_one=lambda code: market_data.sync_snapshot(
            code_list=[code],
            begin_date=begin_date,
            end_date=end_date,
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "market_snapshot progress=%s/%s security_type=%s",
            index,
            len(code_list),
            DEFAULT_SYNC_SECURITY_TYPE,
        ),
    )


def run_minute_kline(
    market_data: MarketData,
    code_list: list[str],
    begin_date: int,
    end_date: int,
    force: bool,
) -> int:
    """按单只股票顺序同步 1 分钟 K 线."""

    total_inserted = 0
    logger.info(
        "minute_kline start total_codes=%s begin_date=%s end_date=%s",
        len(code_list),
        begin_date,
        end_date,
    )
    return _run_per_code_task(
        task_name="minute_kline",
        code_list=code_list,
        sync_one=lambda code: market_data.sync_kline_minute(
            code_list=[code],
            begin_date=begin_date,
            end_date=end_date,
            force=force,
        ),
        progress_logger=lambda index, _code: logger.info(
            "minute_kline progress=%s/%s security_type=%s period=%s",
            index,
            len(code_list),
            DEFAULT_SYNC_SECURITY_TYPE,
            PeriodName.MIN1,
        ),
    )


def resolve_date_window(
    provider: AmazingDataSDKProvider,
    begin_date: int | None,
    end_date: int | None,
) -> tuple[int, int]:
    latest_trade_date = provider.session.get_latest_trade_date()
    latest_trade_date_value = int(latest_trade_date.strftime("%Y%m%d"))
    resolved_begin_date = begin_date or DEFAULT_FULL_SYNC_BEGIN_DATE
    resolved_end_date = end_date or latest_trade_date_value
    if resolved_begin_date > resolved_end_date:
        raise ValueError("begin_date 不能大于 end_date")
    return resolved_begin_date, resolved_end_date


def resolve_code_list(
    base_data: BaseData,
    task: str,
    raw_codes: str,
    limit: int,
    local_path: str | None = None,
    end_date: int | None = None,
) -> list[str]:
    codes = parse_codes(raw_codes)
    if task in {"daily_kline", "minute_kline"} and not codes:
        codes = resolve_market_kline_code_list(base_data=base_data, task=task, limit=limit)
        if not codes:
            raise RuntimeError("未获取到可同步的行情代码。")
        logger.info("resolved code_list count=%s", len(codes))
        return codes
    if is_option_task(task):
        code_list_source = "user_input"
        if not codes:
            if task == "option_std_ctr_specs":
                codes = list(DEFAULT_OPTION_STD_UNDERLYING_CODES)
                code_list_source = "default_option_underlyings"
                logger.info(
                    "code_list source=default_option_underlyings task=%s raw_count=%s",
                    task,
                    len(codes),
                )
            else:
                try:
                    codes = base_data.get_option_code_list(
                        security_type=SecurityType.EXTRA_ETF_OP,
                        force=False,
                    )
                except BaseDataCacheMissError as exc:
                    raise RuntimeError(f"未获取到可同步的期权代码: {exc}") from exc
                code_list_source = "base_data.get_option_code_list"
                logger.info(
                    "code_list source=base_data.get_option_code_list security_type=%s raw_count=%s",
                    SecurityType.EXTRA_ETF_OP,
                    len(codes),
                )
        logger.info("code_list source=%s raw_count=%s", code_list_source, len(codes))
        codes = sorted(dict.fromkeys(codes))
        if limit and limit > 0:
            codes = codes[:limit]
        if not codes:
            raise RuntimeError("未获取到可同步的期权代码。")
        logger.info("resolved code_list count=%s", len(codes))
        return codes
    if task == "treasury_yield":
        if not codes:
            codes = list(DEFAULT_TREASURY_TERMS)
            logger.info("code_list source=default_treasury_terms raw_count=%s", len(codes))
        else:
            logger.info("code_list source=user_input raw_count=%s", len(codes))
        codes = sorted(dict.fromkeys(codes))
        if limit and limit > 0:
            codes = codes[:limit]
        if not codes:
            raise RuntimeError("未获取到可同步的国债期限。")
        logger.info("resolved code_list count=%s", len(codes))
        return codes
    if not codes:
        security_type = resolve_task_security_type(task)
        if is_index_task(task):
            try:
                codes = base_data.get_index_universe(security_type=security_type, force=False)
            except BaseDataCacheMissError as exc:
                raise RuntimeError(f"未获取到可同步的指数代码: {exc}") from exc
            logger.info(
                "code_list source=base_data.get_index_universe security_type=%s raw_count=%s",
                security_type,
                len(codes),
            )
        elif is_etf_task(task):
            try:
                codes = base_data.get_etf_universe(security_type=security_type, force=False)
            except BaseDataCacheMissError as exc:
                raise RuntimeError(f"未获取到可同步的 ETF 代码: {exc}") from exc
            logger.info(
                "code_list source=base_data.get_etf_universe security_type=%s raw_count=%s",
                security_type,
                len(codes),
            )
        elif is_kzz_task(task):
            try:
                codes = base_data.get_kzz_universe(security_type=security_type, force=False)
            except BaseDataCacheMissError as exc:
                raise RuntimeError(f"未获取到可同步的可转债代码: {exc}") from exc
            logger.info(
                "code_list source=base_data.get_kzz_universe security_type=%s raw_count=%s",
                security_type,
                len(codes),
            )
        else:
            try:
                codes = base_data.get_stock_universe(security_type=security_type, force=False)
            except BaseDataCacheMissError as exc:
                raise RuntimeError(f"未获取到可同步的证券代码: {exc}") from exc
            logger.info(
                "code_list source=base_data.get_stock_universe security_type=%s raw_count=%s",
                security_type,
                len(codes),
            )
    else:
        logger.info("code_list source=user_input raw_count=%s", len(codes))

    codes = sorted(dict.fromkeys(codes))
    if limit and limit > 0:
        codes = codes[:limit]
    if not codes:
        if is_index_task(task):
            raise RuntimeError("未获取到可同步的指数代码。")
        if is_etf_task(task):
            raise RuntimeError("未获取到可同步的 ETF 代码。")
        if is_kzz_task(task):
            raise RuntimeError("未获取到可同步的可转债代码。")
        raise RuntimeError("未获取到可同步的证券代码。")

    logger.info("resolved code_list count=%s", len(codes))
    return codes


def resolve_market_kline_code_list(
    base_data: BaseData,
    task: str,
    limit: int,
) -> list[str]:
    raw_codes: list[str] = []
    sources = (
        ("stock", SecurityType.EXTRA_STOCK_A, base_data.get_stock_universe),
        ("index", SecurityType.EXTRA_INDEX_A, base_data.get_index_universe),
        ("etf", SecurityType.EXTRA_ETF, base_data.get_etf_universe),
    )
    for label, security_type, loader in sources:
        try:
            fetched = loader(security_type=security_type, force=False)
        except BaseDataCacheMissError as exc:
            logger.warning(
                "%s code_list source=%s security_type=%s failed: %s",
                task,
                label,
                security_type,
                exc,
            )
            continue
        logger.info(
            "%s code_list source=%s security_type=%s raw_count=%s",
            task,
            label,
            security_type,
            len(fetched),
        )
        raw_codes.extend(fetched)

    codes = normalize_code_list(raw_codes)
    if limit and limit > 0:
        codes = codes[:limit]
    return codes


def resolve_backward_factor_code_list(
    base_data: BaseData,
    raw_codes: str,
    limit: int,
) -> list[str]:
    codes = parse_codes(raw_codes)
    if not codes:
        security_type = resolve_task_security_type("backward_factor")
        try:
            base_data.ensure_code_list(security_type=security_type, force=False)
            codes = base_data.get_code_list_from_db(security_type=security_type)
        except BaseDataCacheMissError as exc:
            raise RuntimeError(f"未从 ad_code_info 获取到可同步的证券代码: {exc}") from exc
        logger.info(
            "code_list source=ad_code_info security_type=%s raw_count=%s",
            security_type,
            len(codes),
        )
    else:
        logger.info("code_list source=user_input raw_count=%s", len(codes))

    codes = sorted(dict.fromkeys(codes))
    if limit and limit > 0:
        codes = codes[:limit]
    if not codes:
        raise RuntimeError("未从 ad_code_info 获取到可同步的证券代码。")

    logger.info("resolved backward_factor code_list count=%s", len(codes))
    return codes


def resolve_task_security_type(task: str) -> str:
    if task in {"daily_kline", "minute_kline"}:
        return "EXTRA_STOCK_A+EXTRA_INDEX_A+EXTRA_ETF"
    if task == "hist_code_list":
        return DEFAULT_SYNC_SECURITY_TYPE
    if task == "bj_code_mapping":
        return "N/A"
    if is_option_task(task):
        return SecurityType.EXTRA_ETF_OP
    if task == "treasury_yield":
        return "N/A"
    if is_index_task(task):
        return DEFAULT_INDEX_SYNC_SECURITY_TYPE
    if is_etf_task(task):
        return DEFAULT_ETF_SYNC_SECURITY_TYPE
    if is_kzz_task(task):
        return DEFAULT_KZZ_SYNC_SECURITY_TYPE
    if task in {"industry_base_info", "industry_constituent", "industry_weight", "industry_daily"}:
        return "N/A"
    return DEFAULT_SYNC_SECURITY_TYPE


def is_index_task(task: str) -> bool:
    return task in {"index_constituent", "index_weight"}


def is_etf_task(task: str) -> bool:
    return task in {"etf_pcf", "fund_share", "fund_iopv"}


def is_kzz_task(task: str) -> bool:
    return task in {
        "kzz_issuance",
        "kzz_share",
        "kzz_suspend",
        "kzz_put_call_item",
        "kzz_put",
        "kzz_call",
        "kzz_conv",
        "kzz_conv_change",
        "kzz_corr",
        "kzz_call_explanation",
        "kzz_put_explanation",
    }


def is_option_task(task: str) -> bool:
    return task in {"option_basic_info", "option_std_ctr_specs", "option_mon_ctr_specs"}


def task_requires_code_list(task: str) -> bool:
    return task not in {"bj_code_mapping", "industry_base_info", "margin_summary", "hist_code_list"}


def resolve_task_run_mode(task: str) -> str:
    if task == "hist_code_list":
        return "per_trade_date_snapshot"
    if task == "bj_code_mapping":
        return "global_snapshot"
    if task == "treasury_yield":
        return "per_term_sequential"
    if task == "option_basic_info":
        return "per_option_batch_sequential"
    if task == "option_std_ctr_specs":
        return "per_option_batch_sequential"
    if task == "option_mon_ctr_specs":
        return "per_option_batch_sequential"
    if task == "industry_base_info":
        return "global_snapshot"
    if task == "margin_summary":
        return "global_range_snapshot"
    if task == "industry_constituent":
        return "per_index_sequential"
    if task == "industry_weight":
        return "per_index_sequential"
    if task == "industry_daily":
        return "per_index_sequential"
    if task == "etf_pcf":
        return "per_etf_sequential"
    if task == "fund_share":
        return "per_etf_sequential"
    if task == "fund_iopv":
        return "per_etf_sequential"
    if task == "kzz_issuance":
        return "per_kzz_sequential"
    if task == "kzz_share":
        return "per_kzz_sequential"
    if task == "kzz_suspend":
        return "per_kzz_sequential"
    if task == "kzz_put_call_item":
        return "per_kzz_sequential"
    if task == "kzz_put":
        return "per_kzz_sequential"
    if task == "kzz_call":
        return "per_kzz_sequential"
    if task == "kzz_conv":
        return "per_kzz_sequential"
    if task == "kzz_conv_change":
        return "per_kzz_sequential"
    if task == "kzz_corr":
        return "per_kzz_sequential"
    if task == "kzz_call_explanation":
        return "per_kzz_sequential"
    if task == "kzz_put_explanation":
        return "per_kzz_sequential"
    return "per_stock_sequential"


def filter_code_list_for_resume(
    context: SyncExecutionContext,
    task_spec: TaskRunSpec,
    code_list: list[str],
    begin_date: int,
    end_date: int,
) -> list[str]:
    if not task_spec.resume or not code_list:
        return code_list

    scope_pairs = build_resume_scope_pairs(
        context=context,
        task=task_spec.task,
        code_list=code_list,
        begin_date=begin_date,
        end_date=end_date,
    )
    if not scope_pairs:
        logger.info("resume skip task=%s reason=unsupported", task_spec.task)
        return code_list

    successful_scope_keys_by_task: dict[str, set[str]] = {}
    grouped_scope_keys: dict[str, list[str]] = {}
    for _code, scope_key, task_name in scope_pairs:
        grouped_scope_keys.setdefault(task_name, []).append(scope_key)

    for task_name, scope_keys in grouped_scope_keys.items():
        successful_scope_keys_by_task[task_name] = context.base_data.repository.load_successful_scope_keys(
            task_name,
            scope_keys,
        )

    if not any(successful_scope_keys_by_task.values()):
        logger.info("resume enabled task=%s no_successful_checkpoint_found", task_spec.task)
        return code_list

    remaining_codes: list[str] = []
    code_requirements: dict[str, list[tuple[str, str]]] = {}
    for code, scope_key, task_name in scope_pairs:
        code_requirements.setdefault(code, []).append((task_name, scope_key))

    for code in code_list:
        requirements = code_requirements.get(code, [])
        if not requirements:
            remaining_codes.append(code)
            continue
        if any(scope_key not in successful_scope_keys_by_task.get(task_name, set()) for task_name, scope_key in requirements):
            remaining_codes.append(code)

    skipped_count = len(code_list) - len(remaining_codes)
    logger.info(
        "resume enabled task=%s original_codes=%s skipped_codes=%s remaining_codes=%s",
        task_spec.task,
        len(code_list),
        skipped_count,
        len(remaining_codes),
    )
    if remaining_codes:
        logger.info("resume next_code task=%s code=%s", task_spec.task, remaining_codes[0])
    return remaining_codes


def build_resume_scope_pairs(
    context: SyncExecutionContext,
    task: str,
    code_list: list[str],
    begin_date: int,
    end_date: int,
) -> list[tuple[str, str, str]]:
    begin = None if task_ignores_date_range(task) else to_ch_date(begin_date)
    end = None if task_ignores_date_range(task) else to_ch_date(end_date)

    if task == "adj_factor":
        return [
            (code, context.base_data._build_factor_scope_key(FactorType.ADJ, [code]), "get_adj_factor")
            for code in code_list
        ]
    if task == "backward_factor":
        return [
            (code, context.base_data._build_factor_scope_key(FactorType.BACKWARD, [code]), "get_backward_factor")
            for code in code_list
        ]
    if task == "etf_pcf":
        pairs: list[tuple[str, str, str]] = []
        for code in code_list:
            pairs.append((code, context.base_data._build_code_scope_key("get_etf_pcf", [code]), "get_etf_pcf"))
            pairs.append(
                (
                    code,
                    context.base_data._build_code_scope_key("get_etf_pcf_constituent", [code]),
                    "get_etf_pcf_constituent",
                )
            )
        return pairs

    info_task_names = {
        "stock_basic": ("get_stock_basic", False),
        "history_stock_status": ("get_history_stock_status", True),
        "balance_sheet": ("get_balance_sheet", True),
        "cash_flow": ("get_cash_flow", True),
        "income": ("get_income", True),
        "profit_express": ("get_profit_express", True),
        "profit_notice": ("get_profit_notice", True),
        "fund_share": ("get_fund_share", True),
        "fund_iopv": ("get_fund_iopv", True),
        "share_holder": ("get_share_holder", True),
        "holder_num": ("get_holder_num", True),
        "equity_structure": ("get_equity_structure", True),
        "equity_pledge_freeze": ("get_equity_pledge_freeze", True),
        "equity_restricted": ("get_equity_restricted", True),
        "dividend": ("get_dividend", True),
        "right_issue": ("get_right_issue", True),
        "index_constituent": ("get_index_constituent", False),
        "index_weight": ("get_index_weight", True),
        "industry_constituent": ("get_industry_constituent", False),
        "industry_weight": ("get_industry_weight", True),
        "industry_daily": ("get_industry_daily", True),
        "kzz_issuance": ("get_kzz_issuance", False),
        "kzz_share": ("get_kzz_share", False),
        "kzz_suspend": ("get_kzz_suspend", False),
        "option_basic_info": ("get_option_basic_info", False),
        "option_std_ctr_specs": ("get_option_std_ctr_specs", False),
        "option_mon_ctr_specs": ("get_option_mon_ctr_specs", False),
        "treasury_yield": ("get_treasury_yield", True),
        "kzz_conv_change": ("get_kzz_conv_change", False),
        "kzz_corr": ("get_kzz_corr", False),
        "kzz_call_explanation": ("get_kzz_call_explanation", False),
        "kzz_put_explanation": ("get_kzz_put_explanation", False),
    }
    if task in info_task_names:
        task_name, use_date_range = info_task_names[task]
        return [
            (
                code,
                context.info_data._build_code_scope_key(
                    task_name,
                    [code],
                    begin_date=begin if use_date_range else None,
                    end_date=end if use_date_range else None,
                ),
                task_name,
            )
            for code in code_list
        ]

    if task == "daily_kline":
        return [
            (
                code,
                context.market_data._build_market_scope_key(
                    task_name="query_kline",
                    code_list=[code],
                    begin_date=begin,
                    end_date=end,
                    period=PeriodName.DAY,
                ),
                "query_kline",
            )
            for code in code_list
        ]
    if task == "minute_kline":
        return [
            (
                code,
                context.market_data._build_market_scope_key(
                    task_name="query_kline_minute",
                    code_list=[code],
                    begin_date=begin,
                    end_date=end,
                    period="10000",
                ),
                "query_kline_minute",
            )
            for code in code_list
        ]
    if task == "market_snapshot":
        return [
            (
                code,
                context.market_data._build_market_scope_key(
                    task_name="query_snapshot",
                    code_list=[code],
                    begin_date=begin,
                    end_date=end,
                ),
                "query_snapshot",
            )
            for code in code_list
        ]

    return []


def resolve_industry_code_list(
    info_data: InfoData,
    raw_codes: str,
    limit: int,
) -> list[str]:
    codes = parse_codes(raw_codes)
    if not codes:
        info_data.sync_industry_base_info(force=False)
        codes = info_data.repository.load_industry_base_index_codes()
        logger.info(
            "code_list source=ad_industry_base_info raw_count=%s",
            len(codes),
        )
    else:
        logger.info("code_list source=user_input raw_count=%s", len(codes))

    codes = sorted(dict.fromkeys(codes))
    if limit and limit > 0:
        codes = codes[:limit]
    if not codes:
        raise RuntimeError("未获取到可同步的行业指数代码。")
    logger.info("resolved code_list count=%s", len(codes))
    return codes


def parse_codes(raw: str) -> list[str]:
    text = str(raw).strip()
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


if __name__ == "__main__":
    raise SystemExit(main())
