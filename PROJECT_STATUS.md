# AmazingData System 项目状态说明

## 1. 项目定位

这是一个把 AmazingData SDK 数据同步到 ClickHouse 的项目。

当前仓库也新增了一条独立的 `BaoStock -> ClickHouse` 同步入口：

- 统一入口：`/Users/zhao/Desktop/git/sync_data_system/run_sync.py`
- BaoStock 全量配置：`/Users/zhao/Desktop/git/sync_data_system/run_sync.baostock.full.toml`
- 独立脚本入口：`/Users/zhao/Desktop/git/sync_data_system/scripts/run_baostock_sync.py`
- 运行手册：`/Users/zhao/Desktop/git/sync_data_system/BAOSTOCK_RUNBOOK.md`
- `begin/end` 型 BaoStock 接口已支持按业务表最新业务日期自动增量
- `BaoStock` 正式实现目录：`/Users/zhao/Desktop/git/sync_data_system/sources/baostock/`
- 公共同步层目录：`/Users/zhao/Desktop/git/sync_data_system/sync_core/`
- AmazingData 专项配置：`/Users/zhao/Desktop/git/sync_data_system/run_sync.amazingdata.special.toml`

仓库结构正在收敛为“多数据源 + 公共层”：

- `sources/amazingdata/`
  当前先通过包层承接根目录 AmazingData 主实现
- `sources/baostock/`
  当前已作为 BaoStock 正式实现目录
- `sync_core/`
  当前放 ClickHouse / 同步元数据 / 增量窗口 / scope_key / 同步日志写入等公共层

当前公共层复用进度：

- `BaoStock runner` 已开始直接复用 `sync_core`
- `BaseData` 已切到 `sync_core.amazingdata` 的 scope_key / 增量起始日 / 同步日志写入
- `InfoData` 仍保留原实现，后续再逐步迁移

当前目标不是做一个通用数据平台，而是先稳定完成 A 股主线数据同步，保证：

- 可以从 AmazingData 拉取数据
- 可以按统一表结构落到 ClickHouse
- 可以做增量同步
- 可以记录同步日志和断点
- 可以通过 `run_sync.py` 用统一命令执行

当前正式同步入口文件：

- `/Users/zhao/Desktop/git/sync_data_system/run_sync.py`

---

## 2. 当前设计原则

项目当前已经固化的设计原则如下：

- 默认只同步 `EXTRA_STOCK_A`
- 正式同步入口默认读取项目根目录 `runtime.local.yaml`
- 正式同步按“逐股顺序同步”执行，不再走复杂批调度
- ClickHouse 表名统一为小写
- ClickHouse 列名统一为小写 `snake_case`
- SDK 原始大写字段只在 provider 映射时使用，不直接进库
- 同步状态记录在 `ad_sync_task_log`
- 市场行情相关业务表不保留 `source/synced_at/created_at/updated_at`
- 日线表 `ad_market_kline_daily` 不保留 `period`
- 分钟表 `ad_market_kline_minute` 不保留 `period`
- `ad_trade_calendar` 不保留 `market`
- `.BJ` 股票代码只保留 `920xxx.BJ`

---

## 3. 当前已完成内容

### 3.1 基础能力

已完成以下基础设施：

- AmazingData SDK 登录与 provider 适配
- ClickHouse 客户端封装
- BaseData / InfoData / MarketData 三层结构
- 统一 `run_sync.py` 正式同步入口
- API 服务层（任务启动 / 状态查询 / 日志查看 / 取消）
- 装饰器任务注册表（第一版）
- 同步日志表与断点表
- 自动建表
- 对部分结构化表自动补列，避免强制删表重建

核心文件：

- `/Users/zhao/Desktop/git/sync_data_system/amazingdata_sdk_provider.py`
- `/Users/zhao/Desktop/git/sync_data_system/clickhouse_client.py`
- `/Users/zhao/Desktop/git/sync_data_system/base_data.py`
- `/Users/zhao/Desktop/git/sync_data_system/info_data.py`
- `/Users/zhao/Desktop/git/sync_data_system/market_data.py`
- `/Users/zhao/Desktop/git/sync_data_system/clickhouse_tables.py`
- `/Users/zhao/Desktop/git/sync_data_system/data_models.py`
- `/Users/zhao/Desktop/git/sync_data_system/service/api.py`
- `/Users/zhao/Desktop/git/sync_data_system/service/job_manager.py`
- `/Users/zhao/Desktop/git/sync_data_system/service/task_registry.py`
- `/Users/zhao/Desktop/git/sync_data_system/repositories/base_data_repository.py`
- `/Users/zhao/Desktop/git/sync_data_system/repositories/info_data_repository.py`
- `/Users/zhao/Desktop/git/sync_data_system/repositories/market_data_repository.py`
- `/Users/zhao/Desktop/git/sync_data_system/scripts/run_api_service.py`

### 3.2 当前正式任务

`run_sync.py` 当前已暴露 50 个正式任务：

1. `code_info`
2. `hist_code_list`
3. `bj_code_mapping`
4. `stock_basic`
5. `history_stock_status`
5. `adj_factor`
6. `backward_factor`
7. `balance_sheet`
8. `cash_flow`
9. `income`
10. `profit_express`
11. `profit_notice`
12. `share_holder`
13. `holder_num`
14. `equity_structure`
15. `equity_pledge_freeze`
16. `equity_restricted`
17. `dividend`
18. `right_issue`
19. `index_constituent`
20. `index_weight`
21. `industry_base_info`
22. `industry_constituent`
23. `industry_weight`
24. `industry_daily`
25. `etf_pcf`
26. `fund_share`
27. `fund_iopv`
28. `kzz_issuance`
29. `kzz_share`
30. `kzz_suspend`
31. `kzz_put_call_item`
32. `kzz_put`
33. `kzz_call`
34. `kzz_conv`
35. `option_basic_info`
36. `option_std_ctr_specs`
37. `option_mon_ctr_specs`
38. `treasury_yield`
39. `kzz_conv_change`
40. `kzz_corr`
41. `kzz_call_explanation`
42. `kzz_put_explanation`
43. `block_trading`
44. `long_hu_bang`
45. `margin_detail`
46. `margin_summary`
47. `margin_summary`
48. `daily_kline`
49. `minute_kline`
50. `market_snapshot`

### 3.3 已完成的表

当前主表包括：

- `ad_code_info`
- `ad_stock_basic`
- `ad_history_stock_status`
- `ad_adj_factor`
- `ad_backward_factor`
- `ad_balance_sheet`
- `ad_cash_flow`
- `ad_income`
- `ad_profit_express`
- `ad_profit_notice`
- `ad_share_holder`
- `ad_holder_num`
- `ad_equity_structure`
- `ad_equity_pledge_freeze`
- `ad_equity_restricted`
- `ad_dividend`
- `ad_right_issue`
- `ad_index_constituent`
- `ad_index_weight`
- `ad_industry_base_info`
- `ad_industry_constituent`
- `ad_industry_weight`
- `ad_industry_daily`
- `ad_etf_pcf`
- `ad_etf_pcf_constituent`
- `ad_fund_share`
- `ad_fund_iopv`
- `ad_kzz_issuance`
- `ad_kzz_share`
- `ad_kzz_suspend`
- `ad_kzz_put_call_item`
- `ad_kzz_put`
- `ad_kzz_call`
- `ad_kzz_conv`
- `ad_kzz_conv_change`
- `ad_kzz_corr`
- `ad_kzz_call_explanation`
- `ad_kzz_put_explanation`
- `ad_block_trading`
- `ad_long_hu_bang`
- `ad_margin_detail`
- `ad_margin_summary`
- `ad_option_basic_info`
- `ad_option_std_ctr_specs`
- `ad_option_mon_ctr_specs`
- `ad_treasury_yield`
- `ad_bj_code_mapping`
- `ad_market_kline_daily`
- `ad_market_kline_minute`
- `ad_market_snapshot`
- `ad_sync_task_log`
- `ad_sync_checkpoint`

### 3.4 已完成的结构化接口

下面这些接口已经不是简单存 `payload_json`，而是已经按业务字段结构化落库：

- `get_stock_basic`
- `get_history_stock_status`
- `get_share_holder`
- `get_holder_num`
- `get_equity_structure`
- `get_equity_pledge_freeze`
- `get_equity_restricted`
- `get_dividend`
- `get_right_issue`
- `get_index_constituent`
- `get_index_weight`
- `get_industry_base_info`
- `get_industry_constituent`
- `get_industry_weight`
- `get_industry_daily`
- `get_etf_pcf` 主表
- `get_etf_pcf` 成分明细
- `get_fund_share`
- `get_fund_iopv`
- `get_kzz_issuance`
- `get_kzz_share`
- `get_kzz_suspend`
- `get_kzz_conv_change`
- `get_kzz_corr`
- `get_kzz_call_explanation`
- `get_kzz_put_explanation`
- `get_kzz_put_call_item`
- `get_kzz_put`
- `get_kzz_call`
- `get_kzz_conv`
- `get_block_trading`
- `get_long_hu_bang`
- `get_margin_detail`
- `get_margin_summary`
- `get_option_basic_info`
- `get_option_std_ctr_specs`
- `get_option_mon_ctr_specs`
- `get_treasury_yield`
- `get_balance_sheet`
- `get_cash_flow`
- `get_income`
- `get_profit_express`
- `get_profit_notice`
- `query_kline` 日线
- `query_kline` 1 分钟
- `query_snapshot`
- `get_adj_factor`
- `get_backward_factor`
- `get_code_info`

### 3.5 增量逻辑现状

当前增量逻辑已经实现：

- `code_info` / `code_list` 走“当天成功跳过”
- 前复权 / 后复权按单 code 请求 SDK、按批次写入 ClickHouse
- `run_sync.py --resume` 可按成功 checkpoint 过滤已完成 code
- 日线按股票最新日期增量同步
- 分钟线按股票最新日期增量同步
- 快照按业务表已有最大日期增量同步
- 财务/股本/分红/配股这批 InfoData 表优先按业务表已有最大日期增量同步
- `etf_pcf` 现在同步两层：主表 + constituent 明细表

说明：

- 财务结构化表目前仍保留 `report_date / report_date_raw / payload_json` 兼容列
- 这是为了不强制删旧表，同时兼容历史数据
- 新字段已经是结构化字段，并统一小写
- 期权类任务当前需要显式传 `--codes`
- 国债收益率任务当前需要显式传期限列表，例如 `m3,m6,y1`

### 3.6 真实环境验证进度

目前用户已在真实 AmazingData 环境中确认以下表可正常同步：

- `ad_market_kline_minute`
- `ad_market_kline_daily`
- `ad_history_stock_status`
- `ad_trade_calendar`
- `ad_code_info`

当前仍待继续验证的主要方向：

- 股票基础与财务类
  - `ad_stock_basic`
  - `ad_adj_factor`
  - `ad_backward_factor`
  - `ad_balance_sheet`
  - `ad_cash_flow`
  - `ad_income`
  - `ad_profit_express`
  - `ad_profit_notice`
- 股本与权息类
  - `ad_share_holder`
  - `ad_holder_num`
  - `ad_equity_structure`
  - `ad_equity_pledge_freeze`
  - `ad_equity_restricted`
  - `ad_dividend`
  - `ad_right_issue`
- 指数 / 行业
  - `ad_index_constituent`
  - `ad_index_weight`
    当前默认全量配置已临时关闭，因为 AmazingData SDK 在部分环境会内部报 `KeyError('TRADE_DATE')`
  - `ad_industry_base_info`
  - `ad_industry_constituent`
  - `ad_industry_weight`
  - `ad_industry_daily`
- ETF / 可转债
  - `ad_etf_pcf`
  - `ad_etf_pcf_constituent`
  - `ad_fund_share`
  - `ad_fund_iopv`
  - `ad_kzz_issuance`
  - `ad_kzz_share`
  - `ad_kzz_suspend`
  - `ad_kzz_conv_change`
  - `ad_kzz_corr`
  - `ad_kzz_call_explanation`
  - `ad_kzz_put_explanation`
- 期权 / 国债
  - `ad_option_basic_info`
  - `ad_option_std_ctr_specs`
  - `ad_option_mon_ctr_specs`
  - `ad_treasury_yield`
- 其他行情
  - `ad_market_snapshot`

---

## 4. 当前未完成内容

### 4.1 当前离线同步范围外的接口

按当前这套 ClickHouse 离线同步框架，手册中的批量查询类接口已经基本接完。

当前仍未纳入主线范围的是：

- 实时订阅类接口
  - `onSnapshotindex`
  - `onSnapshotoption`
  - `OnKLine`
- 当前项目尚未接的期权代码池入口
  - `get_option_code_list`
- 密码管理类接口
  - `update_password`

### 4.2 尚未解决的问题

当前还有这些未收尾项：

- 本地 `RUNBOOK.md` 部分描述已落后于当前实现
  - 例如新增 ETF / 可转债 / 期权 / 国债接口后，操作示例需要同步更新
- 当前这个开发环境没有 AmazingData SDK
  - 无法在这里直接真实调用后续接口探字段
- 新完成的结构化表虽然已经过 `py_compile`，但仍需要在真实 AmazingData 环境里逐个实际跑命令验证

---

## 5. 当前最重要的注意点

### 5.1 不要轻易删表

这个项目现在已经明确要求：

- 不要随便建议删表重建
- 尽量保留现有数据
- 优先用“自动补列”兼容升级

### 5.2 字段命名已经统一

当前数据库设计要求非常明确：

- 表名统一小写
- 字段名统一小写 `snake_case`
- 不要把 SDK 的原始大写字段直接写进 ClickHouse

### 5.3 A 股范围有额外过滤规则

- 当前正式同步只考虑 `EXTRA_STOCK_A`
- `.BJ` 结尾代码只保留 `920xxx.BJ`
- 非 `920` 开头的北交所代码不应进入股票主线表

### 5.4 市场行情表的约束

- `ad_market_kline_daily` 只存日线，不要加 `period`
- `ad_market_kline_minute` 只存分钟线，不要加 `period`
- `ad_market_snapshot` 已去掉 `snapshot_kind`
- 市场表不要再加 `source/synced_at/created_at/updated_at`

### 5.5 运行环境注意事项

当前这个 macOS 工作区里：

- `python3` 可以编译代码
- 但本地没有 `AmazingData` SDK
- 所以这里只能做代码接入和静态检查
- 真正跑同步，要在用户自己的 AmazingData 可用环境里执行

---

## 6. 当前建议的下一步

建议下一步优先做下面两件事：

### 6.1 先验证最近完成的结构化表

建议至少单股验证这些命令：

```bash
python run_sync.py balance_sheet --codes 000001.SZ --begin-date 20240101 --end-date 20241231 --force --log-level INFO
python run_sync.py cash_flow --codes 000001.SZ --begin-date 20240101 --end-date 20241231 --force --log-level INFO
python run_sync.py income --codes 000001.SZ --begin-date 20240101 --end-date 20241231 --force --log-level INFO
python run_sync.py profit_express --codes 000001.SZ --begin-date 20240101 --end-date 20241231 --force --log-level INFO
python run_sync.py profit_notice --codes 000001.SZ --begin-date 20240101 --end-date 20241231 --force --log-level INFO
```

### 6.2 做真实环境验收

如果继续推进，优先级建议是：

1. 验证新增 ETF / 可转债 / 期权 / 国债接口
2. 验证 `etf_pcf` 主表与 constituent 明细是否同时完整落库
3. 验证 `--resume` 在长任务中是否符合预期
4. 再决定是否接实时订阅类接口

---

## 7. 给新窗口的简版说明

如果在新窗口里继续对话，可以直接先告诉对方：

- 这是一个 AmazingData -> ClickHouse 的同步项目
- 当前正式入口是 `run_sync.py`
- 当前 `run_sync.py` 已接上 50 个正式任务
- 财务/股本/分红/配股 / ETF / 可转债 / 期权 / 国债这批离线查询接口已经接上
- 表名和字段名统一小写
- 不要建议删表重建
- 期权类任务当前需要显式传 `--codes`
- 国债收益率任务当前需要显式传期限列表
- 如果没有 AmazingData SDK 运行环境，就继续按字段清单接表或在真实环境验收
