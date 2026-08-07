# AmazingData Financial DB Runbook

## 环境准备

执行同步前，至少确认下面几类环境变量已经可用。

AmazingData：

- `AD_ACCOUNT`
- `AD_PASSWORD`
- `AD_IP`
- `AD_PORT`
- `AD_LOCAL_PATH`

ClickHouse：

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_USERNAME`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`

说明：

- 默认读取 `config/runtime.local.yaml`；也可以通过 `SYNC_DATA_RUNTIME_CONFIG` 覆盖
- `scripts/run_provider_sync.py` 现在是统一入口
- `run_sync.full.toml` / `run_sync.amazingdata.full.toml` 走 AmazingData
- `run_sync.baostock.full.toml` 走 BaoStock
- BaoStock 详细说明见 [BAOSTOCK_RUNBOOK.md](BAOSTOCK_RUNBOOK.md)

推荐先做一次最小验证：

```bash
python3 scripts/validate_sync_config.py
python3 scripts/install_provider_deps.py amazingdata --check
python3 scripts/run_provider_sync.py amazingdata.code_info --limit 10 --force --log-level INFO
```

如果 `install_provider_deps.py amazingdata --check` 报 `import AmazingData` 缺失，需要先把官方 AmazingData SDK 安装到当前 Python 环境；`dependencies` 只覆盖可 pip 安装的公共包。

## API 服务

当前项目已提供一个最小 API 服务层，用来管理同步任务。

启动：

```bash
python3 scripts/run_api_service.py
```

默认监听：

- `0.0.0.0:18080`

服务入口文件：

- `service/api.py`
- `service/job_manager.py`
- `scripts/run_api_service.py`

当前 API 能力：

- 健康检查：`GET /health`
- 列出任务：`GET /api/meta/tasks`
- 列出配置：`GET /api/meta/configs`
- 启动配置任务：`POST /api/jobs/run-config`
- 启动单任务：`POST /api/jobs/run-task`
- 查看任务列表：`GET /api/jobs`
- 查看任务详情：`GET /api/jobs/{job_id}`
- 查看任务日志：`GET /api/jobs/{job_id}/logs`
- 取消任务：`POST /api/jobs/{job_id}/cancel`

当前服务层还支持装饰器注册任务，第一版已接入：

- `amazingdata.daily_kline`
- `amazingdata.minute_kline`

注册表文件：

- `service/task_registry.py`

示例：

```bash
curl http://127.0.0.1:18080/health
```

```bash
curl -X POST http://127.0.0.1:18080/api/jobs/run-config \
  -H 'Content-Type: application/json' \
  -d '{"config":"run_sync.full.toml"}'
```

```bash
curl -X POST http://127.0.0.1:18080/api/jobs/run-task \
  -H 'Content-Type: application/json' \
  -d '{"name":"amazingdata.daily_kline","codes":["000001.SZ"],"begin_date":20240101,"end_date":20240131,"limit":20}'
```

说明：

- `run-task` 当前推荐使用 `name`
- `codes` 当前推荐使用 `list[str]`

## 当前正式同步入口

- 代码池：`python scripts/run_provider_sync.py amazingdata.code_info`
- 历史代码表：`python scripts/run_provider_sync.py amazingdata.hist_code_list`
- 北交所新旧代码映射：`python scripts/run_provider_sync.py amazingdata.bj_code_mapping`
- 股票基础信息：`python scripts/run_provider_sync.py amazingdata.stock_basic`
- 历史证券状态：`python scripts/run_provider_sync.py amazingdata.history_stock_status`
- 单次复权因子：`python scripts/run_provider_sync.py amazingdata.adj_factor`
- 后复权因子：`python scripts/run_provider_sync.py amazingdata.backward_factor`
- 资产负债表：`python scripts/run_provider_sync.py amazingdata.balance_sheet`
- 现金流量表：`python scripts/run_provider_sync.py amazingdata.cash_flow`
- 利润表：`python scripts/run_provider_sync.py amazingdata.income`
- 业绩快报：`python scripts/run_provider_sync.py amazingdata.profit_express`
- 业绩预告：`python scripts/run_provider_sync.py amazingdata.profit_notice`
- 十大股东：`python scripts/run_provider_sync.py amazingdata.share_holder`
- 股东户数：`python scripts/run_provider_sync.py amazingdata.holder_num`
- 股本结构：`python scripts/run_provider_sync.py amazingdata.equity_structure`
- 股权冻结/质押：`python scripts/run_provider_sync.py amazingdata.equity_pledge_freeze`
- 限售股解禁：`python scripts/run_provider_sync.py amazingdata.equity_restricted`
- 分红方案：`python scripts/run_provider_sync.py amazingdata.dividend`
- 配股方案：`python scripts/run_provider_sync.py amazingdata.right_issue`
- ETF 申赎主表 + 成分明细：`python scripts/run_provider_sync.py amazingdata.etf_pcf`
- ETF IOPV：`python scripts/run_provider_sync.py amazingdata.fund_iopv`
- 可转债发行：`python scripts/run_provider_sync.py amazingdata.kzz_issuance`
- 可转债份额：`python scripts/run_provider_sync.py amazingdata.kzz_share`
- 可转债停复牌：`python scripts/run_provider_sync.py amazingdata.kzz_suspend`
- 可转债回售/赎回条款明细：`python scripts/run_provider_sync.py amazingdata.kzz_put_call_item`
- 可转债回售条款：`python scripts/run_provider_sync.py amazingdata.kzz_put`
- 可转债赎回条款：`python scripts/run_provider_sync.py amazingdata.kzz_call`
- 可转债转股条款：`python scripts/run_provider_sync.py amazingdata.kzz_conv`
- 可转债赎回条款执行说明：`python scripts/run_provider_sync.py amazingdata.kzz_call_explanation`
- 可转债回售条款执行说明：`python scripts/run_provider_sync.py amazingdata.kzz_put_explanation`
- 大宗交易：`python scripts/run_provider_sync.py amazingdata.block_trading`
- 龙虎榜：`python scripts/run_provider_sync.py amazingdata.long_hu_bang`
- 融资融券明细：`python scripts/run_provider_sync.py amazingdata.margin_detail`
- 融资融券汇总：`python scripts/run_provider_sync.py amazingdata.margin_summary`
- 期权基本资料：`python scripts/run_provider_sync.py amazingdata.option_basic_info --codes ...`
- 期权标准合约属性：`python scripts/run_provider_sync.py amazingdata.option_std_ctr_specs --codes ...`
- 期权月合约属性变动：`python scripts/run_provider_sync.py amazingdata.option_mon_ctr_specs --codes ...`
- 国债收益率：`python scripts/run_provider_sync.py amazingdata.treasury_yield --codes m3,m6,y1,...`
- 日线 K 线：`python scripts/run_provider_sync.py amazingdata.daily_kline`
- 1 分钟 K 线：`python scripts/run_provider_sync.py amazingdata.minute_kline`
- 历史快照：`python scripts/run_provider_sync.py amazingdata.market_snapshot`

默认行为：

- 默认读取 `config/runtime.local.yaml`；也可以通过 `SYNC_DATA_RUNTIME_CONFIG` 覆盖
- 默认只同步 `EXTRA_STOCK_A`
- 默认起始日期：`20100101`
- 默认结束日期：最新交易日
- 默认按任务自身的粒度顺序同步
- 支持 `--resume`，会按成功 checkpoint 跳过已完成 code / 标的

说明：

- `option_basic_info` / `option_mon_ctr_specs` 当前会默认按 `get_option_code_list(security_type='EXTRA_ETF_OP')` 自动展开代码池
- `option_std_ctr_specs` 当前默认使用官方支持的 ETF 标的列表
- `treasury_yield` 当前默认使用常见期限列表：`m3,m6,y1,y2,y3,y5,y7,y10,y20,y30`

## 如何同步

### 方式一：执行单个任务

适合补数、排障、验证某个接口。

示例：

```bash
python3 scripts/run_provider_sync.py amazingdata.stock_basic --codes 000001.SZ,600000.SH --force --log-level INFO
python3 scripts/run_provider_sync.py amazingdata.adj_factor --codes 000001.SZ --force --log-level INFO
python3 scripts/run_provider_sync.py amazingdata.daily_kline --codes 000001.SZ --begin-date 20240101 --end-date 20240131 --force --log-level INFO
```

### 方式二：按默认全量配置顺序执行

适合日常批量跑全流程。

```bash
python3 scripts/run_provider_sync.py
```

说明：

- 默认读取 `run_sync.full.toml`
- 默认按配置中的 `[[tasks]]` 顺序执行
- `run_sync.full.toml` 当前已经打开 `continue_on_error = true`

### 方式二补充：显式指定数据源全量配置

AmazingData：

```bash
python3 scripts/run_provider_sync.py --config run_sync.amazingdata.full.toml
```

AmazingData 专项补数：

```bash
python3 scripts/run_provider_sync.py --config run_sync.amazingdata.special.toml
```

BaoStock：

```bash
python3 scripts/run_provider_sync.py --config run_sync.baostock.full.toml
```

### 方式三：按自定义 TOML 配置执行

适合分批、分主题、分时间范围跑。

```bash
python3 scripts/run_provider_sync.py --config run_sync.example.toml
```

### 方式四：从上次成功断点继续

适合长任务中断后继续补跑。

```bash
python3 scripts/run_provider_sync.py --resume
python3 scripts/run_provider_sync.py amazingdata.daily_kline --resume
python3 scripts/run_provider_sync.py --config run_sync.full.toml --resume
```

说明：

- `--resume` 会对支持 checkpoint 恢复的任务结合 `ad_sync_checkpoint` 跳过已成功的 code / 标的
- 同步服务重启后会自动把未完成任务改为 `resume=true、force=false`；即使原任务是
  `force=true`，恢复执行也不会再次强制全量。`minute_kline` 会用逐代码成功
  checkpoint 跳过重启前已经完成的代码（例如完成 1000 个后从第 1001 个继续），
  并依据 `ad_market_kline_minute` 中每只证券已有的最大 `trade_time` 增量同步；
  checkpoint 对应代码在目标表中没有数据时不会跳过，避免删表后漏同步
- 已由目标表计算增量日期或数据状态的任务不使用永久 checkpoint 跳过标的，包括：
  - 财务与持有人：`balance_sheet`、`cash_flow`、`income`、`profit_express`、
    `profit_notice`、`fund_share`、`fund_iopv`、`share_holder`、`holder_num`
  - 证券事件：`history_stock_status`、`equity_structure`、`equity_pledge_freeze`、
    `equity_restricted`、`dividend`、`right_issue`
  - 指数与行情：`index_weight`、`industry_weight`、`industry_daily`、
    `daily_kline`、`minute_kline`、`market_snapshot`
  - 其他日期序列：`adj_factor`、`backward_factor`、`option_std_ctr_specs`、
    `option_mon_ctr_specs`、`treasury_yield`
- 是否能继续到你想要的位置，取决于对应任务的 checkpoint scope 设计

## 配置执行

- 支持用 `TOML` 配置文件按列表顺序批量执行任务
- 示例文件：`config/sync/plans/run_sync.example.toml`
- AmazingData 包路径：`providers/amazingdata/`
- BaoStock 包路径：`providers/baostock/`
- QMT 包路径：`providers/qmt/`
- Provider 公共层路径：`core/`
- 旧 `sources/*` 和根目录 AmazingData 模块仅保留兼容导出，不再作为新代码入口

常用命令：

```bash
python3 scripts/run_provider_sync.py
python3 scripts/run_provider_sync.py --config run_sync.full.toml
python3 scripts/run_provider_sync.py --config run_sync.example.toml
```

说明：

- 直接执行 `python3 scripts/run_provider_sync.py` 时，会默认读取 `run_sync.full.toml`
- 如果你想换一份批量任务配置，再显式传 `--config`

配置规则：

- 顶层可写 `runtime_path`、`log_level`、`continue_on_error`
- 可用 `[defaults]` 给所有任务提供默认参数
- 用 `[[tasks]]` 逐项写任务
- 每个任务支持：`task`、`codes`、`begin_date`、`end_date`、`limit`、`force`、`resume`、`enabled`
- `codes` 既可以写成逗号分隔字符串，也可以写成字符串数组

## 同时同步多个数据源

同步服务使用全局“供应商泳道”调度：

- AmazingData、BaoStock、AKShare 等不同供应商可以并发执行。
- 同一供应商在全局范围内只有一个子任务运行，后续任务按入队时间 FIFO 串行执行。
- 一个跨供应商父任务会拆成多个供应商子任务；每个子任务内部仍按配置顺序逐项运行并复用供应商上下文。
- 手动任务、定时任务和同步配置共用同一组泳道。
- 默认最多同时运行 3 个供应商，可在 `config/runtime.local.yaml` 调整：

```yaml
sync:
  scheduler:
    max_parallel_providers: 3
```

也可以用环境变量覆盖，最小值为 1：

```bash
export SYNC_MAX_PARALLEL_PROVIDERS=3
```

父任务取消时，所有尚未运行的供应商子任务会从队列移除，正在运行的供应商子任务会收到终止信号。服务重启时，排队中的子任务会继续调度；重启前正在运行的子任务会标记为 `interrupted`，不会自动重跑。

直接从终端启动的独立 CLI 进程不受服务泳道约束。不要同时手工启动两个相同供应商的全量进程，以免放大重复请求、ClickHouse 写入压力和供应商配额消耗。

## 当前主表

- `ad_code_info`
- `ad_hist_code_daily`
- `ad_bj_code_mapping`
- `ad_stock_basic`
- `ad_history_stock_status`
- `ad_adj_factor`
- `ad_backward_factor`
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
- `ad_market_kline_daily`
- `ad_market_kline_minute`
- `ad_market_snapshot`
- `ad_sync_task_log`
- `ad_sync_checkpoint`

说明：

- 业务表只保留业务字段
- `ad_sync_task_log` 记录同步日志
- `ad_sync_checkpoint` 记录断点与 `--resume` 所需状态

## 当前增量逻辑

### code_info / code_list

- 先同步 `get_code_info`
- 再从 ClickHouse 投影出代码池

### hist_code_list

- 通过 `python scripts/run_provider_sync.py amazingdata.hist_code_list ...` 触发
- 数据落到 `ad_hist_code_daily`
- 当前默认按 `EXTRA_STOCK_A`、`EXTRA_INDEX_A`、`EXTRA_ETF`、`EXTRA_KZZ`
  同步历史代码表
- 会按表里已有最新 `trade_date` 做增量

### stock_basic

- 先获取 A 股代码池
- 同步 `ad_stock_basic`

### history_stock_status

- 先获取 A 股代码池
- 按传入日期范围同步 `ad_history_stock_status`
- 优先按业务表已有最大日期做增量

### adj_factor / backward_factor

- 先获取 A 股代码池
- 按单只股票请求 SDK
- 按批次写入 ClickHouse，减少 parts 数量
- 前复权数据落到 `ad_adj_factor`
- 后复权数据落到 `ad_backward_factor`
- 当前通过 checkpoint 控制重复调用
- 当前 SDK 取数使用 `is_local=False`

### balance_sheet / cash_flow / income / profit_express / profit_notice

- 先获取 A 股代码池
- 按单只股票逐个同步
- 三大财务报表会保留 `statement_type`、`report_type`、`reporting_period`、
  `ann_date` 和 `actual_ann_date`，同一报告期的原始报表、调整报表及不同公告版本不会互相覆盖
- 数据分别落到：
  - `ad_balance_sheet`
  - `ad_cash_flow`
  - `ad_income`
  - `ad_profit_express`
  - `ad_profit_notice`
- 五类报告任务普通增量同步都会回看 400 天，以捕获调整报表、业绩快报修订和业绩预告修订
- 五类报告任务使用 `--force` 时都会从指定的 `begin_date` 重新拉取
- `profit_express` 以报告期、公告日期和实际公告日期区分版本
- `profit_notice` 还会按预告类型、报告类型和预告序号区分同一报告期的多条记录

### ReplacingMergeTree 多版本表重建

旧排序键可能合并不同业务记录的表共有 10 张：

- `ad_balance_sheet`
- `ad_cash_flow`
- `ad_income`
- `ad_profit_express`
- `ad_profit_notice`
- `ad_fund_share`
- `ad_share_holder`
- `ad_holder_num`
- `ad_equity_pledge_freeze`
- `ad_equity_restricted`

部署新版代码并删除这些旧表后，可以运行
`providers/amazingdata/plans/rebuild-replacing-mergetree.toml` 一次性重建和回填。
其他 `ReplacingMergeTree` 表属于证券时点快照、行情、因子、权重或已经包含完整事件维度，不需要重建。

### share_holder / holder_num / equity_structure

- 先获取 A 股代码池
- 按单只股票逐个同步
- 数据分别落到：
  - `ad_share_holder`
  - `ad_holder_num`
  - `ad_equity_structure`
- `share_holder` 会按截止日期、公告日期、股东类型和名次保留完整版本
- `holder_num` 会按截止日期和公告日期保留修订版本
- `share_holder`、`holder_num` 普通增量同步回看 400 天；`--force` 从指定的 `begin_date` 开始

### equity_pledge_freeze / equity_restricted

- 先获取 A 股代码池
- 按单只股票逐个同步
- 数据分别落到：
  - `ad_equity_pledge_freeze`
  - `ad_equity_restricted`
- `equity_pledge_freeze` 会区分持有人、质押起始日、冻结机构、股份类别和冻结类型
- `equity_restricted` 会区分同日不同解禁股份类型
- `equity_restricted` 普通增量同步回看 400 天；两项任务的 `--force` 都会从指定的 `begin_date` 开始

### dividend / right_issue

- 先获取 A 股代码池
- 按单只股票逐个同步
- 数据分别落到：
  - `ad_dividend`
  - `ad_right_issue`
- 优先按业务表已有最大日期做增量

### etf_pcf / fund_share / fund_iopv

- ETF 主线任务当前分别落到：
  - `ad_etf_pcf`
  - `ad_etf_pcf_constituent`
  - `ad_fund_share`
  - `ad_fund_iopv`
- `etf_pcf` 当前会同时同步主表和 constituent 明细
- `fund_share` 会区分合并口径、单体口径和变动原因，并在增量同步时回看 400 天

### kzz_*

- 可转债主线任务当前分别落到：
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

### block_trading / long_hu_bang / margin_detail / margin_summary

- `block_trading` / `long_hu_bang` / `margin_detail` 按单只股票逐个同步
- 数据分别落到：
  - `ad_block_trading`
  - `ad_long_hu_bang`
  - `ad_margin_detail`
- `margin_summary` 落到：
  - `ad_margin_summary`
- `block_trading` / `long_hu_bang` / `margin_detail` 同步前会按 `market_code + trade_date` 读取业务表最新日期
- `margin_summary` 同步前会按全表最新 `trade_date` 做增量
- 请求 SDK 时固定使用 `is_local=False`

### option_* / treasury_yield

- 期权主线任务当前分别落到：
  - `ad_option_basic_info`
  - `ad_option_std_ctr_specs`
  - `ad_option_mon_ctr_specs`
- 国债收益率当前落到：
  - `ad_treasury_yield`
- `option_basic_info` / `option_mon_ctr_specs` 当前默认按 `get_option_code_list(security_type='EXTRA_ETF_OP')` 自动展开代码池
- `option_std_ctr_specs` 当前默认使用官方支持的 ETF 标的列表
- `treasury_yield` 当前默认使用期限列表：`m3,m6,y1,y2,y3,y5,y7,y10,y20,y30`
- 期权类任务当前按每批 `500` 个 code 顺序同步，不再逐个 code 请求

### daily_kline

- 从 `ad_hist_code_daily` 获取请求区间内的股票、指数、ETF、ETF 期权、可转债代码池
- 按单只证券逐个同步
- 每只证券同步前先查 `ad_market_kline_daily` 中该证券最新日期
- 如果没有历史数据，则从传入 `begin_date` 开始

补充说明：

- `index_weight` 当前虽然已接入，但默认全量配置中已临时关闭
- 原因是 AmazingData SDK 在部分环境会内部报 `KeyError('TRADE_DATE')`

### minute_kline

- 从 `ad_hist_code_daily` 获取请求区间内的股票、指数、ETF、可转债代码池
- 不同步 `EXTRA_ETF_OP` ETF 期权分钟数据
- 按单只证券逐个同步
- 每只证券同步前先查 `ad_market_kline_minute` 中该证券最新日期
- 仅同步 1 分钟 K 线

### market_snapshot

- 先获取 A 股代码池
- 按单只股票逐个同步
- 优先按业务表已有最大日期做增量

## 常用命令

### 同步代码池

```bash
python scripts/run_provider_sync.py amazingdata.code_info --force --log-level INFO
```

### 同步北交所新旧代码映射

```bash
python scripts/run_provider_sync.py amazingdata.bj_code_mapping --force --log-level INFO
```

### 同步股票基础信息

```bash
python scripts/run_provider_sync.py amazingdata.stock_basic --limit 20 --force --log-level INFO
```

### 同步历史证券状态

```bash
python scripts/run_provider_sync.py amazingdata.history_stock_status --begin-date 20240101 --end-date 20240131 --limit 20 --force --log-level INFO
```

### 同步单次复权因子

```bash
python scripts/run_provider_sync.py amazingdata.adj_factor --limit 20 --force --log-level INFO
```

### 同步后复权因子

```bash
python scripts/run_provider_sync.py amazingdata.backward_factor --limit 20 --force --log-level INFO
```

### 同步 ETF IOPV

```bash
python scripts/run_provider_sync.py amazingdata.fund_iopv --codes 510050.SH --begin-date 20240101 --end-date 20240131 --force --log-level INFO
```

### 同步可转债发行

```bash
python scripts/run_provider_sync.py amazingdata.kzz_issuance --codes 110030.SH --force --log-level INFO
```

### 同步可转债份额

```bash
python scripts/run_provider_sync.py amazingdata.kzz_share --codes 110030.SH --force --log-level INFO
```

### 同步可转债停复牌

```bash
python scripts/run_provider_sync.py amazingdata.kzz_suspend --codes 110030.SH --force --log-level INFO
```

### 同步可转债回售条款执行说明

```bash
python scripts/run_provider_sync.py amazingdata.kzz_put_explanation --codes 110030.SH --force --log-level INFO
```

### 同步期权基本资料

```bash
python scripts/run_provider_sync.py amazingdata.option_basic_info --codes 10000001.SH --force --log-level INFO
```

### 同步期权标准合约属性

```bash
python scripts/run_provider_sync.py amazingdata.option_std_ctr_specs --codes 510050.SH --force --log-level INFO
```

### 同步期权月合约属性变动

```bash
python scripts/run_provider_sync.py amazingdata.option_mon_ctr_specs --codes 510050.SH --force --log-level INFO
```

### 同步国债收益率

```bash
python scripts/run_provider_sync.py amazingdata.treasury_yield --codes m3,m6,y1,y2,y3,y5,y7,y10,y30 --begin-date 20240101 --end-date 20240131 --force --log-level INFO
```

### 同步资产负债表

```bash
python scripts/run_provider_sync.py amazingdata.balance_sheet --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 同步现金流量表

```bash
python scripts/run_provider_sync.py amazingdata.cash_flow --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 同步利润表

```bash
python scripts/run_provider_sync.py amazingdata.income --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 同步业绩快报

```bash
python scripts/run_provider_sync.py amazingdata.profit_express --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 同步业绩预告

```bash
python scripts/run_provider_sync.py amazingdata.profit_notice --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 同步十大股东

```bash
python scripts/run_provider_sync.py amazingdata.share_holder --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 同步股东户数

```bash
python scripts/run_provider_sync.py amazingdata.holder_num --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 同步股本结构

```bash
python scripts/run_provider_sync.py amazingdata.equity_structure --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 同步股权冻结/质押

```bash
python scripts/run_provider_sync.py amazingdata.equity_pledge_freeze --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 同步限售股解禁

```bash
python scripts/run_provider_sync.py amazingdata.equity_restricted --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 同步分红方案

```bash
python scripts/run_provider_sync.py amazingdata.dividend --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 同步配股方案

```bash
python scripts/run_provider_sync.py amazingdata.right_issue --begin-date 20240101 --end-date 20241231 --limit 20 --force --log-level INFO
```

### 小范围验证日线

```bash
python scripts/run_provider_sync.py amazingdata.daily_kline --begin-date 20240101 --end-date 20240131 --limit 20 --force --log-level INFO
```

### 小范围验证 1 分钟

```bash
python scripts/run_provider_sync.py amazingdata.minute_kline --begin-date 20240115 --end-date 20240115 --limit 20 --force --log-level INFO
```

### 小范围验证快照

```bash
python scripts/run_provider_sync.py amazingdata.market_snapshot --begin-date 20240115 --end-date 20240115 --limit 20 --force --log-level INFO
```

## 表结构变更后的注意事项

当前代码会自动创建缺失表，不会主动删除已有表。

只有在你自己明确要做不兼容结构迁移时，才需要先手工处理旧表，例如：

```sql
DROP TABLE IF EXISTS ad_code_info;
DROP TABLE IF EXISTS ad_stock_basic;
DROP TABLE IF EXISTS ad_history_stock_status;
DROP TABLE IF EXISTS ad_market_kline_daily;
DROP TABLE IF EXISTS ad_market_kline_minute;
DROP TABLE IF EXISTS ad_market_snapshot;
DROP TABLE IF EXISTS ad_sync_task_log;
```

如果不再使用历史代码池，也可删除：

```sql
DROP TABLE IF EXISTS ad_hist_code_daily;
```
