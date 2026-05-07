#!/usr/bin/env markdown

# BaoStock Runbook

这个项目现在新增了一条独立的 `BaoStock -> ClickHouse` 同步链路，不会和现有 `AmazingData` 任务混在一起。

目录说明：

- 统一入口：`/Users/zhao/Desktop/git/sync_data_system/run_sync.py`
- BaoStock 全量配置：`/Users/zhao/Desktop/git/sync_data_system/run_sync.baostock.full.toml`
- 独立脚本入口：`/Users/zhao/Desktop/git/sync_data_system/scripts/run_baostock_sync.py`
- 正式实现：`/Users/zhao/Desktop/git/sync_data_system/sources/baostock/runner.py`
- 规格定义：`/Users/zhao/Desktop/git/sync_data_system/sources/baostock/specs.py`
- Provider：`/Users/zhao/Desktop/git/sync_data_system/sources/baostock/provider.py`
- Repository：`/Users/zhao/Desktop/git/sync_data_system/sources/baostock/repository.py`
- 公共层：`/Users/zhao/Desktop/git/sync_data_system/sync_core/`

## 入口

命令入口：

```bash
python3 run_sync.py --config run_sync.baostock.full.toml
```

也保留独立脚本入口：

```bash
python3 scripts/run_baostock_sync.py <task> [options]
```

默认会使用现有 `CLICKHOUSE_*` 连接信息，把表写入 ClickHouse 的 `baostock` database。

BaoStock 登录默认走匿名账号：

- `BAOSTOCK_USER_ID=anonymous`
- `BAOSTOCK_PASSWORD=123456`

如需覆盖，可以写入 `runtime.local.yaml` 或环境变量。

## 环境变量

BaoStock：

- `BAOSTOCK_USER_ID`
- `BAOSTOCK_PASSWORD`

ClickHouse：

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_USERNAME`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`

推荐先做一次连通性验证：

```bash
python3 run_sync.py --config run_sync.baostock.full.toml
```

## 已实现任务

- `trade_dates`
- `all_stock`
- `stock_basic`
- `adjust_factor`
- `daily_kline`
- `hs300_stocks`
- `sz50_stocks`
- `zz500_stocks`
- `stock_industry`
- `dividend_data`
- `profit_data`
- `operation_data`
- `growth_data`
- `dupont_data`
- `balance_data`
- `cash_flow_data`
- `performance_express_report`
- `forecast_report`
- `deposit_rate_data`
- `loan_rate_data`
- `required_reserve_ratio_data`
- `money_supply_data_month`
- `money_supply_data_year`

## 代码格式

BaoStock 原始代码格式是：

- `sh.600000`
- `sz.000001`

入库时统一转换成：

- `600000.SH`
- `000001.SZ`

同时保留原始字段 `source_code`。

## 常用示例

交易日历：

```bash
python3 scripts/run_baostock_sync.py trade_dates --begin-date 20240101 --end-date 20240131
```

全市场证券列表：

```bash
python3 scripts/run_baostock_sync.py all_stock --day 20240110
```

股票基本资料：

```bash
python3 scripts/run_baostock_sync.py stock_basic --codes 600000.SH,000001.SZ
```

前后复权因子对应的复权信息：

```bash
python3 scripts/run_baostock_sync.py adjust_factor --codes 600000.SH --begin-date 20240101 --end-date 20241231
```

日线 K 线：

```bash
python3 scripts/run_baostock_sync.py daily_kline --codes 600000.SH --begin-date 20240101 --end-date 20240131 --adjustflag 3
```

行业分类：

```bash
python3 scripts/run_baostock_sync.py stock_industry --codes 600000.SH --day 20240110
```

沪深 300 成分：

```bash
python3 scripts/run_baostock_sync.py hs300_stocks --day 20240110
```

季频财务类：

```bash
python3 scripts/run_baostock_sync.py profit_data --codes 600000.SH --year 2023 --quarter 3
python3 scripts/run_baostock_sync.py balance_data --codes 600000.SH --year 2023 --quarter 3
python3 scripts/run_baostock_sync.py cash_flow_data --codes 600000.SH --year 2023 --quarter 3
```

宏观数据：

```bash
python3 scripts/run_baostock_sync.py deposit_rate_data --begin-date 20230101 --end-date 20241231
python3 scripts/run_baostock_sync.py money_supply_data_month --begin-date 202301 --end-date 202412
python3 scripts/run_baostock_sync.py money_supply_data_year --begin-date 2020 --end-date 2024
```

## 如何同步

### 方式一：执行单个任务

适合补数、排障、验证接口。

```bash
python3 run_sync.py --config run_sync.baostock.full.toml
python3 scripts/run_baostock_sync.py stock_basic --codes 600000.SH,000001.SZ
python3 scripts/run_baostock_sync.py daily_kline --codes 600000.SH --begin-date 20240101 --end-date 20240131
```

### 方式二：自动展开代码池批量同步

适合股票类批量任务。

```bash
python3 scripts/run_baostock_sync.py daily_kline --begin-date 20240101 --end-date 20240131 --limit 100
```

说明：

- 不传 `--codes` 时，会先用 `all_stock` 展开代码池
- 然后逐个 code 顺序请求 BaoStock

### 方式三：容错批量执行

```bash
python3 scripts/run_baostock_sync.py daily_kline --begin-date 20240101 --end-date 20240131 --limit 500 --continue-on-error
```

说明：

- 单只 code 失败不会打断整批
- 成功和失败都会写到 `bs_sync_task_log` / `bs_sync_checkpoint`

## 行为说明

- 代码类任务不传 `--codes` 时，会先调用 `all_stock` 自动展开当日代码池，再逐个代码同步。
- 默认会记录同步结果到：
  - `baostock.bs_sync_task_log`
  - `baostock.bs_sync_checkpoint`
- `begin/end` 型接口现在会自动按业务表里最新业务日期做增量：
  - 例如 `daily_kline` 会查每个 code 当前最大 `date`
  - `adjust_factor` 会查每个 code 当前最大 `divid_operate_date`
  - 宏观时间序列会查各自表里的最大业务日期 / 月份 / 年份
  - 然后把下一次请求的开始时间自动推进到 `latest + 1`
- `day` / `year+quarter` / 单 code 静态类接口，当前按“请求参数对应的数据是否已存在”来跳过：
  - 例如 `all_stock --day 20240110`
  - `stock_basic --codes 600000.SH`
  - `profit_data --codes 600000.SH --year 2023 --quarter 3`
- 同一自然日内，同一个 `scope_key` 已成功执行过时，也会额外通过日志表做一次跳过保护；加 `--force` 可强制重跑。
- 代码类任务可加 `--continue-on-error`，单个 code 失败不会打断整批。

## 表设计

- 每个 BaoStock 接口一张独立表
- 原始返回字段按 snake_case 入库
- 为避免字段类型猜错，当前源字段统一按 `String` 保存
- `code` 字段统一写标准化后的 `600000.SH`
- 原始代码保留在 `source_code`

## 能不能和 AmazingData 同时同步

可以，当前支持 BaoStock 和 AmazingData 同时运行。

推荐方式：

终端 1：

```bash
python3 run_sync.py --config run_sync.amazingdata.full.toml >> logs/amazingdata.log 2>&1
```

终端 2：

```bash
python3 run_sync.py --config run_sync.baostock.full.toml >> logs/baostock.log 2>&1
```

原因：

- AmazingData 主要写 `ad_*` 表
- BaoStock 主要写 `baostock.bs_*` 表
- 两边日志和 checkpoint 分开

不建议：

- 同时开多个 BaoStock 批量进程跑同一类股票任务
- 同时开两个 BaoStock 进程跑大范围 `daily_kline` / `adjust_factor`

## BaoStock 请求量限制

BaoStock 有每日 API 请求量限制，超过后可能进入黑名单控制。

当前项目里的防范原则：

- 默认按单只 code 顺序请求，不做 BaoStock 内部并发
- 建议同一时间只跑一个 BaoStock 批量任务
- 先用 `--limit` 小批量验证，再逐步放大
- `daily_kline`、`adjust_factor` 已支持按业务表最新日期自动增量
- 静态类任务会按请求参数是否已存在进行跳过，减少重复请求

推荐做法：

- 日常只开一个 BaoStock 进程
- 优先跑增量，不要频繁 `--force`
- 真要重跑时，尽量缩小 `--codes` 或日期窗口
