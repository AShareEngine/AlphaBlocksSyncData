#!/usr/bin/env markdown

# BaoStock Runbook

这个项目现在新增了一条独立的 `BaoStock -> ClickHouse` 同步链路，不会和现有 `AmazingData` 任务混在一起。

目录说明：

- 统一入口：`scripts/run_provider_sync.py`
- BaoStock 全量配置：`config/sync/plans/run_sync.baostock.full.toml`
- 统一脚本入口：`scripts/run_provider_sync.py`
- 正式实现：`providers/baostock/runner.py`
- 规格定义：`providers/baostock/specs.py`
- Provider：`providers/baostock/provider.py`
- Repository：`providers/baostock/repository.py`
- 公共层：`sync_core/`

## 入口

命令入口：

```bash
python3 scripts/run_provider_sync.py --config run_sync.baostock.full.toml
```

也保留独立脚本入口：

```bash
python3 scripts/run_provider_sync.py baostock.<task> [options]
```

默认会使用现有 `CLICKHOUSE_*` 连接信息，把表写入 ClickHouse 的 `baostock` database。

## 当前股票池与历史股票池

- `--universe-mode current` 使用结束日附近的 BaoStock `query_all_stock` 快照，适合日常增量同步。
- `--universe-mode historical` 从 `starlight.ad_hist_code_daily` 读取请求区间内
  `security_type='EXTRA_STOCK_A'` 的历史股票代码，并与结束日附近的 BaoStock
  当前证券列表取并集，适合包含退市股票的历史回补。
- 对 `dividend_data` 和六张年/季频财务表，代码池会额外按
  `baostock.bs_stock_basic.type='1'` 限定为股票；这些任务不会把指数、ETF、
  可转债等 `query_all_stock` 证券拿去请求财务接口。日线任务仍保留全市场并集。
- `--universe-mode missing_historical` 仅保留请求区间内在 `bs_daily_kline`
  完全没有任何记录的历史 A 股。它不会重跑已有股票，也不检查已有股票内部的日期缺口。
- 若请求从 2010 年开始，应先确认 `ad_hist_code_daily` 也覆盖到 2010；否则可以从
  2010 请求行情，但无法发现已在该历史代码表起点前退市的股票。
- 历史模式要求先同步 `amazingdata.hist_code_list`；历史表不可用或请求区间为空时会
  明确失败，不会静默降级为当前股票池。
- 显式 `--codes` 始终优先于上述两种自动代码池。

示例：

```bash
python3 scripts/run_provider_sync.py baostock.daily_kline \
  --begin-date 20100101 \
  --end-date 20241231 \
  --universe-mode missing_historical \
  --force \
  --continue-on-error
```

BaoStock 登录默认走匿名账号：

- `BAOSTOCK_USER_ID=anonymous`
- `BAOSTOCK_PASSWORD=123456`

如需覆盖，可以写入 `config/runtime.local.yaml` 或环境变量。

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
python3 scripts/run_provider_sync.py --config run_sync.baostock.full.toml
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
python3 scripts/run_provider_sync.py baostock.trade_dates --begin-date 20240101 --end-date 20240131
```

全市场证券列表：

```bash
python3 scripts/run_provider_sync.py baostock.all_stock --day 20240110
```

股票基本资料：

```bash
python3 scripts/run_provider_sync.py baostock.stock_basic --codes 600000.SH,000001.SZ
```

前后复权因子对应的复权信息：

```bash
python3 scripts/run_provider_sync.py baostock.adjust_factor --codes 600000.SH --begin-date 20240101 --end-date 20241231
```

日线 K 线：

```bash
python3 scripts/run_provider_sync.py baostock.daily_kline --codes 600000.SH --begin-date 20240101 --end-date 20240131 --adjustflag 3
```

行业分类：

```bash
python3 scripts/run_provider_sync.py baostock.stock_industry --codes 600000.SH --day 20240110
```

沪深 300 成分：

```bash
python3 scripts/run_provider_sync.py baostock.hs300_stocks --day 20240110
```

季频财务类：

```bash
python3 scripts/run_provider_sync.py baostock.profit_data --codes 600000.SH --year 2023 --quarter 3
python3 scripts/run_provider_sync.py baostock.balance_data --codes 600000.SH --year 2023 --quarter 3
python3 scripts/run_provider_sync.py baostock.cash_flow_data --codes 600000.SH --year 2023 --quarter 3
```

季频财务历史回填（2010Q1 至最新已结束季度，包含历史退市股票）：

```bash
python3 scripts/run_provider_sync.py \
  --config providers/baostock/plans/historical-financial-backfill.toml
```

该计划会展开 `profit_data`、`operation_data`、`growth_data`、`dupont_data`、
`balance_data`、`cash_flow_data` 的全部季度，并按年度补齐 `dividend_data`。
历史已结束期间启用 `resume`，任务跨天或中断后重新执行时，会按
`task + code + year + quarter` 跳过任何日期已经成功的请求。当前仍处于披露期的
年度/季度不启用永久跳过，重新执行计划可以刷新后来披露的数据。

季频财务逐股票增量：

```bash
python3 scripts/run_provider_sync.py \
  --config providers/baostock/plans/financial-incremental.toml
```

这个配置不固定 `year/quarter`，由运行器一次批量读取每张目标表中各股票的最大业务日期：

- 股票没有任何记录：从 2010Q1 开始；分红从 2010 年开始。
- 股票已有记录但落后：季频表从游标后的季度开始；分红从游标所在年度开始。
- 股票已经到最近完成季度：跳过，不调用 BaoStock。
- 最近完成季度仍无返回时，只在当天跳过；以后再次执行会重新检查，避免披露期空结果被永久标记完成。
- 自动增量的历史代码池覆盖 2010 年至最近完成季度，并合并
  `ad_hist_code_daily.EXTRA_STOCK_A` 与 `bs_stock_basic.type='1'`，因此包含退市股票但排除指数和 ETF。

`/sync/freshness` 页面的上述七个 BaoStock 财务任务使用同一逻辑；页面按最近完成季度判断时效，
不会再把正在进行的季度当成应到日期。

### 按股票增量与整表增量

Provider 清单通过 `incremental_scope = "code"` 明确标记按股票/代码增量的任务。
从 `/sync/freshness` 立即同步或创建批量配置时，这类任务不会再使用目标表整体的
`max(date)` 覆盖开始日期，而是：

1. 将 `20100101` 作为无数据股票的历史下限。
2. 将结束日期设为当前应到交易日。
3. 由 Provider 查询每只股票自己的最新业务日期。
4. 无记录的股票从 2010 年开始；已有记录的股票从自身游标后开始；已最新的股票跳过。

BaoStock 的 `daily_kline`、`adjust_factor`、`performance_express_report`、
`forecast_report` 以及七张年/季频财务表都使用该模式。页面触发时，
BaoStock 和 AmazingData 支持历史股票池的股票任务还会合并
`ad_hist_code_daily.EXTRA_STOCK_A`，避免退市股票不进入任务。

该规则能发现“整只股票无数据”和“最新日期之后缺数据”，但仅凭最大日期不能发现
最新日期之前的内部断档。例如某股票已有 2026 年数据，但单独缺少 2022 年某天，
仍需要交易日历反连接的数据质量审计或定期重叠窗口回补。

宏观数据：

```bash
python3 scripts/run_provider_sync.py baostock.deposit_rate_data --begin-date 20230101 --end-date 20241231
python3 scripts/run_provider_sync.py baostock.money_supply_data_month --begin-date 202301 --end-date 202412
python3 scripts/run_provider_sync.py baostock.money_supply_data_year --begin-date 2020 --end-date 2024
```

## 如何同步

### 方式一：执行单个任务

适合补数、排障、验证接口。

```bash
python3 scripts/run_provider_sync.py --config run_sync.baostock.full.toml
python3 scripts/run_provider_sync.py baostock.stock_basic --codes 600000.SH,000001.SZ
python3 scripts/run_provider_sync.py baostock.daily_kline --codes 600000.SH --begin-date 20240101 --end-date 20240131
```

### 方式二：自动展开代码池批量同步

适合股票类批量任务。

```bash
python3 scripts/run_provider_sync.py baostock.daily_kline --begin-date 20240101 --end-date 20240131 --limit 100
```

说明：

- 不传 `--codes` 时，会先用 `all_stock` 展开代码池
- 然后逐个 code 顺序请求 BaoStock

### 方式三：容错批量执行

```bash
python3 scripts/run_provider_sync.py baostock.daily_kline --begin-date 20240101 --end-date 20240131 --limit 500
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
python3 scripts/run_provider_sync.py --config run_sync.amazingdata.full.toml >> logs/amazingdata.log 2>&1
```

终端 2：

```bash
python3 scripts/run_provider_sync.py --config run_sync.baostock.full.toml >> logs/baostock.log 2>&1
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
