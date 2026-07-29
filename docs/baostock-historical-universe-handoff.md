# BaoStock 历史股票池遗漏修复交接

## 目标

修复 `AlphaBlocksSyncData` 在同步 BaoStock 历史日线时只使用结束日期（未指定时为当天）的证券列表，导致已经退市的历史股票没有进入同步任务，从而使 AlphaBlocks 长周期回测产生幸存者偏差的问题。

本任务只修改 `AlphaBlocksSyncData`。完成数据回补后，AlphaBlocks 侧还需要失效相关查询缓存并重新执行对齐回测。

## 实施确认（2026-07-29）

AmazingData 的历史代码接口已经在本项目中实现并落库：

- 任务：`amazingdata.hist_code_list`
- SDK：`BaseData.get_hist_code_list`
- 目标表：`starlight.ad_hist_code_daily`

当前库中该表有 14,187,962 行，覆盖 2013-01-04 至 2026-07-17。2020-05-06
按 `security_type` 拆分后：

- `EXTRA_STOCK_A`：3824 只
- `EXTRA_ETF_OP`：358 只

本文列出的六个退市样本均存在于 `EXTRA_STOCK_A` 历史代码表中。因此最终实现以
`starlight.ad_hist_code_daily` 的 `EXTRA_STOCK_A` 区间代码并集作为历史股票池来源，
不使用只返回最新快照的 `get_code_info/ad_code_info`。

### 财务表逐股票增量补齐

`dividend_data`、`profit_data`、`operation_data`、`growth_data`、`dupont_data`、
`balance_data`、`cash_flow_data` 在不显式传 `year/quarter` 时，已经改为：

- 每张表用一次 ClickHouse 分组查询批量取得全部股票的最新业务日期，不会逐股票发数据库查询。
- 单只股票无记录时从 2010 年开始。
- 季频表有记录但落后时，从该股票最新 `stat_date` 的下一季度同步到最近已完成季度。
- 已到最近完成季度的股票不调用 BaoStock。
- 历史模式的代码池覆盖 2010 年至最近完成季度，合并历史 A 股与
  `bs_stock_basic.type='1'`，包含退市股票并排除指数、ETF。
- `/sync/freshness` 页面触发这些任务时使用历史股票池，且按最近完成季度展示应到日期。

可直接执行：

```bash
python3 scripts/run_provider_sync.py \
  --config providers/baostock/plans/financial-incremental.toml
```

### 其他股票表的逐股票增量

同步任务清单新增 `incremental_scope = "code"`。BaoStock、AmazingData、QMT、
AKShare 和 yfinance 中按股票/代码执行且支持日期区间的任务，都会由 Provider
按单只股票游标决定开始日期。服务批量层不再把整张表的最大日期作为这些任务的开始日期。

`/sync/freshness` 的行为：

- 无数据股票以 `20100101` 为开始下限。
- 有数据股票由 Provider 推进到自身最新日期之后。
- 已最新股票跳过远端请求。
- BaoStock/AmazingData 支持历史股票池的股票任务使用
  `current UNION ad_hist_code_daily.EXTRA_STOCK_A`，包含退市股票。
- 快照表、宏观表和没有代码维度的表仍使用整表/请求级逻辑。

边界：最大日期游标只保证补齐尾部，不能证明最新日期之前没有内部日期缺口。
要保证内部连续性，需要额外按交易日历检查 `(code, trade_date)` 缺口。

## 已确认的现象

AlphaBlocks 对 2020-05-06 的数据覆盖审计结果：

- 历史股票主表：3824 只。
- BaoStock 行情/状态覆盖：3623 只。
- 缺少：201 只。
- 缺失样本：
  - `000005.SZ`
  - `000023.SZ`
  - `000038.SZ`
  - `000040.SZ`
  - `000046.SZ`
  - `000150.SZ`
- 上述样本在以下表的目标日期附近均没有数据：
  - `baostock.bs_daily_kline`
  - `starlight.ad_market_kline_daily`
  - `starlight.ad_history_stock_status`
  - `ab_factor.stock_daily_factor_source`

这些证券在 2020 年仍属于历史上市股票，但后来退市，因此是典型的历史股票池遗漏。

注意：这不是普通停牌过滤问题。`fetch_all_stock_codes()` 当前只读取 `code` 字段，没有按照 `tradeStatus` 过滤；当日停牌但仍在当日证券列表中的股票仍会进入任务。主要遗漏的是在选定快照日之前已经退市、因而不再出现在该日 `query_all_stock()` 结果中的证券。

## 修复前根因

### 1. 所有自动代码池任务只取一个快照日

文件：

- `providers/baostock/runner.py`

修复前 `resolve_code_list()` 的关键逻辑：

```python
snapshot_day = args.day or args.end_date or datetime.now().strftime("%Y%m%d")
resolved_day, codes = provider.fetch_latest_all_stock_codes(snapshot_day)
```

这意味着：

- 同步 `2010-01-01` 至今天时，代码池只包含今天仍存在的证券。
- 同步 `2020-01-01` 至 `2024-12-31` 时，代码池只包含 2024-12-31 当天存在的证券。
- 在结束日期前退市的证券不会调用 `query_history_k_data_plus()`，与 BaoStock 能否返回历史数据无关。
- `--force` 只影响增量游标和“今日已成功”跳过逻辑，不会改变代码池，因此不能修复遗漏。

### 2. 日常计划刚好触发“当前股票池”

文件：

- `providers/baostock/plans/daily.toml`

修复前配置只有：

```toml
begin_date = 20100101
```

没有 `end_date`，所以自动代码池使用当天日期。随后虽然每只现存证券可以从 2010 年开始补数据，但所有已退市证券都不会进入循环。

### 3. `stock_basic` 的批量能力没有被利用

文件：

- `providers/baostock/provider.py`
- `providers/baostock/specs.py`
- `providers/baostock/runner.py`

BaoStock 的 `query_stock_basic(code="", code_name="")` 允许代码为空，返回字段中包含：

- `code`
- `code_name`
- `ipoDate`
- `outDate`
- `type`
- `status`，其中 `0` 表示退市、`1` 表示上市

本机安装的 BaoStock Python 包也确认了 `query_stock_basic` 的函数签名为：

```python
(code='', code_name='')
```

但是修复前任务规格将 `stock_basic` 配置为：

```python
uses_code=True
auto_code_universe=True
```

因此运行器会先用当前 `query_all_stock` 股票池，再逐只调用 `query_stock_basic`。这形成了循环依赖：本来可以用 `stock_basic` 发现退市证券，但调用 `stock_basic` 前就已经用当前股票池把退市证券排除了。

## BaoStock 源站验证状态

2026-07-29 尝试直接登录 BaoStock，并对上述六个样本查询：

- `query_all_stock(day='2020-05-06')`
- `query_all_stock(day='2024-12-31')`
- `query_stock_basic(code=...)`
- `query_history_k_data_plus(..., start_date='2020-04-27', end_date='2020-05-12')`

源站连接返回：

```text
error_code=10002007
error_msg=网络接收错误。
```

因此当前还没有完成源站样本行数的实测。这个结果不能解释为“BaoStock 没有数据”，只是当时 BaoStock TCP 服务不可用。实施者应在能正常登录 BaoStock 的环境先完成下文的“小样本探针”，再运行全量回补。

## 已实施设计

### 行为原则

保留两种明确的代码池语义：

1. 当前增量同步：使用最近交易日的 `query_all_stock`，速度优先，不重复查询已经退市的证券。
2. 历史区间回补：使用与请求区间有上市时间交集的完整历史证券池，正确性优先。

不要让 `--force` 隐式决定代码池语义。`force` 应继续只表示忽略游标和已完成记录。

### 已实现的接口形式

已增加显式参数：

```text
universe_mode = current | historical | missing_historical
```

当前默认值：

- 日常计划：`current`
- 完整历史重建：`historical`
- 仅回补整个区间完全缺失的股票：`missing_historical`
- 显式传入 `--codes` 时忽略 `universe_mode`，始终使用用户代码。

统一 CLI、服务任务探针、API、任务批次和 TOML 计划均已传递该参数。

### 历史股票池构建

项目已有更直接的历史股票池来源：

```sql
SELECT code
FROM starlight.ad_hist_code_daily
WHERE security_type = 'EXTRA_STOCK_A'
  AND trade_date BETWEEN begin_date AND end_date
GROUP BY code
ORDER BY code
```

然后对代码规范化、去重、排序。

为了不破坏现有指数、ETF 等日线同步，历史模式的最终代码池建议使用：

```text
当前/结束日 query_all_stock 代码
UNION
请求区间内 ad_hist_code_daily 的 EXTRA_STOCK_A 代码
```

这样只补充历史退市股票，不会意外删除当前流程已经同步的指数、ETF、可转债等证券。

历史模式依赖 `amazingdata.hist_code_list` 已覆盖请求区间。如果历史表不可用或区间查询
为空，必须明确失败，不得降级成当前股票池。独立部署 BaoStock 且没有 AmazingData
历史表时，可再实现按月末或年度交易日调用 `query_all_stock(day=...)` 求并集的后备源。

不要只查询 `begin_date` 和 `end_date` 两个端点，因为区间中途上市又退市的证券可能两个端点都不存在。

### `stock_basic` 任务本身

当 `stock_basic` 未显式传入 `codes` 时，应允许一次空代码批量查询，而不是先生成当前代码池逐只查询。

没有简单地将 `uses_code` 改为 `False`，而是在任务规格中增加
`supports_bulk_without_code`，并由 `run_sync_args()` 识别批量分支。

显式传入 `codes` 时仍保持逐只查询。

### 增量与回补

历史模式第一次发现退市代码时，该代码在 `bs_daily_kline` 没有游标，现有增量逻辑会从请求的 `begin_date` 开始，原则上不需要 `force`。

实际生产回补仍建议使用 `--force`，确保：

- 不受错误的旧同步日志影响。
- 已部分写入的样本能按完整区间重跑。
- 验证时范围明确、结果可重复。

## 实施涉及文件

至少检查和修改：

- `providers/baostock/provider.py`
  - 增加空代码批量基础资料方法。
  - 必要时增加历史代码池缓存，避免同一批任务重复请求。
- `providers/baostock/runner.py`
  - 拆分当前代码池与历史代码池解析。
  - 保持显式 `codes` 最高优先级。
  - `limit` 应在合并、去重和稳定排序后应用。
- `providers/baostock/specs.py`
  - 如采用能力字段，增加批量无代码或代码池模式元数据。
- `service/task_registry.py`
  - 如增加 `universe_mode`，将参数加入任务注册、校验和探针对象。
- `scripts/run_provider_sync.py`
  - 如增加 `universe_mode`，增加 CLI 参数。
- `providers/baostock/plans/daily.toml`
  - 明确使用 `current`。
- 新增历史回补计划，例如：
  - `providers/baostock/plans/historical-backfill.toml`
- `tests/test_baostock_sync.py`
- `tests/test_service_task_registry.py`
- 与 CLI、配置校验相关的测试文件。
- `BAOSTOCK_RUNBOOK.md`
  - 说明当前增量与历史回补的区别。

## 必须补充的测试

### 代码池

1. 显式 `codes` 始终优先，不调用自动代码池。
2. 当前模式继续使用最近交易日 `query_all_stock`。
3. 当前模式不根据 `tradeStatus` 删除停牌证券。
4. 历史模式包含：
   - `ad_hist_code_daily` 请求区间内出现过的 `EXTRA_STOCK_A` 股票。
   - 请求区间中途退市的股票。
   - BaoStock 当前证券列表中的指数、ETF 等原有证券。
5. 历史模式排除：
   - 请求区间内没有记录的历史股票。
   - `EXTRA_ETF_OP` 等非 `EXTRA_STOCK_A` 历史代码。
6. 历史表不可用或区间内为空时明确失败。
7. 历史代码表查询严格使用请求的起止日期。
8. 合并结果去重并稳定排序。
9. `limit` 在最终代码池上应用。
10. 获取历史股票池失败时应明确失败，不能静默降级成当前股票池，否则会重新制造幸存者偏差。
11. `missing_historical` 只返回区间内完全没有日线记录的历史股票，不重跑已有股票。

### `stock_basic`

1. 未传 `codes` 时只调用一次空代码批量接口。
2. 显式传 `codes` 时继续逐只查询。
3. 批量结果包含 `status=0` 和非空 `outDate` 的退市样本。

### 增量行为

1. 退市代码无游标时从请求 `begin_date` 开始。
2. 已有游标时非 `force` 从下一日期开始。
3. `force` 不改变代码池模式，只重置增量窗口和跳过判断。

## 小样本源站探针

先在 BaoStock 能正常登录的机器上运行：

```bash
python3 scripts/run_provider_sync.py baostock.daily_kline \
  --codes 000005.SZ,000023.SZ,000038.SZ,000040.SZ,000046.SZ,000150.SZ \
  --begin-date 20200427 \
  --end-date 20200512 \
  --force
```

随后检查：

```sql
SELECT
    code,
    min(date) AS min_date,
    max(date) AS max_date,
    count() AS rows
FROM baostock.bs_daily_kline
WHERE code IN (
    '000005.SZ',
    '000023.SZ',
    '000038.SZ',
    '000040.SZ',
    '000046.SZ',
    '000150.SZ'
)
GROUP BY code
ORDER BY code;
```

验收要求：

- 至少确认 BaoStock 对其中若干在 2020 年实际交易的退市样本能够返回历史行。
- 如果某一代码仍为空，记录 BaoStock 的 `error_code/error_msg`，并与交易所历史核对；不能用单一空样本推断全部退市股票都不支持。

## 全量回补

样本验证通过后，使用历史代码池模式回补策略需要的完整区间。当前 JoinQuant 对齐样例主要需要 2020–2024：

```bash
python3 scripts/run_provider_sync.py baostock.daily_kline \
  --begin-date 20100101 \
  --end-date 20241231 \
  --universe-mode missing_historical \
  --force \
  --continue-on-error
```

统一 CLI 已实现 `--universe-mode` 和 `--continue-on-error`，也可以直接使用
`providers/baostock/plans/historical-backfill.toml` 回补计划。

建议先仅对计算出的“缺失历史代码集合”执行回补，确认正确性和耗时后再决定是否对完整历史池强制重跑。

## 数据回补后的下游操作

1. 补齐持久原始表：
   - `baostock.bs_daily_kline`
   - `starlight.ad_market_kline_daily`
   - `starlight.ad_history_stock_status`
2. 验证依赖这些原始表的 `ab_factor.stock_daily_factor_source` 视图。
3. AlphaBlocks 侧失效以下实体资产/查询缓存：
   - `market.daily_bars`
   - `market.current_state`
   - `valuation.daily_snapshot`
4. 先重新运行 2020Q2 输入对齐探针。
5. 只有在股票池数量、中位数阈值和选股结果接近 JoinQuant 后，再运行完整 2020–2024 回测。

`stock_daily_real` 不属于本次回补与验收范围。不能通过重建其他派生表来绕过原始表的数据缺口。

## AlphaBlocks 侧复验基线

修复前 2020-05-06 的关键基线：

```text
历史主表股票数：3824
BaoStock 行情/状态覆盖：3623
缺失：201
AlphaBlocks eligible：3474
```

回补后至少应验证：

- `bs_daily_kline` 对历史主表的覆盖显著增加。
- 上述六个缺失样本中，源站有数据的代码已经落库。
- 2020-05-06 的 eligible 数量不再因原始行情缺失而少 201 只。
- AlphaBlocks 与 JoinQuant 的基本面中位数和最终选股分叉缩小。

最终验收不是“同步任务显示成功”，而是：

```text
历史证券池覆盖正确
→ 原始行情存在
→ 宽表存在
→ AlphaBlocks 查询返回
→ JoinQuant 输入与回测结果对齐
```

## 建议测试命令

```bash
python3 -m pytest -q tests/test_baostock_sync.py
python3 -m pytest -q tests/test_service_task_registry.py
python3 -m pytest -q tests/test_service_api.py
python3 -m pytest -q tests/test_run_sync_resume.py
```

完成后再运行整个同步项目测试集。

## 非目标

- 不在本任务中修改 AlphaBlocks 回测撮合逻辑。
- 不使用当前股票池替代历史股票池并声称问题已解决。
- 不通过在下游宽表中伪造缺失行来绕过原始数据缺口。
- 不把停牌股票与退市股票混为一谈。
