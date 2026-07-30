# 免费美股数据接入

`providers/yfinance/` 在不接入 AKShare、无需付费 API Key 的前提下提供美股日线和分类数据：

- [yfinance](https://github.com/ranaroussi/yfinance)：日线、分红拆股、板块/概念 ETF 行情和 ETF Top Holdings
- [FinanceDatabase](https://github.com/JerBouma/FinanceDatabase)：美股 symbol 主表、交易所、Sector、Industry Group 和 Industry

## 任务与表

| 任务 | ClickHouse 表 | 数据说明 |
| --- | --- | --- |
| `yfinance.symbol_master` | `yf_symbol_master` | 美国主要交易所证券主表 |
| `yfinance.daily_kline` | `yf_daily_kline` | 未复权 OHLC、Adj Close、成交量 |
| `yfinance.corporate_actions` | `yf_corporate_actions` | 分红、拆股、Capital Gain |
| `yfinance.industry_membership` | `yf_industry_membership` | Sector / Industry Group / Industry 快照 |
| `yfinance.sector_daily` | `yf_sector_daily` | 11 个标准板块 ETF 日线 |
| `yfinance.concept_daily` | `yf_concept_daily` | AI、半导体、网络安全、清洁能源、生物科技代表 ETF 日线 |
| `yfinance.concept_membership` | `yf_concept_membership` | 代表 ETF 披露的 Top Holdings |
| `yfinance.income_statement` | `yf_income_statement` | 年度和季度利润表长表 |
| `yfinance.balance_sheet` | `yf_balance_sheet` | 年度和季度资产负债表长表 |
| `yfinance.cash_flow` | `yf_cash_flow` | 年度和季度现金流量表长表 |
| `yfinance.financial_metrics` | `yf_financial_metrics` | 估值、盈利能力、成长、股本和现金债务快照 |
| `yfinance.earnings_calendar` | `yf_earnings_calendar` | 财报时间、EPS 预期/实际和 Surprise |
| `yfinance.analyst_estimates` | `yf_analyst_estimates` | EPS、营收、增长、预测修正、评级分布和目标价 |
| `yfinance.institutional_holders` | `yf_institutional_holders` | 机构及共同基金持仓快照 |
| `yfinance.insider_transactions` | `yf_insider_transactions` | 公司内部人交易记录 |

这些业务表不保存 `source` 和 `fetched_at`：表所在的 `yfinance` 数据库已经能明确数据来源，
而增量进度由业务日期、`yf_symbol_cursor` 和 `yf_sync_checkpoint` 管理。旧版本表会在 Provider
初始化时自动删除 `source`；曾用 `fetched_at` 作为表引擎版本列的表会自动复制业务字段并换表，
原有业务数据会保留。大表首次迁移需要临时额外磁盘空间，迁移期间不要中断进程。

板块基准使用 `XLC/XLY/XLP/XLE/XLF/XLV/XLI/XLB/XLRE/XLK/XLU`。概念使用：

- 人工智能：`AIQ`，成分参考 `AIQ/BOTZ/ROBO`
- 半导体：`SMH`，成分参考 `SMH/SOXX`
- 网络安全：`CIBR`，成分参考 `CIBR/HACK`
- 清洁能源：`ICLN`，成分参考 `ICLN/TAN`
- 生物科技：`XBI`，成分参考 `XBI/IBB`

`concept_membership.membership_scope` 固定为 `top_holdings`。它是 ETF 公开的主要持仓，不是完整、权威的“概念股全集”。
如果 FinanceDatabase 没有返回任何行业分类，`industry_membership` 会失败；如果所有概念 ETF
持仓请求均失败或返回空数据，`concept_membership` 也会失败，不再把 `row_count=0` 记录为成功。
FinanceDatabase 降级原因和逐 ETF 异常会同时写入网页批任务日志。

## 配置

```yaml
sync:
  yfinance:
    proxy: "http://127.0.0.1:7890"
    batch_size: 5
    threads: false
    auto_adjust: false
    repair: false
    timeout: 30
    network_retries: 2
    request_interval_seconds: 2.0
    rate_limit_retries: 4
    rate_limit_backoff_seconds: 30.0
    rate_limit_max_backoff_seconds: 300.0
    rate_limit_jitter_seconds: 3.0
    active_symbols_only: true
    symbol_directory_timeout: 60
    default_start_date: "2010-01-01"
    include_otc: false
```

`proxy` 会同时用于 Yahoo Finance、FinanceDatabase 的 GitHub 文件和 Nasdaq Trader 证券目录，支持
普通 HTTP/HTTPS 代理地址；留空表示直连。代理必须能从实际运行 PM2 任务的服务器访问，本机的
`127.0.0.1` 代理不会自动转发到远程服务器。

Provider 会串行化 Yahoo 请求，每次调用至少间隔 `request_interval_seconds`。遇到 HTTP 429 或 `YFRateLimitError` 时，最多额外重试 `rate_limit_retries` 次，按 `rate_limit_backoff_seconds` 指数退避，并受最大退避时间和随机抖动限制。`network_retries` 是 yfinance 自身针对瞬时网络错误的重试次数。共享代理出口仍可能被 Yahoo 限制，建议保持 `threads: false`。

`active_symbols_only: true` 会使用 Nasdaq Trader 当日证券目录筛选当前正常上市的普通证券，排除
ETF、测试证券、异常上市状态、权证、Rights、Units，以及名称使用 `Preferred` 或英式
`Preference Shares` 的优先股。对非 Nasdaq 证券还会按 ACT Symbol 排除 `$` 优先股以及
`.U/.W/.V/.R` 的 Units、权证、When-Issued 和 Rights；交易所和市场字段以当日 Nasdaq Trader
目录为准，FinanceDatabase 继续提供公司与行业元数据。读取已有 `yf_symbol_master` 时也会再次执行
这层过滤，兼容修复前已经写入的旧快照。目录下载同样使用 `proxy`，获取失败时会停止生成股票池，
避免退回 FinanceDatabase 的历史全集后污染日线任务。

FinanceDatabase 会保留退市代码供历史研究，因此不建议关闭 `active_symbols_only`。关闭时只按精确的 NASDAQ、NYSE 和 NYSE American 市场名称过滤；`include_otc: true` 仅在关闭当前证券目录过滤时生效。

日线和公司行动任务优先复用 ClickHouse 最新的 `yf_symbol_master`，不会在每次运行时重新下载
FinanceDatabase。行业任务优先复用最近一个包含 Sector / Industry 的主表快照。只有主表为空或显式运行
`yfinance.symbol_master` 时才刷新远程股票主数据。

基本面任务同样优先复用 `yf_symbol_master` 股票池，也可以通过 `--codes` 只同步指定股票。三大财务
报表使用统一长表结构：`symbol/report_date/period_type/metric/value`，其中 `period_type` 为
`annual` 或 `quarterly`。这样 Yahoo 新增或删除财务科目时不需要修改 ClickHouse 表结构。
估值、分析师和机构持仓包含 `snapshot_date`，保留每次运行时的截面；财报日历和内部人交易按事件时间去重。

如果 FinanceDatabase 的 GitHub 文件不可达，但 Nasdaq Trader 可访问，`symbol_master` 会退回当前
上市普通证券目录，因此日线仍可执行；该备用目录不含 Sector / Industry 元数据，此时
`industry_membership` 会失败并输出明确诊断，不会把 0 行误报为成功。网络恢复或配置代理后重新运行
`yfinance.symbol_master`，再运行 `yfinance.industry_membership` 即可补齐行业数据。

## 运行

先安装该 Provider 的依赖：

```bash
python3 scripts/install_provider_deps.py yfinance --install
```

首次建议先做小批量验证：

```bash
python3 scripts/run_provider_sync.py yfinance.symbol_master --limit 20
python3 scripts/run_provider_sync.py yfinance.daily_kline --codes AAPL,MSFT --begin-date 20240101
python3 scripts/run_provider_sync.py yfinance.income_statement --codes AAPL,MSFT
python3 scripts/run_provider_sync.py yfinance.financial_metrics --codes AAPL,MSFT
```

执行完整计划：

```bash
python3 scripts/run_provider_sync.py --config providers/yfinance/plans/full.toml
python3 scripts/run_provider_sync.py --config providers/yfinance/plans/daily.toml
python3 scripts/run_provider_sync.py --config providers/yfinance/plans/fundamentals.toml
```

`fundamentals.toml` 包含 8 个逐股基本面任务，没有加入 `daily.toml`。完整股票池有数千只股票，
首次运行可能持续数小时并遇到部分证券无数据；建议先用 `--limit 10` 或 `--codes` 验证代理和
yfinance 版本。逐股异常会写入网页日志，任务继续处理其他股票，结束时若存在请求失败会保留已写入
行数并将任务标记失败；正常返回空数据的证券只记录汇总警告。

日线、公司行动、板块和概念行情会按 ClickHouse 中各 symbol/ETF 的最新日期增量续传。`--force` 会忽略游标，按传入的日期范围重跑。
所有行情任务的结束日期都会按 `America/New_York` 时区自动截断到最近已完成的美股交易日；
工作日美东时间 16:15 前不会请求当天日线，避免把盘中 OHLC 写成完整日线。周末会回退到周五，
交易所休市日则由 Yahoo 返回空数据。

Yahoo 批量下载偶尔只返回部分代码。同步器会识别缺少的 symbol 并逐个重试；已经有历史游标的
代码若本次没有新增数据会记录警告并继续，而从未成功写入过历史行情的代码在重试后仍为空时，
任务会以失败结束，并在同步日志中保留已经落库的行数和缺失 symbol，避免“部分数据”被误报为完整成功。
如果整批已有历史游标的代码都没有新交易日（例如节假日），则不会放大成逐股重试。

## 使用边界

yfinance 是对 Yahoo Finance 公开接口的开源封装。代码可以免费使用，但 Yahoo Finance 数据通常面向个人研究用途；生产商用、对外分发或对 SLA 有要求的场景，应另行确认数据授权并准备付费数据源作为替代。
