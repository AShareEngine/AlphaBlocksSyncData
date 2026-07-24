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

板块基准使用 `XLC/XLY/XLP/XLE/XLF/XLV/XLI/XLB/XLRE/XLK/XLU`。概念使用：

- 人工智能：`AIQ`，成分参考 `AIQ/BOTZ/ROBO`
- 半导体：`SMH`，成分参考 `SMH/SOXX`
- 网络安全：`CIBR`，成分参考 `CIBR/HACK`
- 清洁能源：`ICLN`，成分参考 `ICLN/TAN`
- 生物科技：`XBI`，成分参考 `XBI/IBB`

`concept_membership.membership_scope` 固定为 `top_holdings`。它是 ETF 公开的主要持仓，不是完整、权威的“概念股全集”。

## 配置

```yaml
sync:
  yfinance:
    batch_size: 100
    threads: true
    auto_adjust: false
    repair: false
    timeout: 30
    default_start_date: "2010-01-01"
    include_otc: false
```

默认只保留 NASDAQ、NYSE 和 NYSE American 等主要美国交易所；`include_otc: true` 会同时接受可识别的 OTC 市场证券。

## 运行

先安装该 Provider 的依赖：

```bash
python3 scripts/install_provider_deps.py yfinance --install
```

首次建议先做小批量验证：

```bash
python3 scripts/run_provider_sync.py yfinance.symbol_master --limit 20
python3 scripts/run_provider_sync.py yfinance.daily_kline --codes AAPL,MSFT --begin-date 20240101
```

执行完整计划：

```bash
python3 scripts/run_provider_sync.py --config providers/yfinance/plans/full.toml
python3 scripts/run_provider_sync.py --config providers/yfinance/plans/daily.toml
```

日线、公司行动、板块和概念行情会按 ClickHouse 中各 symbol/ETF 的最新日期增量续传。`--force` 会忽略游标，按传入的日期范围重跑。

## 使用边界

yfinance 是对 Yahoo Finance 公开接口的开源封装。代码可以免费使用，但 Yahoo Finance 数据通常面向个人研究用途；生产商用、对外分发或对 SLA 有要求的场景，应另行确认数据授权并准备付费数据源作为替代。
