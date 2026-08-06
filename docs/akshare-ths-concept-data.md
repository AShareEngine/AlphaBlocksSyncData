# AKShare 同花顺概念板块接入

`providers/akshare/` 提供以下三个可在数据同步项目中选择、手动执行或定时执行的任务：

| 任务 | ClickHouse 表 | 内容 |
| --- | --- | --- |
| `akshare.stock_board_concept_name_ths` | `akshare.ak_stock_board_concept_name_ths` | 同花顺概念名称和代码的每日目录快照 |
| `akshare.stock_board_concept_index_ths` | `akshare.ak_stock_board_concept_index_ths` | 每个概念板块的日频 OHLC、成交量和成交额 |
| `akshare.stock_board_concept_info_ths` | `akshare.ak_stock_board_concept_info_ths` | 今开、昨收、板块涨幅、排名、涨跌家数等每日简介快照 |

指数任务支持按概念独立增量。默认从概念目录取全部概念；传入 `codes` 时，既可以填写概念名称，
也可以填写同花顺概念代码。`limit` 用于限制本次处理的概念数量。

```bash
python3 scripts/run_provider_sync.py akshare.stock_board_concept_name_ths --force
python3 scripts/run_provider_sync.py akshare.stock_board_concept_index_ths \
  --codes 阿里巴巴概念,机器人概念 --begin-date 20200101 --force
python3 scripts/run_provider_sync.py akshare.stock_board_concept_info_ths \
  --codes 301558 --force
```

简介接口的“值”可能是数值、百分比或 `317/396` 形式的排名，因此统一以字符串保存，避免信息丢失。
全市场同步会逐个概念请求同花顺；建议先用 `limit` 小范围验证，并通过
`sync.akshare.request_interval_seconds` 控制请求间隔。
