# 数据查询接口调用文档

本文档只覆盖 REST 版 11 个查询类数据接口，不包含订阅管理、WebSocket、gRPC 和交易接口。

## 基础信息

默认服务地址：

```text
http://172.16.2.89:8000
```

统一接口前缀：

```text
/api/v1/data
```

所有查询接口都需要在请求头中传 API Key：

```text
Authorization: Bearer dev-api-key-001
Content-Type: application/json
```

不同运行模式默认 API Key：

| 运行模式 | 默认 API Key |
| --- | --- |
| mock | `mock-api-key-001` |
| dev | `dev-api-key-001` |
| prod | `prod-api-key-001` |

如果启动服务时设置过 `APP_API_KEYS=xxx,yyy`，以环境变量中的值为准。

## 通用返回结构

成功返回：

```json
{
  "success": true,
  "message": "获取全量 Tick 快照成功",
  "code": 200,
  "timestamp": "2026-05-15T17:25:19.963797",
  "data": {}
}
```

失败返回：

```json
{
  "success": false,
  "message": "无效的 API 密钥",
  "code": 401,
  "timestamp": "2026-05-15T17:25:19.963797",
  "data": {
    "error_code": "AUTHENTICATION_ERROR"
  }
}
```

常见错误：

| HTTP 状态码 | 含义 |
| --- | --- |
| 401 | 未传 token 或 token 不正确 |
| 400 | 请求参数或数据服务调用失败 |
| 422 | 请求体字段类型不对，或部分业务参数非法 |
| 503 | xtdata 不可用，通常是 QMT 未登录、路径配置错误或 xtquant 未连接成功 |
| 501 | 当前 QMT 客户端不支持对应能力 |

## 1. 获取 K 线历史

```text
POST /api/v1/data/kline-history
```

用途：查询一个或多个股票的历史 K 线数据。

请求体：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| symbols | string[] | 是 | 无 | 股票代码列表，例如 `["600000.SH", "000001.SZ"]` |
| period | string | 否 | `1d` | 周期，例如 `1d`、`1m`、`5m`，具体取决于 xtdata 支持范围 |
| start_time | string | 否 | `""` | 开始时间，常用格式 `YYYYMMDD` |
| end_time | string | 否 | `""` | 结束时间，常用格式 `YYYYMMDD` |
| fields | string[] | 否 | `[]` | 字段列表，空数组表示默认字段 |
| adjust_type | string | 否 | `none` | 复权类型，传给 xtdata 的 `dividend_type` |
| fill_data | boolean | 否 | `true` | 是否填充数据 |

示例：

```bash
curl -X POST "http://172.16.2.89:8000/api/v1/data/kline-history" \
  -H "Authorization: Bearer dev-api-key-001" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["600000.SH"],
    "period": "1d",
    "start_time": "20240101",
    "end_time": "20240131",
    "fields": [],
    "adjust_type": "none",
    "fill_data": true
  }'
```

返回 data：

```json
{
  "items": [
    {
      "symbol": "600000.SH",
      "fields": ["time", "open", "high", "low", "close", "volume"],
      "bars": [
        {
          "time_ms": 1704038400000,
          "open": 8.1,
          "high": 8.3,
          "low": 8.0,
          "close": 8.2,
          "volume": 100000,
          "amount": 820000.0,
          "settle": 0.0,
          "open_interest": 0,
          "pre_close": 8.0,
          "suspend_flag": 0
        }
      ]
    }
  ]
}
```

## 2. 获取 Tick 历史

```text
POST /api/v1/data/tick-history
```

用途：查询一个或多个股票的历史 tick 数据。

请求体：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| symbols | string[] | 是 | 无 | 股票代码列表 |
| start_time | string | 否 | `""` | 开始时间，常用格式 `YYYYMMDDHHMMSS` |
| end_time | string | 否 | `""` | 结束时间，常用格式 `YYYYMMDDHHMMSS` |
| fields | string[] | 否 | `[]` | 字段列表，空数组表示默认字段 |
| adjust_type | string | 否 | `none` | 复权类型 |

示例：

```bash
curl -X POST "http://172.16.2.89:8000/api/v1/data/tick-history" \
  -H "Authorization: Bearer dev-api-key-001" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["600000.SH"],
    "start_time": "20240101093000",
    "end_time": "20240101150000",
    "fields": [],
    "adjust_type": "none"
  }'
```

返回 data：

```json
{
  "items": [
    {
      "symbol": "600000.SH",
      "fields": ["time", "lastPrice", "open", "high", "low"],
      "ticks": [
        {
          "time_ms": 1704072600000,
          "last_price": 8.2,
          "open": 8.1,
          "high": 8.3,
          "low": 8.0,
          "last_close": 8.0,
          "amount": 1000000.0,
          "volume": 120000,
          "pvolume": 120000,
          "open_int": 0,
          "stock_status": 0,
          "last_settlement_price": 0.0,
          "ask_price": [8.21, 8.22],
          "bid_price": [8.19, 8.18],
          "ask_vol": [1000, 2000],
          "bid_vol": [1500, 2500],
          "transaction_num": 300
        }
      ]
    }
  ]
}
```

## 3. 获取全量 Tick 快照

```text
POST /api/v1/data/full-tick
```

用途：获取一个或多个股票的实时 full tick 快照。

请求体使用 `TickHistoryRequestModel`，但本接口实际只使用 `symbols`。

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| symbols | string[] | 是 | 无 | 股票代码列表 |
| start_time | string | 否 | `""` | 此接口不使用 |
| end_time | string | 否 | `""` | 此接口不使用 |
| fields | string[] | 否 | `[]` | 此接口不使用 |
| adjust_type | string | 否 | `none` | 此接口不使用 |

示例：

```bash
curl -X POST "http://172.16.2.89:8000/api/v1/data/full-tick" \
  -H "Authorization: Bearer dev-api-key-001" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["600000.SH", "000001.SZ"]
  }'
```

返回 data：

```json
{
  "items": [
    {
      "symbol": "600000.SH",
      "tick": {
        "time_ms": 1778840719963,
        "last_price": 8.2,
        "open": 8.1,
        "high": 8.3,
        "low": 8.0,
        "last_close": 8.0,
        "amount": 1000000.0,
        "volume": 120000,
        "pvolume": 120000,
        "open_int": 0,
        "stock_status": 0,
        "last_settlement_price": 0.0,
        "ask_price": [8.21, 8.22],
        "bid_price": [8.19, 8.18],
        "ask_vol": [1000, 2000],
        "bid_vol": [1500, 2500],
        "transaction_num": 300
      }
    }
  ]
}
```

## 4. 获取财务数据

```text
POST /api/v1/data/financial
```

用途：查询股票财务表数据。

请求体：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| symbols | string[] | 是 | 无 | 股票代码列表 |
| table_names | string[] | 是 | 无 | 财务表名列表，例如 `["Balance"]` |
| start_time | string | 否 | `""` | 开始时间 |
| end_time | string | 否 | `""` | 结束时间 |

示例：

```bash
curl -X POST "http://172.16.2.89:8000/api/v1/data/financial" \
  -H "Authorization: Bearer dev-api-key-001" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["600000.SH"],
    "table_names": ["Balance"],
    "start_time": "20230101",
    "end_time": "20241231"
  }'
```

返回 data：

```json
{
  "items": [
    {
      "symbol": "600000.SH",
      "table_name": "Balance",
      "columns": ["date", "total_assets"],
      "rows": [
        {
          "date": "20241231",
          "total_assets": "100000000"
        }
      ]
    }
  ]
}
```

说明：`rows` 内字段由 xtdata 返回的财务表决定，服务端会把字段值转为字符串。

## 5. 获取合约信息

```text
GET /api/v1/data/instrument/{symbol}
```

用途：查询单个证券/合约的基础信息。

路径参数：

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| symbol | string | 是 | 股票或合约代码，例如 `600000.SH` |

Query 参数：

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| complete | boolean | 否 | `false` | 是否返回完整信息 |

示例：

```bash
curl -X GET "http://172.16.2.89:8000/api/v1/data/instrument/600000.SH?complete=false" \
  -H "Authorization: Bearer dev-api-key-001"
```

返回 data：

```json
{
  "symbol": "600000.SH",
  "fields": {
    "InstrumentID": "600000.SH",
    "InstrumentName": "浦发银行"
  }
}
```

说明：`fields` 的具体字段由 xtdata 返回决定，服务端统一转为字符串。

## 6. 获取交易日历

```text
POST /api/v1/data/trading-calendar
```

用途：查询指定市场的交易日历。

请求体：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| market | string | 是 | 无 | 市场，例如 `SH`、`SZ` |
| start_time | string | 否 | `""` | 开始日期，常用格式 `YYYYMMDD` |
| end_time | string | 否 | `""` | 结束日期，常用格式 `YYYYMMDD` |

示例：

```bash
curl -X POST "http://172.16.2.89:8000/api/v1/data/trading-calendar" \
  -H "Authorization: Bearer dev-api-key-001" \
  -H "Content-Type: application/json" \
  -d '{
    "market": "SH",
    "start_time": "20240101",
    "end_time": "20240131"
  }'
```

返回 data：

```json
{
  "market": "SH",
  "dates": ["20240102", "20240103", "20240104"]
}
```

说明：部分 QMT/xtdata 版本可能不支持该能力，可能返回 501 和 `FEATURE_NOT_SUPPORTED`。

## 7. 获取指数权重

```text
POST /api/v1/data/index-weight
```

用途：查询指数成分股及权重。

请求体：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| index_code | string | 是 | 无 | 指数代码，例如 `000300.SH` |

示例：

```bash
curl -X POST "http://172.16.2.89:8000/api/v1/data/index-weight" \
  -H "Authorization: Bearer dev-api-key-001" \
  -H "Content-Type: application/json" \
  -d '{
    "index_code": "000300.SH"
  }'
```

返回 data：

```json
{
  "index_code": "000300.SH",
  "components": [
    {
      "symbol": "600000.SH",
      "weight": 0.2
    }
  ]
}
```

## 8. 获取板块列表

```text
GET /api/v1/data/sectors
```

用途：查询 QMT 中可用板块，以及每个板块下的股票列表。这个接口也可以用于获取全部股票列表，比如从 `沪深A股`、`上证A股`、`深证A股` 等板块中取 `symbols`。

Query 参数：

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| sector_name | string | 否 | 无 | 指定板块名称；传入后只查询该板块股票，不遍历全部板块 |

示例：

```bash
curl -X GET "http://172.16.2.89:8000/api/v1/data/sectors" \
  -H "Authorization: Bearer dev-api-key-001"
```

只获取 `沪深A股`：

```bash
curl -G "http://172.16.2.89:8000/api/v1/data/sectors" \
  --data-urlencode "sector_name=沪深A股" \
  -H "Authorization: Bearer dev-api-key-001"
```

返回 data：

```json
{
  "items": [
    {
      "sector_name": "沪深A股",
      "symbols": ["000001.SZ", "600000.SH"]
    }
  ]
}
```

说明：板块名称由本机 QMT/xtdata 返回，具体名称可能因版本、数据源、登录状态不同而不同。

## 9. 获取 L2 快照

```text
POST /api/v1/data/l2/quote
```

用途：查询 L2 快照数据。

请求体：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| symbols | string[] | 是 | 无 | 股票代码列表 |
| start_time | string | 否 | `""` | 开始时间 |
| end_time | string | 否 | `""` | 结束时间 |

示例：

```bash
curl -X POST "http://172.16.2.89:8000/api/v1/data/l2/quote" \
  -H "Authorization: Bearer dev-api-key-001" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["600000.SH"],
    "start_time": "",
    "end_time": ""
  }'
```

返回 data：

```json
{
  "items": [
    {
      "symbol": "600000.SH",
      "quote": {
        "time_ms": 1778840719963,
        "last_price": 8.2,
        "open": 8.1,
        "high": 8.3,
        "low": 8.0,
        "last_close": 8.0,
        "amount": 1000000.0,
        "volume": 120000,
        "pvolume": 120000,
        "open_int": 0,
        "stock_status": 0,
        "last_settlement_price": 0.0,
        "ask_price": [8.21, 8.22],
        "bid_price": [8.19, 8.18],
        "ask_vol": [1000, 2000],
        "bid_vol": [1500, 2500],
        "transaction_num": 300
      }
    }
  ]
}
```

说明：L2 数据依赖 QMT 数据权限；没有权限或本地环境不支持时，可能返回空数组或数据服务错误。

## 10. 获取 L2 逐笔委托

```text
POST /api/v1/data/l2/order
```

用途：查询 L2 逐笔委托数据。

请求体：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| symbols | string[] | 是 | 无 | 股票代码列表 |
| start_time | string | 否 | `""` | 开始时间 |
| end_time | string | 否 | `""` | 结束时间 |

示例：

```bash
curl -X POST "http://172.16.2.89:8000/api/v1/data/l2/order" \
  -H "Authorization: Bearer dev-api-key-001" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["600000.SH"],
    "start_time": "",
    "end_time": ""
  }'
```

返回 data：

```json
{
  "items": [
    {
      "symbol": "600000.SH",
      "orders": [
        {
          "time_ms": 1778840719963,
          "price": 8.2,
          "volume": 1000,
          "entrust_no": 12345,
          "entrust_type": 1,
          "entrust_direction": 1
        }
      ]
    }
  ]
}
```

## 11. 获取 L2 逐笔成交

```text
POST /api/v1/data/l2/transaction
```

用途：查询 L2 逐笔成交数据。

请求体：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| symbols | string[] | 是 | 无 | 股票代码列表 |
| start_time | string | 否 | `""` | 开始时间 |
| end_time | string | 否 | `""` | 结束时间 |

示例：

```bash
curl -X POST "http://172.16.2.89:8000/api/v1/data/l2/transaction" \
  -H "Authorization: Bearer dev-api-key-001" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["600000.SH"],
    "start_time": "",
    "end_time": ""
  }'
```

返回 data：

```json
{
  "items": [
    {
      "symbol": "600000.SH",
      "transactions": [
        {
          "time_ms": 1778840719963,
          "price": 8.2,
          "volume": 1000,
          "amount": 8200.0,
          "trade_index": 1,
          "buy_no": 100,
          "sell_no": 200,
          "trade_type": 1,
          "trade_flag": 1
        }
      ]
    }
  ]
}
```

## Postman 调用要点

1. Method 按接口选择 `GET` 或 `POST`。
2. Headers 添加：

```text
Authorization: Bearer dev-api-key-001
Content-Type: application/json
```

3. `POST` 接口进入 Body，选择 `raw`，格式选 `JSON`。
4. `GET /instrument/{symbol}` 的 `complete` 放在 Params 中。
5. REST 接口 token 不放 Params，只有 WebSocket 才使用 `?token=...`。

## 字段速查

常见 tick 字段：

| 字段 | 说明 |
| --- | --- |
| time_ms | 时间戳，毫秒 |
| last_price | 最新价 |
| open | 开盘价 |
| high | 最高价 |
| low | 最低价 |
| last_close | 昨收价 |
| amount | 成交额 |
| volume | 成交量 |
| pvolume | 原始成交量字段 |
| open_int | 持仓量，股票通常为 0 |
| stock_status | 股票状态 |
| ask_price | 卖价数组 |
| bid_price | 买价数组 |
| ask_vol | 卖量数组 |
| bid_vol | 买量数组 |
| transaction_num | 成交笔数 |

常见 K 线字段：

| 字段 | 说明 |
| --- | --- |
| time_ms | K 线时间，毫秒 |
| open | 开盘价 |
| high | 最高价 |
| low | 最低价 |
| close | 收盘价 |
| volume | 成交量 |
| amount | 成交额 |
| settle | 结算价 |
| open_interest | 持仓量 |
| pre_close | 前收盘价 |
| suspend_flag | 停牌标记 |

## 新增只读查询接口

以下接口只做查询，不触发 `download_*` 下载或补数据。若当前 QMT/xtdata 客户端没有对应函数，接口会返回 501，错误码为 `FEATURE_NOT_SUPPORTED`。

### 获取扩展行情数据

```text
POST /api/v1/data/market-data-ex
```

请求体：

```json
{
  "symbols": ["600000.SH"],
  "period": "1d",
  "start_time": "20240101",
  "end_time": "20240131",
  "count": -1,
  "fields": [],
  "adjust_type": "none",
  "fill_data": true
}
```

### 获取本地行情数据

```text
POST /api/v1/data/local-data
```

请求体同 `/market-data-ex`。

### 获取最新交易日 K 线

```text
POST /api/v1/data/full-kline
```

请求体同 `/market-data-ex`。

### 获取合约类型

```text
GET /api/v1/data/instrument-type/{symbol}
```

示例：

```bash
curl "http://172.16.2.89:8000/api/v1/data/instrument-type/600000.SH" \
  -H "Authorization: Bearer dev-api-key-001"
```

### 获取交易时间段

```text
GET /api/v1/data/trade-times/{symbol}
```

### 获取主力合约

```text
GET /api/v1/data/main-contract/{code_market}
```

### 获取交易日列表

```text
POST /api/v1/data/trading-dates
```

请求体：

```json
{
  "market": "SH",
  "start_time": "20240101",
  "end_time": "20240131",
  "count": -1
}
```

### 获取节假日列表

```text
GET /api/v1/data/holidays
```

### 获取可用周期列表

```text
GET /api/v1/data/periods
```

### 获取数据目录

```text
GET /api/v1/data/data-dir
```

### 获取除权除息数据

```text
POST /api/v1/data/divid-factors
```

请求体：

```json
{
  "stock_code": "600000.SH",
  "start_time": "",
  "end_time": ""
}
```

### 获取可转债信息

```text
GET /api/v1/data/cb-info/{symbol}
```

### 获取新股申购信息

```text
GET /api/v1/data/ipo-info
```

### 获取 ETF 信息

```text
GET /api/v1/data/etf-info/{symbol}
```

## 下载接口

以下接口会显式触发 xtdata 的 `download_*` 能力，把数据同步到本地 QMT/xtdata 数据目录。查询接口不会自动下载；需要补齐本地数据时再调用这些接口。

下载接口同样需要：

```text
Authorization: Bearer dev-api-key-001
Content-Type: application/json
```

如果当前 QMT/xtdata 客户端没有对应能力，会返回 501 和 `FEATURE_NOT_SUPPORTED`。

### 下载单只历史行情

```text
POST /api/v1/data/download/history
```

请求体：

```json
{
  "stock_code": "600000.SH",
  "period": "1d",
  "start_time": "20240101",
  "end_time": "20240131",
  "incrementally": false
}
```

### 批量下载历史行情

```text
POST /api/v1/data/download/history/batch
```

请求体：

```json
{
  "symbols": ["600000.SH", "000001.SZ"],
  "period": "1d",
  "start_time": "20240101",
  "end_time": "20240131",
  "incrementally": false
}
```

### 下载财务数据

```text
POST /api/v1/data/download/financial
```

请求体：

```json
{
  "symbols": ["600000.SH"],
  "table_names": ["Balance", "Income", "CashFlow"],
  "start_time": "",
  "end_time": ""
}
```

下载完成后再调用：

```text
POST /api/v1/data/financial
```

### 下载指数权重

```text
POST /api/v1/data/download/index-weight
```

请求体：

```json
{
  "index_code": "000300.SH"
}
```

`index_code` 为空时由当前 xtdata 客户端决定是否下载全部。

### 下载历史合约信息

```text
POST /api/v1/data/download/history-contracts
```

请求体：

```json
{
  "market": "SH"
}
```

### 下载板块数据

```text
POST /api/v1/data/download/sector
```

请求体：

```json
{
  "sector_name": "沪深A股"
}
```

### 下载节假日数据

```text
POST /api/v1/data/download/holiday
```

请求体为空。

### 下载可转债数据

```text
POST /api/v1/data/download/cb
```

请求体为空。

### 下载 ETF 数据

```text
POST /api/v1/data/download/etf
```

请求体为空。

### 下载接口返回结构

成功时：

```json
{
  "success": true,
  "message": "下载财务数据成功",
  "code": 200,
  "data": {
    "function": "download_financial_data2",
    "success": true,
    "result": null
  }
}
```

说明：不同 xtdata 函数的返回值不完全一致，`result` 可能是 `null`、布尔值、字符串、列表或字典。判断是否可用以 HTTP 状态和 `success` 为准。
