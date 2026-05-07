# AmazingData 开发手册 API 分组版

这份文档基于 `AmazingData开发手册.docx` 整理，只保留：调用方式、输入参数、返回参数。

模块分组：基础数据 / 行情 / 财务 / 股东 / 指数 / 行业 / ETF / 可转债 / 期权 / 国债

## 基础数据

## login 功能描述：api 登陆输入参数：

- 章节: `3.5.1.1登录` 调用任何数据接口之前，必须先调用登录接口。

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| 类型 | 说明 | username | str |
| 用户名 |  |  |  |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| code_info | dataframe | index 为股票代码column 为symbol (证券简称) security_status（产品状态标志）pre_close (昨收价) high_limited  (涨停价) low_limited ( 跌停价) price_tick (最小价格变动单位) |

## logout 功能描述：api 退出登录链接，必须在登录状态下，才可使用；正常使用情况下，无需使用此接口

- 章节: `3.5.1.2登出` 函数接口：logout 功能描述：api 退出登录链接，必须在登录状态下，才可使用；正常使用情况下，无需使用此接口

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() code_info = base_data_object.get_code_info(security_type='EXTRA_ETF')
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| 类型 | 说明 | username | str |
| 用户名 |  |  |  |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| code_info | dataframe | index 为股票代码column 为symbol (证券简称) security_status（产品状态标志）pre_close (昨收价) high_limited  (涨停价) low_limited ( 跌停价) price_tick (最小价格变动单位) |

## update_password功能描述：更新密码接口，必须先登录才能修改密码

- 章节: `3.5.1.3更新密码` 函数接口：update_password功能描述：更新密码接口，必须先登录才能修改密码

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() code_info = base_data_object.get_code_info(security_type='EXTRA_ETF')
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| 类型 | 说明 | username | str |
| 用户名 | old_password | str | 旧密码 |
| new_password | str | 新密码 |  |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| code_info | dataframe | index 为股票代码column 为symbol (证券简称) security_status（产品状态标志）pre_close (昨收价) high_limited  (涨停价) low_limited ( 跌停价) price_tick (最小价格变动单位) |

## get_code_info

- 章节: `3.5.2.1每日最新证券信息` 函数接口：get_code_info
- 功能: 获取每日最新证券信息，交易日早上9 点前更新当日最新输入：

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() code_info = base_data_object.get_code_info(security_type='EXTRA_ETF')
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| security_type | str | 否 | 代码类型security_type（见附录），默认为EXTRA_STOCK_A（上交所A 股、深交所A 股和北交所的股票列表） |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| code_info | dataframe | index 为股票代码column 为symbol (证券简称) security_status（产品状态标志）pre_close (昨收价) high_limited  (涨停价) low_limited ( 跌停价) price_tick (最小价格变动单位) |

## get_code_list

- 章节: `3.5.2.2每日最新代码表（沪深北）` 交易日早上9 点前更新
- 功能: 获取代码表（每日最新），此接口无法获取历史代码表

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A')
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| security_type | str | 否 | 代码类型security_type（见附录），默认为EXTRA_STOCK_A（上交所A 股、深交所A 股和北交所的股票列表） |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| code_list | list | 证券代码 |

## BaseData.get_backward_factor 功能描述：获取复权因子数据并本地存储，复权因子为根据交易所行情数据计算得出的后复

- 章节: `3.5.2.5复权因子（后复权因子）` 函数接口：BaseData.get_backward_factor 功能描述：获取复权因子数据并本地存储，复权因子为根据交易所行情数据计算得出的后复

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A') backward_factor = base_data_object.get_backward_factor(code_list, local_path='D://AmazingData_local_data//', is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | lis[str] | 是 | 代码列表，支持股票、ETF |
| local_path | str | 是 | 本地存储复权因子数据的文件夹地址 |
| is_local | Bool | 是 | 是否使用本地存储的数据，默认为True |
| 注：（1）local_path 类似'D://AmazingData_local_data//'，只写文件夹的绝对路径即可 | （2）is_local True: 本地local_path 有数据的情况下，从本地取数据，但无法从服务端获取最新的数据本地local_path 无数据的情况下，从互联网取数据，并更新本地local_path 的数据False:从互联网取数据，并更新本地local_path 的数据输出： | 参数 | 数据类型 |
| 解释 | backward_factor | dataframe | index 为交易日期column 为股票代码 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| code_list | List[str] | 证券代码 |

## BaseData.get_adj_factor 功能描述：获取复权因子数据并本地存储，复权因子为根据交易所行情数据计算得出的单次

- 章节: `3.5.2.6复权因子（单次复权因子）` 函数接口：BaseData.get_adj_factor 功能描述：获取复权因子数据并本地存储，复权因子为根据交易所行情数据计算得出的单次

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A')
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | lis[str] | 是 | 代码列表，支持股票、ETF |
| local_path | str | 是 | 本地存储复权因子数据的文件夹地址 |
| is_local | Bool | 是 | 是否使用本地存储的数据，默认为True |
| 注：（1）local_path 类似'D://AmazingData_local_data//'，只写文件夹的绝对路径即可 | （2）is_local True: 本地local_path 有数据的情况下，从本地取数据，但有可能无法获取最新的数据本地local_path 无数据的情况下，从互联网取数据，并更新本地local_path 的数据False:从互联网取数据，并更新本地local_path 的数据输出： | 参数 | 数据类型 |
| 解释 | adj_factor | dataframe | index 为交易日期column 为股票代码 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| code_list | List[str] | 证券代码 |

## BaseData 的get_hist_code_list 功能描述：获取历史代码表，先检查本地数据，再从服务端补充，最后返回数据输入参数：

- 章节: `3.5.2.7历史代码表` 函数接口：BaseData 的get_hist_code_list 功能描述：获取历史代码表，先检查本地数据，再从服务端补充，最后返回数据输入参数：

### 调用方式

```python
BaseData 的get_hist_code_list 功能描述：获取历史代码表，先检查本地数据，再从服务端补充，最后返回数据输入参数：
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| security_type | str | 是 | 默认为"EXTRA_STOCK_A_SH_SZ"  沪深A 股，支持附录security_type(沪深北)和security_type(期货交易所)， |
| start_date | int | 是 | 开始时间，闭区间 |
| end_date | int | 是 | 结束时间，闭区间 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//'” |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| code_list | List[str] | 证券代码 |

## get_calendar

- 章节: `3.5.2.8交易日历` 函数接口：get_calendar
- 功能: 获取交易所的交易日历

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() calendar = base_data_object.get_calendar()
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| data_type | str | 否 | 选择返回数据的类型，默认为str ，可选datetime 或 str |
| market | str | 否 | 选择市场market（见附录），默认为SH（上海） |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| calendar | List[int] | 日期 |

## get_stock_basic

- 章节: `3.5.2.9证券基础信息` 函数接口：get_stock_basic
- 功能: 获取指定股票列表的上市公司的证券基础数据，包含沪深北三个交易所，所有股

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A_SH_SZ') info_data_object = ad.InfoData() stock_basic = info_data_object.get_stock_basic (code_list)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深北三个交易所的代码列表，可见示例 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| stock_basic | dataframe | column 为stock_basic 的字段index 为序号（无意义） |
| stock_basic 的字段说明： |  |  |

## get_history_stock_status

- 章节: `3.5.2.10` 参数
- 功能: 获取指定股票列表的上市公司的历史证券数据，以日度为频率，包含历史的涨跌

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() calendar = base_data_object.get_calendar() today = calendar[-1] all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101, end_date=today) history_stock_status = info_data_object.get_history_stock_status(all_code_list)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“D://AmazingData_local_data//” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 交易日，本地数据缓存方案 |
| end_date | int | 否 | 交易日，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| history_stock_status | dataframe | column 为history_stock_status 的字段index 为序号（无意义） |
| history_stock_status 的字段说明： | 参数 | 数据类型 |
| 必选 | 解释 | MARKET_CODE |
| string | 证券代码 |  |

## get_bj_code_mapping

- 章节: `3.5.2.11` TRADE_DATE
- 功能: 获取北交所的存量上市公司股票新旧代码对照表

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() bj_code_mapping = info_data_object.get_bj_code_mapping()
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，首选从本地读取，读取失败再从服务器取数据False，以本地数据为基础，增量从服务器取数据 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| bj_code_map ping | dataframe | column 为bj_code_mapping 的字段index 为序号（无意义） |

## 行情

## query_snapshot

- 章节: `3.5.4.1历史快照` 函数接口：query_snapshot
- 功能: 快照数据的历史数据查询接口

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A ') calendar = base_data_object.get_calendar() market_data_object=ad.MarketData(calendar) snapshot_dict = market_data_object.query_snapshot(code_list, begin_date=20240530, end_date=20240530)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list:[str] | 是 | 可传入列表，支持北交所、上交所、深交所的可转债、股票、指数、ETF、港股通等、ETF 期权等品种 |
| begin_date | int | 是 | 日期，填写8 位的整型格式的日期，比如20240101 |
| end_date | int | 是 | 日期，填写8 位的整型格式的日期，比如20240201 |
| begin_time | int | 否 | 时分秒毫秒的时间戳，填写8 位或9 位的 |
| 整型格式的日期，时占一位或两位，分占两位，秒占两位，毫秒占三位，例如9 点整为90000000, 17 点25 分为172500000 | end_time | int | 否 |
| 时分秒毫秒的时间戳，填写8 位或9 位的整型格式的日期，时占一位或两位，分占两位，秒占两位，毫秒占三位，例如9 点整为90000000, 17 点25 分为172500000 |  |  |  |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| snapshot_dict | dict | 指字典的key：代码字典的value：dataframe，column 为快照数据（指数为SnapshotIndex（见附录），股票、ETF 和可转债为Snapshot（见附录），港股通为SnapshotHKT（见附录）），ETF 期权为SnapshotOption（见附录））， |
| index 为日期（datetime） |  |  |

## query_kline

- 章节: `3.5.4.2历史K 线` 函数接口：query_kline
- 功能: K 线数据的实时订阅回调函数，支持全部周期的K 线数据查询

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A') calendar = base_data_object.get_calendar() market_data_object=ad.MarketData(calendar) kline_dict = market_data_object.query_kline (code_list, begin_date=20240530, end_date=20240530)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list:[str] | 是 | 可传入列表，支持北交所、上交所、深交所的可转债、股票、指数、ETF 等品种，上交所、深交所的ETF 期权；支持期货（中金所/上期所/大商所/郑商所/上海国际能源交易中心所） |
| begin_date | int | 是 | 日期，填写8 位的整型格式的日期，比如20240101 |
| end_date | int | 是 | 日期，填写8 位的整型格式的日期，比如20240201 |
| period | Period | 是 | 数据周期Period（见附录） |
| begin_time | int | 否 | 时分的时间戳，填写3 位或4 位的整型格式的日期，时占一位或两位，分占两位，，例如9 点整为900, 17 点25 分为1725 |
| end_time | int | 否 | 时分的时间戳，填写3 位或4 位的整型格式的日期，时占一位或两位，分占两位，，例如9 点整为900, 17 点25 分为1725 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| kline_dict | dict | 字典的key：代码字典的value：dataframe，column 为K 线数据Kline（见附录），index 为日期（datetime） |

## onSnapshotindex

- 章节: `3.5.3.1指数实时快照` 函数接口：onSnapshotindex
- 功能: 交易所指数快照数据的实时订阅回调函数

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list:[str] | 是 | 可传入列表，支持北交所、上交所、深交所的指数 |
| period | Period | 是 | Period.snapshot.value |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| data | Object | 指数为SnapshotIndex（见附录） |

## onSnapshotoption

- 章节: `3.5.3.8ETF 期权实时快照` 函数接口：onSnapshotoption
- 功能: 港股通快照数据的实时订阅回调函数

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() option_code_list = base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP') # 实时订阅sub_data = ad.SubscribeData() @sub_data.register(code_list=option_code_list, period=ad.constant.Period.snapshotoption.value) def onSnapshotoption(data: Union[ad.constant.SnapshotOption], period):  print('onSnapshotoption: ', data) sub_data.run()
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list:[str] | 是 | 可传入列表，支持上交所、深交所的ETF期权 |
| period | Period | 是 | Period.snapshotoption.value |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| data | Object | ETF 期权为SnapshotOption（见附录） |

## OnKLine

- 章节: `3.5.3.9实时K 线` 函数接口：OnKLine
- 功能: K 线数据的实时订阅回调函数

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() code_list = base_data_object.get_code_list(security_type='EXTRA_STOCK_A ') # 实时订阅sub_data = ad.SubscribeData() # K 线@sub_data.register(code_list=code_list, period=ad.constant.Period.min1.value) def OnKLine(data: Union[ad.constant.Kline], period):  print('OnKLine: ', data) sub_data.run()
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list:[str] | 是 | 可传入列表，支持北交所、上交所、深交 |
| 所的可转债、股票、指数、ETF 等品种支持期货（中金所/上期所/大商所/郑商所/上海国际能源交易中心所） | period | Period | 是 |
| Period（见附录） |  |  |  |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| data | Object | Kline（见附录） |

## 财务

## get_balance_sheet

- 章节: `3.5.5.1资产负债表` 函数接口：get_balance_sheet
- 功能: 获取指定股票列表的上市公司的资产负债表数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() calendar = base_data_object.get_calendar() today = calendar[-1] all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,  end_date=today) balance_sheet = info_data_object.get_balance_sheet(all_code_list)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 报告期，本地数据缓存方案 |
| end_date | int | 否 | 报告期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| balance_sheet | dict | key：code value:dataframe column 为balance_sheet 的字段index 为序号（无意义） |

## get_cash_flow

- 章节: `3.5.5.2现金流量表` 函数接口：get_cash_flow
- 功能: 获取指定股票列表的上市公司的现金流量表数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() calendar = base_data_object.get_calendar() today = calendar[-1] all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,  end_date=today) cash_flow = info_data_object.get_cash_flow (all_code_list)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 报告期，本地数据缓存方案 |
| end_date | int | 否 | 报告期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| cash_flow | dict | key：code value:dataframe column 为cash_flow 的字段index 为序号（无意义） |

## get_income

- 章节: `3.5.5.3利润表` 函数接口：get_income
- 功能: 获取指定股票列表的上市公司的利润表数据

### 调用方式

```python
import AmazingData as ad
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 报告期，本地数据缓存方案 |
| end_date | int | 否 | 报告期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| income | dict | key：code value:dataframe column 为income 的字段index 为序号（无意义） |

## get_profit_express

- 章节: `3.5.5.4业绩快报` 函数接口：get_profit_express
- 功能: 获取指定股票列表的上市公司的业绩快报数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() calendar = base_data_object.get_calendar() today = calendar[-1] all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 报告期，本地数据缓存方案 |
| end_date | int | 否 | 报告期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| profit_express | dataframe | column 为profit_express 的字段index 为序号（无意义） |

## get_profit_notice

- 章节: `3.5.5.5业绩预告` 函数接口：get_profit_notice
- 功能: 获取指定股票列表的上市公司的业绩预告数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() calendar = base_data_object.get_calendar() today = calendar[-1] all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,  end_date=today) profit_notice = info_data_object.get_profit_notice (all_code_list)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 报告期，本地数据缓存方案 |
| end_date | int | 否 | 报告期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| profit_notice | dataframe | column 为profit_notice 的字段index 为序号（无意义） |

## 股东

## get_share_holder

- 章节: `3.5.6.1十大股东数据` 函数接口：get_share_holder
- 功能: 获取指定股票列表的上市公司的十大股东数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() calendar = base_data_object.get_calendar() today = calendar[-1] all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,  end_date=today) share_holder = info_data_object.get_share_holder (all_code_list)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 到期日期，本地数据缓存方案 |
| end_date | int | 否 | 到期日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| share_holder | dataframe | column 为share_holder 的字段index 为序号（无意义） |

## get_holder_num

- 章节: `3.5.6.2股东户数` 函数接口：get_holder_num
- 功能: 获取指定股票列表的上市公司的股东户数数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData()
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 股东户数统计的截止日期，本地数据缓存方案 |
| end_date | int | 否 | 股东户数统计的截止日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| holder_num | dataframe | column 为holder_num 的字段index 为序号（无意义） |

## get_equity_structure

- 章节: `3.5.6.3股本结构` 函数接口：get_equity_structure
- 功能: 获取指定股票列表的上市公司的股本结构数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData()
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 变动日期，本地数据缓存方案 |
| end_date | int | 否 | 变动日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| equity_structu re | dataframe | column 为equity_structuree 的字段index 为序号（无意义） |

## get_equity_pledge_freeze

- 章节: `3.5.6.4股权冻结/质押` 函数接口：get_equity_pledge_freeze
- 功能: 获取指定股票列表的上市公司的股权冻结/质押数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData()
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 公告日期，本地数据缓存方案 |
| end_date | int | 否 | 公告日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| equity_pledge _freeze | dict | key：code value:dataframe column 为equity_pledge_freeze 的字段index 为序号（无意义） |

## get_equity_restricted

- 章节: `3.5.6.5限售股解禁` 函数接口：get_equity_restricted
- 功能: 获取指定股票列表的上市公司的限售股解禁数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() calendar = base_data_object.get_calendar() today = calendar[-1] all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,  end_date=today) equity_restricted = info_data_object.get_equity_restricted (all_code_list)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 解禁日期，本地数据缓存方案 |
| end_date | int | 否 | 解禁日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| equity_restrict ed | dict | key：code value:dataframe column 为equity_restricted 的字段index 为序号（无意义） |

## get_dividend

- 章节: `3.5.7.1分红数据` 函数接口：get_dividend
- 功能: 获取指定股票列表的上市公司的分红数据

### 调用方式

```python
get_dividend
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 公告日期，本地数据缓存方案 |
| end_date | int | 否 | 公告日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| dividend | dataframe | column 为dividend 的字段index 为序号（无意义） |

## get_right_issue

- 章节: `3.5.7.2配股数据` 函数接口：get_right_issue
- 功能: 获取指定股票列表的上市公司的配股数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() calendar = base_data_object.get_calendar() today = calendar[-1] all_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_STOCK_A_SH_SZ', start_date=20130101,  end_date=today) right_issue = info_data_object.get_right_issue(all_code_list)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深A 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 公告日期，本地数据缓存方案 |
| end_date | int | 否 | 公告日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| right_issue | dataframe | column 为right_issue 的字段index 为序号（无意义） |

## 指数

## get_index_constituent

- 章节: `3.5.12.1交易所指数成分股` 函数接口：get_index_constituent
- 功能: 获取指定交易所指数列表的成分股数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() code_list = base_data_object.get_code_list(security_type='EXTRA_INDEX_A') index_constituent = info_data_object.get_index_constituent(code_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深指数的的代码列表，可见示例，仅支持常用指数，约600 多只，无返回数据则不支持。 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，仅从本地获取，不从服务器获取数据；False ，仅从服务器获取，不从本地获取数据；因为原始数据的剔除日期会根据最新数据修改，所以第一次运行is_local 需要设置成 False 才会从服务器获取数据。 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| index_constit uent | dict | key：code value:dataframe column 为index_constituent 的字段index 为日期 |

## get_index_weight

- 章节: `3.5.12.2` 交易所指数成分股日权重
- 功能: 获取指定交易所指数列表的成分股日权重数据

### 调用方式

```python
import AmazingData as ad
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持指数列表；指数代码：支持以下5 个指数上证50： 000016.SH 沪深300： 000300.SH 中证500：  000905.SH 中证800：  000906.SH 中证1000： 000852.SH |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 变动日期，本地数据缓存方案 |
| end_date | int | 否 | 变动日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| index_weight | dict | key：code value:dataframe column 为index_weight 的字段index 为日期 |

## 行业

## get_industry_base_info

- 章节: `3.5.13.1行业指数基本信息` 函数接口：get_industry_base_info
- 功能: 获取行业指数的基本信息数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() industry_base_info = info_data_object.get_industry_base_info()
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，仅从本地获取，不从服务器获取数据；False ，仅从服务器获取，不从本地获取数据；因为原始数据的剔除日期会根据最新数据修改，所以第一次运行is_local 需要设置成 False 才会从服务器获取数据。 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| industry_base _info | dict | key：code value:dataframe column 为industry_base_info 的字段index 为日期 |

## get_industry_constituent

- 章节: `3.5.13.2` 行业指数成分股
- 功能: 获取指定行业指数列表的成分股数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() industry_base_info = info_data_object.get_industry_base_info() industry_base_list = list(industry_base_info['INDEX_CODE']) # 行业指数成分股industry_constituent = info_data_object.get_industry_constituent(industry_base_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持行业指数的的代码列表，可见示例，仅从get_industry_base_info 取到的指数代码。 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，仅从本地获取，不从服务器获取数据；False ，仅从服务器获取，不从本地获取数据；因为原始数据的剔除日期会根据最新数据修改，所以第一次运行is_local 需要设置成 False 才会从服务器获取数据。 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| industry_cons tituent | dict | key：code value:dataframe column 为industry_constituent 的字段index 为日期 |

## get_industry_weight

- 章节: `3.5.13.3` 行业指数成分股日权重
- 功能: 获取指定行业指数列表的成分股日权重数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() industry_base_info = info_data_object.get_industry_base_info() industry_base_list = list(industry_base_info['INDEX_CODE']) # 行业指数日权重industry_weight = info_data_object.get_industry_weight(industry_base_list)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持行业指数的的代码列表，可见示例，仅从get_industry_base_info 取到的指数代码。 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 交易日期，本地数据缓存方案 |
| end_date | int | 否 | 交易日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| industry_weig ht | dict | key：code value:dataframe column 为industry_weight 的字段index 为日期 |

## get_industry_daily

- 章节: `3.5.13.4` 行业指数日行情
- 功能: 获取指定行业指数列表的日行情数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() industry_base_info = info_data_object.get_industry_base_info() industry_base_list = list(industry_base_info['INDEX_CODE']) # 行业指数日行情industry_daily = info_data_object.get_industry_daily(industry_base_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持行业指数的的代码列表，可见示例，仅从get_industry_base_info 取到的指数代码。 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 交易日期，本地数据缓存方案 |
| end_date | int | 否 | 交易日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| industry_daily | dict | key：code value:dataframe column 为industry_daily 的字段index 为日期 |

## ETF

## get_etf_pcf 功能描述：获取指定ETF 的申赎和成分股数据（沪深交易所的ETF）

- 章节: `3.5.11.1 ETF 每日最新申赎数据` 函数接口： get_etf_pcf 功能描述：获取指定ETF 的申赎和成分股数据（沪深交易所的ETF）

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) base_data_object = ad.BaseData() code_list = base_data_object.get_hist_code_list(security_type='EXTRA_ETF') etf_pcf_info, etf_pcf_constituent = base_data_object.get_etf_pcf(code_list)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深ETF 的的代码列表，可见示例 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| etf_pcf_info | dataframe | column 为etf_pcf_info 的字段index 为ETF 代码 |
| etf_pcf_consti tuent | dict | 字典的key：ETF 代码字典的value：dataframe，column 为etf_pcf_constituent 的字段，index 为序号 |

## get_fund_share

- 章节: `3.5.11.2` ETF 基金份额
- 功能: 获取指定ETF 列表的基金份额数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() etf_code_list = base_data_object.get_code_list(security_type='EXTRA_ETF') # ETF 份额fund_share = info_data_object.get_fund_share(etf_code_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深ETF 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 变动日期，本地数据缓存方案 |
| end_date | int | 否 | 变动日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| fund_share | dict | key：code value:dataframe column 为fund_share 的字段index 为日期 |

## get_fund_iopv

- 章节: `3.5.11.3` ETF 每日收盘iopv
- 功能: 获取指定ETF 列表的基金份额数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() etf_code_list = base_data_object.get_code_list(security_type='EXTRA_ETF') # ETF 份额fund_iopv = info_data_object.get_fund_iopv(etf_code_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深ETF 的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |
| begin_date | int | 否 | 变动日期，本地数据缓存方案 |
| end_date | int | 否 | 变动日期，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| fund_iopv | dict | key：code value:dataframe column 为fund_iopv 的字段index 为序号，无意义 |

## 可转债

## get_kzz_issuance

- 章节: `3.5.14.1可转债发行` 函数接口：get_kzz_issuance
- 功能: 获取指定可转债列表的可转债发行数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() code_list = base_data_object.get_code_list('EXTRA_KZZ') kzz_issuance = info_data_object.get_kzz_issuance(code_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持可转债的的代码列表 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| kzz_issuance | dict | dataframe column 为kzz_issuance 的字段index 无意义 |

## get_kzz_share

- 章节: `3.5.14.2` OFF_SUBSCR_UNIT_INC_ DESC
- 功能: 获取指定可转债列表的可转债份额数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() code_list = base_data_object.get_code_list('EXTRA_KZZ') kzz_share = info_data_object.get_kzz_share(code_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持可转债的的代码列表 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| kzz_share | dict | dataframe column 为kzz_share 的字段index 无意义 |

## get_kzz_conv_change

- 章节: `3.5.14.4` 可转债转股变动数据
- 功能: 获取指定可转债列表的可转债转股变动数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() code_list = base_data_object.get_code_list('EXTRA_KZZ') kzz_conv_change = info_data_object.get_kzz_conv_change(code_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持可转债的的代码列表 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| kzz_conv_cha nge | dict | dataframe column 为kzz_conv_change 的字段index 无意义 |

## get_kzz_corr

- 章节: `3.5.14.5` 派息
- 功能: 获取指定可转债列表的可转债修正数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() code_list = base_data_object.get_code_list('EXTRA_KZZ') kzz_corr = info_data_object.get_kzz_corr(code_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持可转债的的代码列表 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ |
| 'D://AmazingData_local_data//' ” | is_local | bool | 否 |
| 默认为True，本地数据缓存方案 |  |  |  |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| kzz_corr | dict | dataframe column 为kzz_corr 的字段index 无意义 |

## get_kzz_call_explanation

- 章节: `3.5.14.10可转债赎回条款执行说明` 函数接口：get_kzz_call_explanation
- 功能: 获取指定可转债列表的可转债赎回条款执行说明数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() code_list = base_data_object.get_code_list('EXTRA_KZZ') kzz_call_explanation = info_data_object.get_kzz_call_explanation(code_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持可转债的的代码列表 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| kzz_call_expl anation | dict | dataframe column 为kzz_call_explanation 的字段index 无意义 |

## get_kzz_put_explanation

- 章节: `3.5.14.9` 可转债回售条款执行说明
- 功能: 获取指定可转债列表的可转债回售条款执行说明数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() code_list = base_data_object.get_code_list('EXTRA_KZZ') kzz_put_explanation = info_data_object.get_kzz_put_explanation(code_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持可转债的的代码列表 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| kzz_put_expla nation | dict | dataframe column 为kzz_put_explanation 的字段index 无意义 |

## get_kzz_suspend

- 章节: `3.5.14.11可转债停复牌信息` 函数接口：get_kzz_suspend
- 功能: 获取指定可转债列表的可转债停复牌信息数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData()
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持可转债的的代码列表 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| kzz_suspend | dict | dataframe column 为kzz_suspend 的字段index 无意义 |

## 期权

## get_option_basic_info 功能描述：获取指定期权的基本资料（沪深交易所的ETF 期权）

- 章节: `3.5.10.1期权基本资料` 函数接口：get_option_basic_info 功能描述：获取指定期权的基本资料（沪深交易所的ETF 期权）

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() calendar = base_data_object.get_calendar() today = calendar[-1] code_list = base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP') hist_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_ETF_OP'', start_date=20130101,  end_date=today) option_basic_info =info_data_object.get_option_basic_info(code_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深ETF 期权的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| option_basic_ info | dataframe | column 为option_basic_info 的字段index 为序号（无意义） |

## get_option_std_ctr_specs 功能描述：获取指定期权标准合约属性（沪深交易所的ETF 期权）输入参数：

- 章节: `3.5.10.2` 期权标准合约属性

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() option_std_ctr_specs =info_data_object.get_option_std_ctr_specs(['510050.SH'], is_local=False)
```

### 输入参数

无。

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| option_std_ctr _specs | dataframe | column 为option_std_ctr_specs 的字段index 为序号（无意义） |

## get_option_mon_ctr_specs

- 章节: `3.5.10.3` POSITION_LIMIT
- 功能: 获取指定期权月合约属性变动（沪深交易所的ETF 期权）

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() base_data_object = ad.BaseData() calendar = base_data_object.get_calendar() today = calendar[-1] code_list = base_data_object.get_option_code_list(security_type='EXTRA_ETF_OP') hist_code_list = base_data_object.get_hist_code_list(security_type='EXTRA_ETF_OP'', start_date=20130101,  end_date=today) option_mon_ctr_specs =info_data_object.get_option_mon_ctr_specs(code_list, is_local=False)
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| code_list | list[str] | 是 | 支持沪深ETF 期权的的代码列表，可见示例 |
| local_path | str | 是 | 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” |
| is_local | bool | 否 | 默认为True，本地数据缓存方案 |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| block_trading | dataframe | column 为block_trading 的字段index 为序号（无意义） |

## 国债

## get_treasury_yield

- 章节: `3.5.15.1国债收益率` 函数接口：get_treasury_yield
- 功能: 获取指定期限的国债收益率数据

### 调用方式

```python
import AmazingData as ad ad.login(username='username', password='password',host='***.***.***.***',port=****) info_data_object = ad.InfoData() treasury_yield = info_data_object.get_treasury_yield(['m3', 'm6', 'y1', 'y2', 'y3', 'y5', 'y7', 'y10', 'y30'])
```

### 输入参数

| 参数 | 类型 | 必选 | 说明 |
|---|---|---|---|
| term_list | list[str] | 是 | 支持不同期限的国债收益率'm3'：3 个月, 'm6'：6 个月, 'y1'：1 年, 'y2'：2 年, 'y3'：3 年, 'y5'：5 年, 'y7'：7 年, 'y10'：20 年, |
| 'y30'：30 年 | local_path | str | 是 |
| 本地存储数据的路径，需绝对路径，格式类似“ 'D://AmazingData_local_data//' ” | is_local | bool | 否 |
| 默认为True，本地数据缓存方案 | begin_date | int | 否 |
| 变动日期，本地数据缓存方案 | end_date | int | 否 |
| 变动日期，本地数据缓存方案 |  |  |  |

### 返回参数

| 参数 | 类型 | 说明 |
|---|---|---|
| treasury_yield | dict | 字典的key：期限字典的value：dataframe，column 为YIELD，国债收益率数据，index 为日期 |
