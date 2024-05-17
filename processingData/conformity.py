from getData import archive_data

# 数据整合，策略数据与历史行情数据整合

query_sql = """SELECT
	`gpcode`,
	`gpname`,
	`zxj`,
	`zdf`,
	`jx`,
	`zsz`,
	`ltsz`,
	`mrxh`,
	`jd`,
	`hy`,
	`jsxt`,
	`xg`,
	`jishu`,
	`market_code`,
	`code`,
	`signal_date`,
	`id`,
	`create_date`,
	`trading_day_5`,
	`trading_day__id_5`,
	`trading_day_10`,
	`trading_day__id_10`,
	`trading_day_15`,
	`trading_day__id_15`,
	`trading_day_20`,
	`trading_day__id_20`,
	`trading_day_30`,
	`trading_day__id_30`,
	`trading_day_45`,
	`trading_day__id_45`,
	`trading_day_60`,
	`trading_day__id_60`,
	`trading_day_90`,
	`trading_day__id_90`,
	`trading_day_120`,
	`trading_day__id_120`,
	`trading_day_160`,
	`trading_day__id_160`,
	`trading_day_180`,
	`trading_day__id_180`,
	`trading_day_230`,
	`trading_day__id_230`,
	`trading_day_360`,
	`trading_day__id_360`,
	`trading_day_500`,
	`trading_day__id_500`,
	`trading_day_600`,
	`trading_day__id_600`,
	`trading_day_700`,
	`trading_day__id_700`,
	`trading_day_900`,
	`trading_day__id_900` 
FROM
	20_tactics
WHERE id = '000001940da111ef8d741831bf13c82e'
	 """
# signal_date BETWEEN '2010-01-01'
# 	AND '2010-12-31'
# 	AND is_conformity IS NULL
tacDatas = archive_data.select_data(query_sql)
for tacData in tacDatas:
    code = tacData[14]
    signal_date = tacData[15]
    qyery_stock_data = "SELECT `trade_time`, `open`, `close`, `high`, `low`, `volume`, `amount`, `change`, `change_pct`, `turnover_ratio`, `pre_close`, `stock_code`, `trade_date`, `id`, `create_date`, `stock_code_date` FROM stocks_trading_data WHERE stock_code = %s AND trade_date = %s"
    stockDatas = archive_data.select_datas(qyery_stock_data,(code,signal_date))
    for stockData in stockDatas:
        dataList = tacData + stockData
        insert_sql ="""INSERT INTO `a_historical_data`.`conformity_20_tactics` (`gpcode`, `gpname`, `zxj`, `zdf`, `jx`, `zsz`, `ltsz`, `mrxh`, `jd`, `hy`, `jsxt`, `xg`, `jishu`, `market_code`, `code`, `signal_date`, `id`, `create_date`, `trading_day_5`, `trading_day__id_5`, `trading_day_10`, `trading_day__id_10`, `trading_day_15`, `trading_day__id_15`, `trading_day_20`, `trading_day__id_20`, `trading_day_30`, `trading_day__id_30`, `trading_day_45`, `trading_day__id_45`, `trading_day_60`, `trading_day__id_60`, `trading_day_90`, `trading_day__id_90`, `trading_day_120`, `trading_day__id_120`, `trading_day_160`, `trading_day__id_160`, `trading_day_180`, `trading_day__id_180`, `trading_day_230`, `trading_day__id_230`, `trading_day_360`, `trading_day__id_360`, `trading_day_500`, `trading_day__id_500`, `trading_day_600`, `trading_day__id_600`, `trading_day_700`, `trading_day__id_700`, `trading_day_900`, `trading_day__id_900`, `trade_time_5`, `open_5`, `close_5`, `high_5`, `low_5`, `volume_5`, `amount_5`, `change_5`, `change_pct_5`, `turnover_ratio_5`, `pre_close_5`, `stock_code_5`, `trade_date_5`, `stock_id_5`, `stock_create_date_5`, `stock_code_date_5`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
        archive_data.addData(insert_sql,dataList)
        print(dataList[68])
    print(dataList)


